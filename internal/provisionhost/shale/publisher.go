// Package shale deploys camera capture to an existing adopted Producer. It does
// not create Source metadata in lieu of starting capture, or delete archives.
package shale

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"go.yaml.in/yaml/v3"
)

const BindingLabel = "shale.provision.binding"
const DeviceLabel = "shale.provision.device"

// Authority uses an authenticated CP identity. It must see the entire tenant
// alias namespace, validate the assignment and require the binding labels.
type Authority interface {
	Check(context.Context, provision.Deployment, string) error
	Observe(context.Context, provision.Deployment, provision.BindingState) (provision.BindingState, error)
}

type RestartFunc func(context.Context) error

// CommandRestart executes an administrator-configured argv without a shell. Its
// output is deliberately discarded: a service manager might echo credentials.
func CommandRestart(argv []string) RestartFunc {
	return func(ctx context.Context) error {
		if len(argv) == 0 {
			return errors.New("producer restart command is not configured")
		}
		if err := exec.CommandContext(ctx, argv[0], argv[1:]...).Run(); err != nil {
			return errors.New("producer graceful restart failed")
		}
		return nil
	}
}

// FilePublisher is local to the assigned Producer's network/filesystem. Remote
// controllers dispatch to a worker; they do not write an arbitrary remote path.
type FilePublisher struct {
	ConfigPath string
	StateDir   string
	Scope      camera.EnrollmentScope
	Authority  Authority
	Restart    RestartFunc
}

func (p *FilePublisher) Impacts(q provision.Request) []provision.Impact {
	return []provision.Impact{{Kind: "producer_restart", ProducerID: q.Scope.ProducerID, RestartMayBeRequired: true, AffectsOtherSources: true}}
}

type deploymentJournal struct {
	OperationID       string     `json:"operation_id"`
	BindingID         string     `json:"binding_id"`
	Alias             string     `json:"alias"`
	Before            *yaml.Node `json:"before,omitempty"`
	After             *yaml.Node `json:"after"`
	Installed         bool       `json:"installed"`
	Restarted         bool       `json:"restarted"`
	DeployedAt        time.Time  `json:"deployed_at"`
	RollbackRestarted bool       `json:"rollback_restarted"`
}

func (p *FilePublisher) Publish(ctx context.Context, d provision.Deployment) (provision.BindingState, error) {
	if d.Request.Scope != p.Scope || !d.Credential.Authorizes(p.Scope, camera.AuthStreaming) || !d.Credential.Recoverable {
		return provision.BindingState{}, camera.ErrForbidden
	}
	if p.Authority == nil || p.Restart == nil || p.ConfigPath == "" || p.StateDir == "" {
		return provision.BindingState{}, camera.ErrInvalid
	}
	alias := d.Previous.Alias
	if alias == "" {
		alias = d.Request.Alias
	}
	if alias == "" {
		return provision.BindingState{}, camera.ErrInvalid
	}
	if err := p.Authority.Check(ctx, d, alias); err != nil {
		return provision.BindingState{}, err
	}
	state, err := filestore.Open(p.StateDir)
	if err != nil {
		return provision.BindingState{}, err
	}
	unlock, err := state.Lock(ctx, "producer-config/"+p.ConfigPath)
	if err != nil {
		return provision.BindingState{}, err
	}
	defer unlock()
	path := filepath.Join(p.StateDir, "deployment-"+provision.Key(d.OperationID)+".json")
	var journal deploymentJournal
	if b, er := os.ReadFile(path); er == nil {
		if er = json.Unmarshal(b, &journal); er != nil {
			return provision.BindingState{}, er
		}
		if journal.OperationID != d.OperationID || journal.BindingID != d.Request.BindingID || journal.Alias != alias {
			return provision.BindingState{}, provision.ErrConflict
		}
	} else if !errors.Is(er, os.ErrNotExist) {
		return provision.BindingState{}, er
	}
	doc, sources, mode, err := readConfig(p.ConfigPath)
	if err != nil {
		return provision.BindingState{}, err
	}
	// A reference-only manifest is useful only if the actual capture process
	// can consume it. Check the configured local resolver before any restart.
	credentials := mapping(mapping(doc.Content[0], "producer"), "credentials")
	directory, providerName := scalar(credentials, "directory"), scalar(credentials, "provider")
	if directory == "" || !filepath.IsAbs(directory) {
		return provision.BindingState{}, camera.ErrAuthRequired
	}
	resolver := credential.FileProvider{Dir: directory, Name: providerName}
	lease, err := camera.Acquire(ctx, resolver, camera.AuthRequest{Scope: d.Request.Scope, Device: d.Request.Device, Purpose: camera.AuthStreaming, Requirement: camera.AuthRequirement{Access: camera.AccessRead, Recoverable: true}, ExistingRef: &d.Credential})
	if err != nil {
		return provision.BindingState{}, err
	}
	lease.Close()
	idx, old, err := sourceAt(sources, alias, d.Request.BindingID, string(d.Request.Device.ID))
	if err != nil {
		return provision.BindingState{}, err
	}
	after, err := sourceNode(d, alias)
	if err != nil {
		return provision.BindingState{}, err
	}
	if journal.OperationID == "" {
		journal = deploymentJournal{OperationID: d.OperationID, BindingID: d.Request.BindingID, Alias: alias, Before: old, After: after, DeployedAt: time.Now().UTC()}
		if err = writeJournal(path, journal); err != nil {
			return provision.BindingState{}, err
		}
	} else if !nodesEqual(journal.After, after) {
		return provision.BindingState{}, provision.ErrConflict
	}
	if journal.Restarted && !nodesEqual(old, journal.After) {
		return provision.BindingState{}, provision.ErrConflict
	}
	if !journal.Restarted {
		if !d.Request.AllowProducerRestart {
			return provision.BindingState{}, camera.ErrNeedsUserInput
		}
		// A prior process may have installed the manifest just before dying.
		// If neither planned version matches, an external writer intervened.
		if !nodesEqual(old, journal.After) {
			if !nodesEqual(old, journal.Before) {
				return provision.BindingState{}, provision.ErrConflict
			}
			if idx < 0 {
				sources.Content = append(sources.Content, after)
			} else {
				sources.Content[idx] = after
			}
			b, er := yaml.Marshal(doc)
			if er != nil {
				return provision.BindingState{}, er
			}
			if err = filestore.WriteAtomic(p.ConfigPath, b, mode); err != nil {
				return provision.BindingState{}, err
			}
		}
		journal.Installed = true
		if err = writeJournal(path, journal); err != nil {
			return provision.BindingState{}, err
		}
		// This command must gracefully stop the old process before returning;
		// a successful reload request alone is not this contract.
		if err = p.Restart(ctx); err != nil {
			return provision.BindingState{}, err
		}
		journal.Restarted = true
		if err = writeJournal(path, journal); err != nil {
			return provision.BindingState{}, err
		}
	}
	return provision.BindingState{BindingID: journal.BindingID, Alias: alias, DeploymentID: journal.OperationID, DeployedAt: journal.DeployedAt, SourceID: d.Previous.SourceID}, nil
}

func (p *FilePublisher) Observe(ctx context.Context, d provision.Deployment, b provision.BindingState) (provision.BindingState, error) {
	if p.Authority == nil || d.Request.Scope != p.Scope {
		return b, camera.ErrForbidden
	}
	return p.Authority.Observe(ctx, d, b)
}

// Rollback restores only this binding's reversible capture entry, preserving
// unrelated entries and every CP Source/archive. It is explicit, never an
// automatic response to an ambiguous device/registration timeout.
func (p *FilePublisher) Rollback(ctx context.Context, operationID string) error {
	state, err := filestore.Open(p.StateDir)
	if err != nil {
		return err
	}
	u, err := state.Lock(ctx, "producer-config/"+p.ConfigPath)
	if err != nil {
		return err
	}
	defer u()
	path := filepath.Join(p.StateDir, "deployment-"+provision.Key(operationID)+".json")
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var j deploymentJournal
	if err = json.Unmarshal(b, &j); err != nil {
		return err
	}
	doc, sources, mode, err := readConfig(p.ConfigPath)
	if err != nil {
		return err
	}
	idx, old, err := sourceAt(sources, j.Alias, j.BindingID, "")
	if err != nil {
		return err
	}
	if j.RollbackRestarted && nodesEqual(old, j.Before) {
		return nil
	}
	if !nodesEqual(old, j.After) && !nodesEqual(old, j.Before) {
		return provision.ErrConflict
	}
	if !nodesEqual(old, j.Before) {
		if j.Before == nil {
			sources.Content = append(sources.Content[:idx], sources.Content[idx+1:]...)
		} else {
			sources.Content[idx] = j.Before
		}
		b, err = yaml.Marshal(doc)
		if err != nil {
			return err
		}
		if err = filestore.WriteAtomic(p.ConfigPath, b, mode); err != nil {
			return err
		}
	}
	if p.Restart == nil {
		return camera.ErrInvalid
	}
	if err = p.Restart(ctx); err != nil {
		return err
	}
	j.RollbackRestarted = true
	return writeJournal(path, j)
}

func writeJournal(path string, j deploymentJournal) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return filestore.WriteAtomic(path, b, 0600)
}
func readConfig(path string) (*yaml.Node, *yaml.Node, os.FileMode, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, 0, camera.ErrInvalid
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, 0, err
	}
	var doc yaml.Node
	if err = yaml.Unmarshal(b, &doc); err != nil {
		return nil, nil, 0, errors.New("invalid producer configuration")
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, nil, 0, camera.ErrInvalid
	}
	root := doc.Content[0]
	producer := mapping(root, "producer")
	if producer == nil || producer.Kind != yaml.MappingNode {
		return nil, nil, 0, camera.ErrInvalid
	}
	sources := mapping(producer, "sources")
	if sources == nil || sources.Kind != yaml.SequenceNode || len(sources.Content) == 0 {
		return nil, nil, 0, errors.New("provisioning requires an existing configured Producer")
	}
	return &doc, sources, info.Mode().Perm(), nil
}
func mapping(n *yaml.Node, key string) *yaml.Node {
	if n == nil {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}
func scalar(n *yaml.Node, key string) string {
	v := mapping(n, key)
	if v == nil {
		return ""
	}
	return v.Value
}
func sourceAt(sources *yaml.Node, alias, binding, device string) (int, *yaml.Node, error) {
	idx := -1
	var out *yaml.Node
	for i, n := range sources.Content {
		if scalar(n, "alias") != alias {
			continue
		}
		if idx >= 0 {
			return -1, nil, provision.ErrConflict
		}
		idx, out = i, n
	}
	if out != nil {
		labels := mapping(out, "labels")
		if scalar(labels, BindingLabel) != binding || (device != "" && scalar(labels, DeviceLabel) != device) {
			return -1, nil, provision.ErrConflict
		}
		input := scalar(out, "input")
		if input != camera.SanitizeURI(input) {
			return -1, nil, camera.ErrInvalid
		}
	}
	return idx, out, nil
}
func sourceNode(d provision.Deployment, alias string) (*yaml.Node, error) {
	p := d.Profile
	switch strings.ToLower(p.Codec) {
	case "h264", "avc":
		p.Codec = "h264"
	case "h265", "hevc":
		p.Codec = "h265"
	default:
		return nil, camera.ErrUnsupported
	}
	if p.StreamURI == "" || p.StreamURI != camera.SanitizeURI(p.StreamURI) {
		return nil, camera.ErrInvalid
	}
	if p.Width <= 0 || p.Height <= 0 || p.FPS <= 0 {
		return nil, camera.ErrInvalid
	}
	ceiling := p.Bitrate
	if ceiling <= 0 {
		ceiling = producer.StartingCeiling(fmt.Sprintf("%dx%d", p.Width, p.Height), int(math.Round(p.FPS)), p.Codec)
	}
	// ONVIF's bitrate describes video; Shale also budgets the default archive
	// audio and live Opus track, as the Producer's ONVIF negotiation does.
	ceiling += 2 * producer.DefaultAudioBitrate
	m := map[string]any{"alias": alias, "name": d.Request.Name, "input": p.StreamURI, "input_credential": d.Credential, "scope": d.Request.Scope, "labels": map[string]string{BindingLabel: d.Request.BindingID, DeviceLabel: string(d.Request.Device.ID)}, "format": p.Codec, "size": fmt.Sprintf("%dx%d", p.Width, p.Height), "encoder": "copy", "max_bitrate": fmt.Sprintf("%dbps", ceiling)}
	// The legacy Producer field is integer-only. Copy capture preserves the
	// source cadence without it; retain the exact fractional FPS in Verified,
	// rather than silently truncating it in capture metadata.
	if p.FPS > 0 && math.Trunc(p.FPS) == p.FPS {
		m["fps"] = int(p.FPS)
	}
	if p.GOP > 0 {
		m["keyframe_interval"] = (time.Duration(float64(time.Second) * float64(p.GOP) / p.FPS)).String()
	}
	var node yaml.Node
	if err := node.Encode(m); err != nil {
		return nil, err
	}
	return &node, nil
}
func nodesEqual(a, b *yaml.Node) bool {
	if a == nil || b == nil {
		return a == b
	}
	var x, y any
	if a.Decode(&x) != nil || b.Decode(&y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}
