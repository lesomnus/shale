package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/lesomnus/payday/pdcmd"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera/preset"
	"github.com/lesomnus/shale/cmd"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/flg"
)

func addCameraPresetCommands(t *pdcmd.Tree, c *cmd.Config) {
	t.Add("camera-preset/publish", cameraPresetPublishCommand(&connector{c: c}))
}

func cameraPresetPublishCommand(connection pdcmd.Connector) *xli.Command {
	return &xli.Command{
		Name:  "publish",
		Brief: "publish an immutable CP preset revision from a scoped YAML or JSON declaration",
		Flags: flg.Flags{
			&flg.String{Name: "file", Brief: "portable camera/preset YAML or JSON file (required)"},
			&flg.String{Name: "tenant", Brief: "target tenant UUID; otherwise the single tenant named by every rule"},
			&flg.String{Name: "site", Brief: "target site UUID; absent publishes a tenant-level ruleset"},
			&flg.String{Name: "name", Brief: "preset display name; defaults to the document id"},
			&flg.Switch{Name: "dry-run", Brief: "validate locally and print Create request without reading credentials or contacting CP"},
			&flg.String{Name: "output", Alias: 'o', Brief: "json (default) or protojson"},
		},
		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, _ xli.Next) error {
			var o cameraPresetPublishOptions
			o.file, _ = flg.Find[string](self, "file")
			o.tenant, _ = flg.Find[string](self, "tenant")
			o.site, _ = flg.Find[string](self, "site")
			o.name, _ = flg.Find[string](self, "name")
			o.dryRun, _ = flg.Find[bool](self, "dry-run")
			o.output, _ = flg.Find[string](self, "output")
			return publishCameraPreset(ctx, self, connection, o)
		}),
	}
}

type cameraPresetPublishOptions struct {
	file, tenant, site, name, output string
	dryRun                           bool
}

var cameraPresetPublishKey = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,127}$`)

func presetScopeID(value, entity string) ([]byte, error) {
	id, err := pdid.Parse(value)
	domain, ok := pdid.Lookup(entity)
	if err != nil || id.String() != value || !ok || id.Domain() != domain {
		return nil, fmt.Errorf("preset scope requires a canonical %s UUID", strings.TrimPrefix(entity, "shale."))
	}
	return id.Bytes(), nil
}

// buildCameraPresetPublish does not connect to CP or derive authorization from
// a rule. It validates one explicit target; CP still authorizes every selector
// and the caller's current standing when Create receives this request.
func buildCameraPresetPublish(o cameraPresetPublishOptions) (*api.CameraPresetCreateRequest, error) {
	if o.file == "" {
		return nil, errors.New("--file is required")
	}
	provider, err := preset.Load(o.file)
	if err != nil {
		// A wrong file may contain private values. Do not echo YAML diagnostics.
		return nil, errors.New("cannot read a valid bounded preset YAML or JSON document")
	}
	declaration := provider.Config()
	if !cameraPresetPublishKey.MatchString(declaration.ID) || !cameraPresetPublishKey.MatchString(declaration.Revision) {
		return nil, errors.New("preset id and revision must be nonempty CP identifiers of at most 128 characters")
	}
	tenant := o.tenant
	if tenant == "" {
		tenant = declaration.Rules[0].Scope.TenantID
	}
	tenantID, err := presetScopeID(tenant, "shale.Tenant")
	if err != nil {
		return nil, err
	}
	var targetSite *api.SiteRef
	if o.site != "" {
		siteID, err := presetScopeID(o.site, "shale.Site")
		if err != nil {
			return nil, err
		}
		targetSite = api.SiteRef_builder{Id: siteID}.Build()
	}
	for _, rule := range declaration.Rules {
		if !cameraPresetPublishKey.MatchString(rule.ID) {
			return nil, errors.New("preset rule ids must be nonempty CP identifiers of at most 128 characters")
		}
		if rule.Scope.TenantID != tenant || o.site != "" && rule.Scope.SiteID != o.site {
			return nil, errors.New("preset rules cross the target tenant or site")
		}
		for _, selector := range []struct{ value, entity string }{
			{rule.Scope.SiteID, "shale.Site"}, {rule.Scope.SetID, "shale.Set"},
			{rule.Scope.ProducerID, "shale.Producer"}, {rule.Scope.NetworkID, "shale.CameraNetwork"},
			{string(rule.Match.DeviceID), "shale.CameraDevice"},
		} {
			if selector.value != "" {
				if _, err := presetScopeID(selector.value, selector.entity); err != nil {
					return nil, err
				}
			}
		}
	}
	name := o.name
	if name == "" {
		name = declaration.ID
	}
	if len(name) > 256 {
		return nil, errors.New("preset name must be at most 256 bytes")
	}
	rules, err := presetwire.EncodeRules(declaration.Rules)
	if err != nil {
		return nil, errors.New("preset rules cannot be represented by the CP contract")
	}
	return api.CameraPresetCreateRequest_builder{Tenant: api.TenantRef_builder{Id: tenantID}.Build(), Site: targetSite, PresetKey: declaration.ID, Revision: declaration.Revision, Name: name, Rules: rules}.Build(), nil
}

func publishCameraPreset(ctx context.Context, out io.Writer, connection pdcmd.Connector, o cameraPresetPublishOptions) error {
	var printer pdcmd.Printer = pdcmd.Json
	switch o.output {
	case "", "json":
	case "protojson":
		printer = pdcmd.ProtoJson
	default:
		return errors.New("--output must be json or protojson")
	}
	req, err := buildCameraPresetPublish(o)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.dryRun {
		return printer.Print(out, req)
	}
	conn, done, err := connection.Connect(ctx)
	if err != nil {
		return err
	}
	if done != nil {
		defer done()
	}
	created, err := api.NewCameraPresetServiceClient(conn).Create(ctx, req)
	if err != nil {
		return err
	}
	return printer.Print(out, created)
}
