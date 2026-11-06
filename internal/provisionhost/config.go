package provisionhost

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/media"
	"github.com/lesomnus/shale/camera/onvif"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	shaleadapter "github.com/lesomnus/shale/internal/provisionhost/shale"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"go.yaml.in/yaml/v3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	StateDir          string           `json:"state_dir" yaml:"state_dir"`
	Listen            string           `json:"listen" yaml:"listen"`
	CertFile          string           `json:"cert_file,omitempty" yaml:"cert_file,omitempty"`
	KeyFile           string           `json:"key_file,omitempty" yaml:"key_file,omitempty"`
	AllowLoopbackHTTP bool             `json:"allow_loopback_http" yaml:"allow_loopback_http"`
	Networks          []Network        `json:"networks" yaml:"networks"`
	Providers         []ProviderConfig `json:"providers" yaml:"providers"`
	Targets           []TargetConfig   `json:"targets" yaml:"targets"`
	Clients           []ClientConfig   `json:"clients" yaml:"clients"`
	Ffmpeg            string           `json:"ffmpeg,omitempty" yaml:"ffmpeg,omitempty"`
	Ffprobe           string           `json:"ffprobe,omitempty" yaml:"ffprobe,omitempty"`
}

type ProviderConfig struct {
	Name      string `json:"name" yaml:"name"`
	Directory string `json:"directory" yaml:"directory"`
}
type ClientConfig struct {
	TokenFile string                   `json:"token_file" yaml:"token_file"`
	Scopes    []camera.EnrollmentScope `json:"scopes" yaml:"scopes"`
	Networks  []string                 `json:"networks,omitempty" yaml:"networks,omitempty"`
	Worker    bool                     `json:"worker" yaml:"worker"`
}

type ConnectionConfig struct {
	URL               string `json:"url" yaml:"url"`
	CAFile            string `json:"ca_file,omitempty" yaml:"ca_file,omitempty"`
	CertFile          string `json:"cert_file,omitempty" yaml:"cert_file,omitempty"`
	KeyFile           string `json:"key_file,omitempty" yaml:"key_file,omitempty"`
	TokenFile         string `json:"token_file,omitempty" yaml:"token_file,omitempty"`
	AllowLoopbackHTTP bool   `json:"allow_loopback_http" yaml:"allow_loopback_http"`
}
type VendorConfig struct {
	Name                 string `json:"name,omitempty" yaml:"name,omitempty"`
	WebEndpoint          string `json:"web_endpoint,omitempty" yaml:"web_endpoint,omitempty"`
	CompatibilityProfile string `json:"compatibility_profile,omitempty" yaml:"compatibility_profile,omitempty"`
}
type TargetConfig struct {
	Scope      camera.EnrollmentScope `json:"scope" yaml:"scope"`
	Provider   string                 `json:"provider" yaml:"provider"`
	ConfigPath string                 `json:"config_path,omitempty" yaml:"config_path,omitempty"`
	StateDir   string                 `json:"state_dir,omitempty" yaml:"state_dir,omitempty"`
	Restart    []string               `json:"restart,omitempty" yaml:"restart,omitempty"`
	CP         ConnectionConfig       `json:"cp" yaml:"cp"`
	Worker     ConnectionConfig       `json:"worker" yaml:"worker"`
	Vendor     VendorConfig           `json:"vendor" yaml:"vendor"`
}

// ReadConfig reads administrator-owned configuration. Jobs cannot select paths,
// programs, scopes, network policy or a new authentication provider.
func ReadConfig(path string) (Config, error) {
	var config Config
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
		return config, errors.New("provisioner configuration requires a private regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return config, errors.New("cannot open provisioner configuration")
	}
	defer f.Close()
	decoder := yaml.NewDecoder(io.LimitReader(f, 1<<20))
	decoder.KnownFields(true)
	if err = decoder.Decode(&config); err != nil {
		return config, camera.ErrInvalid
	}
	return config, nil
}

type Built struct {
	Host        *Host
	Config      Config
	connections []*grpc.ClientConn
	transports  []*http.Transport
}

func (b *Built) Close() {
	for _, connection := range b.connections {
		connection.Close()
	}
	for _, transport := range b.transports {
		transport.CloseIdleConnections()
	}
}

func Build(config Config) (built *Built, err error) {
	built = &Built{Config: config}
	defer func() {
		if err != nil {
			built.Close()
		}
	}()
	if config.StateDir == "" {
		return built, camera.ErrInvalid
	}
	options := Options{StateDir: config.StateDir, Networks: config.Networks}
	coreStore, er := filestore.Open(filepath.Join(config.StateDir, "core"))
	if er != nil {
		return built, er
	}
	providers := map[string]camera.AuthProvider{}
	for _, entry := range config.Providers {
		if entry.Name == "" || entry.Directory == "" || providers[entry.Name] != nil {
			return built, camera.ErrInvalid
		}
		providers[entry.Name] = credential.FileProvider{Name: entry.Name, Dir: entry.Directory}
	}
	for _, entry := range config.Clients {
		token, er := ReadToken(entry.TokenFile)
		if er != nil {
			return built, er
		}
		options.Clients = append(options.Clients, Client{Token: token, Scopes: entry.Scopes, Networks: entry.Networks, Worker: entry.Worker})
	}
	for _, entry := range config.Targets {
		network, er := networkByID(config.Networks, entry.Scope.NetworkID)
		if er != nil {
			return built, er
		}
		target := Target{Scope: entry.Scope, Provider: entry.Provider}
		if entry.Worker.URL != "" {
			client, token, er := connectionHTTP(entry.Worker)
			if er != nil {
				return built, er
			}
			if transport, ok := client.Transport.(*http.Transport); ok {
				built.transports = append(built.transports, transport)
			}
			remote := &RemoteExecutor{URL: entry.Worker.URL, Token: token, HTTPClient: client, AllowLoopbackHTTP: entry.Worker.AllowLoopbackHTTP}
			target.Executor = remote
			target.Checkpoint = remote.Checkpoint
		} else {
			provider := providers[entry.Provider]
			if provider == nil || entry.ConfigPath == "" || len(entry.Restart) == 0 {
				return built, camera.ErrInvalid
			}
			client, er := network.HTTPClient()
			if er != nil {
				return built, er
			}
			if transport, ok := client.Transport.(*http.Transport); ok {
				built.transports = append(built.transports, transport)
			}
			baseline := onvif.NewClient(client)
			var inspector camera.Inspector = baseline
			var configurator camera.VideoConfigurator = baseline
			var bootstrapper camera.Bootstrapper
			if entry.Vendor.Name != "" {
				if entry.Vendor.Name != "tplink" || network.AllowURI(entry.Vendor.WebEndpoint) != nil {
					return built, camera.ErrInvalid
				}
				driver := tplink.NewDriver(baseline, entry.Vendor.WebEndpoint)
				driver.CompatibilityProfile = entry.Vendor.CompatibilityProfile
				selected := &selectedDriver{Baseline: baseline, Vendor: driver, Endpoint: entry.Vendor.WebEndpoint}
				inspector, configurator, bootstrapper = selected, selected, selected
			}
			stateDir := entry.StateDir
			if stateDir == "" {
				stateDir = filepath.Join(config.StateDir, "target-"+provision.Key(entry.Scope.TenantID, entry.Scope.SetID, entry.Scope.ProducerID, entry.Scope.NetworkID))
			}
			store := coreStore
			connection, er := connectionGRPC(entry.CP)
			if er != nil {
				return built, er
			}
			built.connections = append(built.connections, connection)
			liveTLS, er := connectionTLS(entry.CP)
			if er != nil {
				return built, er
			}
			liveTransport := &http.Transport{TLSClientConfig: liveTLS, ResponseHeaderTimeout: 10 * time.Second, IdleConnTimeout: 30 * time.Second}
			built.transports = append(built.transports, liveTransport)
			liveClient := &http.Client{Transport: liveTransport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			publisher := &shaleadapter.FilePublisher{ConfigPath: entry.ConfigPath, StateDir: filepath.Join(stateDir, "deployments"), Scope: entry.Scope, Authority: &shaleadapter.RPCAuthority{Conn: connection, LiveHTTPClient: liveClient}, Restart: shaleadapter.CommandRestart(entry.Restart)}
			verifier := &media.Verifier{AuthProvider: provider, Ffmpeg: config.Ffmpeg, Ffprobe: config.Ffprobe, NetworkScope: network.ID}
			target.Executor = &provision.Engine{Store: store, Auth: provider, Inspector: inspector, Configurator: configurator, Bootstrapper: bootstrapper, Verifier: scopedVerifier{Network: network, Verifier: verifier}, Publisher: publisher}
			target.Checkpoint = func(ctx context.Context, q provision.Request) (provision.Run, error) { return store.Load(ctx, q.ID) }
		}
		for _, prior := range options.Targets {
			if prior.Scope == target.Scope {
				return built, camera.ErrConflict
			}
		}
		options.Targets = append(options.Targets, target)
	}
	built.Host, err = New(options)
	return built, err
}

func networkByID(networks []Network, id string) (Network, error) {
	for _, network := range networks {
		if network.ID == id {
			return network, nil
		}
	}
	return Network{}, camera.ErrForbidden
}

func ReadToken(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 4096 {
		return "", errors.New("provisioner token requires a private regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", errors.New("cannot read provisioner token")
	}
	token := strings.TrimSpace(string(data))
	if len(token) < 32 || strings.ContainsAny(token, "\r\n\t ") {
		return "", errors.New("provisioner token must contain at least 32 non-whitespace bytes")
	}
	return token, nil
}

func connectionTLS(c ConnectionConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		data, err := os.ReadFile(c.CAFile)
		if err != nil || !roots.AppendCertsFromPEM(data) {
			return nil, camera.ErrInvalid
		}
		tlsConfig.RootCAs = roots
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return nil, camera.ErrInvalid
	}
	if c.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, camera.ErrInvalid
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

func validateConnection(c ConnectionConfig) (*url.URL, error) {
	u, err := url.Parse(c.URL)
	if err != nil || u.User != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, camera.ErrInvalid
	}
	if u.Scheme != "https" && !(c.AllowLoopbackHTTP && u.Scheme == "http" && loopbackURL(c.URL)) {
		return nil, camera.ErrForbidden
	}
	return u, nil
}

func connectionHTTP(c ConnectionConfig) (*http.Client, string, error) {
	if _, err := validateConnection(c); err != nil {
		return nil, "", err
	}
	tlsConfig, err := connectionTLS(c)
	if err != nil {
		return nil, "", err
	}
	token, err := ReadToken(c.TokenFile)
	if err != nil {
		return nil, "", err
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig, ResponseHeaderTimeout: 5 * time.Minute, IdleConnTimeout: 30 * time.Second}
	return &http.Client{Transport: transport, Timeout: 5 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, token, nil
}

func connectionGRPC(c ConnectionConfig) (*grpc.ClientConn, error) {
	u, err := validateConnection(c)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := connectionTLS(c)
	if err != nil {
		return nil, err
	}
	tlsConfig.ServerName = u.Hostname()
	var transport credentials.TransportCredentials = credentials.NewTLS(tlsConfig)
	if u.Scheme == "http" {
		transport = insecure.NewCredentials()
	}
	options := []grpc.DialOption{grpc.WithTransportCredentials(transport)}
	if c.TokenFile != "" {
		token, err := ReadToken(c.TokenFile)
		if err != nil {
			return nil, err
		}
		options = append(options, grpc.WithPerRPCCredentials(bearerCredential{Token: token, Secure: u.Scheme != "http"}))
	} else if c.CertFile == "" {
		return nil, camera.ErrAuthRequired
	}
	return grpc.NewClient(u.Host, options...)
}

type bearerCredential struct {
	Token  string
	Secure bool
}

func (b bearerCredential) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.Token}, nil
}
func (b bearerCredential) RequireTransportSecurity() bool { return b.Secure }

// Serve starts only the provisioner; recording Producers have independent
// lifetimes. Worker mode uses the same host with only local authorized targets.
func (b *Built) Serve(ctx context.Context, ready func(net.Addr)) error {
	address := b.Config.Listen
	if address == "" {
		address = "127.0.0.1:7415"
	}
	if (b.Config.CertFile == "") != (b.Config.KeyFile == "") {
		return camera.ErrInvalid
	}
	if b.Config.CertFile == "" && !(b.Config.AllowLoopbackHTTP && loopbackAddress(address)) {
		return camera.ErrForbidden
	}
	handler, err := b.Host.Handler()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.New("cannot listen for camera provisioning")
	}
	defer listener.Close()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 6 * time.Minute, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10}
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return b.Host.Run(ctx) })
	group.Go(func() error {
		if ready != nil {
			ready(listener.Addr())
		}
		var err error
		if b.Config.CertFile != "" {
			err = server.ServeTLS(listener, b.Config.CertFile, b.Config.KeyFile)
		} else {
			err = server.Serve(listener)
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	group.Go(func() error {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			server.Close()
		}
		return nil
	})
	return group.Wait()
}

// MarshalConfig is useful for CLI fixtures without relying on global Shale
// configuration. The serialized form contains paths/references only.
func MarshalConfig(c Config) ([]byte, error) { return json.Marshal(c) }

// Execute is the embedded/CLI path through exactly the same configured worker
// factory. It opens no listener and makes no host-level background job.
func (b *Built) Execute(ctx context.Context, q provision.Request, apply bool) (provision.Run, error) {
	run, _, err := b.Host.execute(ctx, Job{Kind: "enroll", Request: q, Apply: apply})
	return run, err
}

func (b *Built) Discover(ctx context.Context, id string) ([]camera.Observation, error) {
	network, err := networkByID(b.Config.Networks, id)
	if err != nil {
		return nil, err
	}
	return b.Host.discover(ctx, camera.DiscoveryScope{ID: network.ID, Interface: network.Interface, LocalAddress: network.LocalAddress, Seeds: network.Seeds, Timeout: 2 * time.Second, Multicast: network.Multicast})
}

// A configured compatibility override is bounded to its camera endpoint. Other
// cameras assigned to this Producer still use the standard ONVIF implementation.
type selectedDriver struct {
	Baseline *onvif.Client
	Vendor   *tplink.Driver
	Endpoint string
}

func (d *selectedDriver) matches(hint camera.DeviceHint) bool {
	web, err := url.Parse(d.Endpoint)
	if err != nil {
		return false
	}
	for _, endpoint := range hint.Endpoints {
		u, err := url.Parse(endpoint.URL)
		if err == nil && u.Hostname() == web.Hostname() {
			return true
		}
	}
	return false
}
func (d *selectedDriver) Inspect(ctx context.Context, h camera.DeviceHint, l camera.CredentialLease) (camera.Inspection, error) {
	if d.matches(h) {
		return d.Vendor.Inspect(ctx, h, l)
	}
	return d.Baseline.Inspect(ctx, h, l)
}
func (d *selectedDriver) Plan(ctx context.Context, i camera.Inspection, s camera.DesiredState) (camera.ChangePlan, error) {
	if d.matches(i.Device) {
		return d.Vendor.Plan(ctx, i, s)
	}
	return d.Baseline.Plan(ctx, i, s)
}
func (d *selectedDriver) Apply(ctx context.Context, h camera.DeviceHint, l camera.CredentialLease, p camera.ChangePlan) (camera.CurrentState, error) {
	if d.matches(h) {
		return d.Vendor.Apply(ctx, h, l, p)
	}
	return d.Baseline.Apply(ctx, h, l, p)
}
func (d *selectedDriver) Activation(ctx context.Context, h camera.DeviceHint) (camera.ActivationState, error) {
	if d.matches(h) {
		return d.Vendor.Activation(ctx, h)
	}
	return camera.ActivationUnknown, nil
}
func (d *selectedDriver) Activate(ctx context.Context, h camera.DeviceHint, l camera.CredentialLease) error {
	if d.matches(h) {
		return d.Vendor.Activate(ctx, h, l)
	}
	return camera.ErrManualActivation
}
