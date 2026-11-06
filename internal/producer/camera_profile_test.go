package producer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/media"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type profileInspector struct {
	calls   int
	bitrate int64
}

func (f *profileInspector) Inspect(_ context.Context, _ camera.DeviceHint, _ camera.CredentialLease) (camera.Inspection, error) {
	f.calls++
	return camera.Inspection{Current: camera.CurrentState{Profiles: []camera.MediaProfile{{Token: "main", Codec: "h264", Width: 1280, Height: 720, FPS: 30, Bitrate: f.bitrate, StreamURI: "rtsp://camera/stream"}}}}, nil
}

type fixedVerifier struct {
	calls    int
	complete bool
}

func (f *fixedVerifier) Verify(context.Context, camera.MediaRequest) (camera.VerifiedState, error) {
	f.calls++
	return camera.VerifiedState{Complete: f.complete, Profile: camera.MediaProfile{Codec: "h264", Width: 1280, Height: 720, FPS: 30, Bitrate: 1_500_000}}, nil
}
func TestOnvifCeilingFollowsEveryNegotiation(t *testing.T) {
	ctx := context.Background()
	cp := &fakeCP{}
	lis, e := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, e)
	server := grpc.NewServer()
	api.RegisterSetServiceServer(server, fakeSets{fakeCP: cp})
	go server.Serve(lis)
	defer server.Stop()
	conn, e := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, e)
	defer conn.Close()
	inspector := &profileInspector{bitrate: 2_000_000}
	p, e := New(Config{Sources: []SourceConfig{{Alias: "camera", Input: "rtsp://camera/stream", Onvif: &OnvifConfig{Endpoint: "http://camera/onvif/device_service", ProfileToken: "main"}, MaxBitrateOnvif: true}}, OnvifInspector: inspector, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, e)
	p.conn.Store(conn)
	p.id = pdid.New(DomProducer)
	p.set = api.Set_builder{Id: pdid.New(8).Bytes()}.Build()
	s := p.order[0]
	s.row = api.Source_builder{Id: pdid.New(9).Bytes()}.Build()
	require.NoError(t, p.negotiate(ctx))
	inspector.bitrate = 4_000_000
	require.NoError(t, p.negotiate(ctx))
	require.Equal(t, 2, inspector.calls)
	require.Len(t, cp.proposed, 2)
	require.Equal(t, int64(2_000_000)+s.cfg.audioShare(), cp.proposed[0])
	require.Equal(t, int64(4_000_000)+s.cfg.audioShare(), cp.proposed[1])
}
func TestRTSPCeilingWithoutDeclaredProfile(t *testing.T) {
	verifier := &fixedVerifier{complete: true}
	p, e := New(Config{Sources: []SourceConfig{{Alias: "camera", Input: "rtsp://camera/stream"}}, MediaVerifier: verifier})
	require.NoError(t, e)
	require.NoError(t, p.refreshCeiling(context.Background(), p.order[0]))
	require.Greater(t, p.startingCeiling(p.order[0]), int64(1_500_000))
	require.Equal(t, 1, verifier.calls)
	require.NoError(t, p.refreshCeiling(context.Background(), p.order[0]))
	require.Equal(t, 1, verifier.calls, "initial RTSP guess should not open a session on every heartbeat")
	verifier = &fixedVerifier{complete: false}
	p, e = New(Config{Sources: []SourceConfig{{Alias: "camera", Input: "rtsp://camera/stream"}}, MediaVerifier: verifier})
	require.NoError(t, e)
	require.ErrorIs(t, p.refreshCeiling(context.Background(), p.order[0]), media.ErrIncomplete)
}

type countingProvider struct {
	camera.AuthProvider
	mu   sync.Mutex
	refs []camera.CredentialRef
}

func (p *countingProvider) Acquire(ctx context.Context, r camera.AuthRequest) (camera.CredentialLease, error) {
	p.mu.Lock()
	if r.ExistingRef != nil {
		p.refs = append(p.refs, *r.ExistingRef)
	}
	p.mu.Unlock()
	return p.AuthProvider.Acquire(ctx, r)
}
func TestCaptureReferenceRetryAndSecretDiagnostics(t *testing.T) {
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
	fp := credential.FileProvider{Dir: filepath.Join(t.TempDir(), "private")}
	material := camera.CredentialMaterial{Username: "camera-user", Password: "p:@/?% &word"}
	ref, e := fp.Prepare(context.Background(), scope, camera.AuthStreaming, "capture", material)
	require.NoError(t, e)
	binary := filepath.Join(t.TempDir(), "tool")
	require.NoError(t, os.WriteFile(binary, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >&2\nexit 1\n"), 0700))
	var logs bytes.Buffer
	provider := &countingProvider{AuthProvider: fp}
	c := &Capture{Ffmpeg: binary, AuthProvider: provider, Source: SourceConfig{Alias: "camera", Input: "rtsp://host/stream", InputCredential: &ref, Scope: scope, Encoder: "copy"}, Log: slog.New(slog.NewTextHandler(&logs, nil)), Profile: func() (int64, time.Duration) { return 2_000_000, time.Second }}
	for i := 0; i < 2; i++ {
		e = c.once(context.Background(), func(rd io.Reader) { io.Copy(io.Discard, rd) })
		require.Error(t, e)
	}
	// Drain the intentionally short stderr reader before inspecting diagnostics.
	deadline := time.Now().Add(time.Second)
	for c.LastError() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	provider.mu.Lock()
	require.Equal(t, []camera.CredentialRef{ref, ref}, provider.refs)
	provider.mu.Unlock()
	diagnostics := logs.String() + c.LastError() + e.Error()
	for _, secret := range []string{material.Password, url.QueryEscape(material.Password), url.UserPassword(material.Username, material.Password).String()} {
		require.NotContains(t, diagnostics, secret)
	}
	require.NotContains(t, c.Source.Input, "@")
	require.Equal(t, ref, *c.Source.InputCredential)
	wrong := scope
	wrong.NetworkID = "other"
	c.Source.Scope = wrong
	e = c.once(context.Background(), func(io.Reader) { t.Fatal("unauthorized capture started") })
	require.ErrorIs(t, e, camera.ErrForbidden)
}
func TestLegacyCaptureStderrRedactedBeforeObservers(t *testing.T) {
	var logs bytes.Buffer
	c := &Capture{Source: SourceConfig{Input: "rtsp://user:pa%40ss@host/stream"}, Log: slog.New(slog.NewTextHandler(&logs, nil))}
	observed := ""
	c.OnLine = func(line string) bool { observed = line; return false }
	c.scanStderr(strings.NewReader("failed rtsp://user:pa%40ss@host/stream; password pa@ss\n"))
	for _, text := range []string{logs.String(), observed, c.LastError()} {
		require.NotContains(t, text, "pa%40ss")
		require.NotContains(t, text, "pa@ss")
	}
	require.True(t, errors.Is(credential.SanitizeError(media.ErrMismatch, credential.NewSanitizer()), media.ErrMismatch))
}
