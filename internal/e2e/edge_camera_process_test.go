package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode"

	camapi "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratest"
	"golang.org/x/sys/unix"
)

const edgeCameraProcessOptIn = "SHALE_EDGE_CAMERA_PROCESS"

// TestEdgeCameraProcess is a subprocess fixture, not an assembled E2E test.
// Launch the compiled test binary with SHALE_EDGE_CAMERA_PROCESS=1 and
// -test.run '^TestEdgeCameraProcess$'. A private stdin JSONL stream owns all
// controls; the original stdout carries only credential-free JSONL responses.
// Testing diagnostics and PASS/FAIL use stderr. No credential belongs in argv,
// environment, public status, error text or endpoint userinfo.
//
// The first request is version=1, op=start, a positive numeric id, an assigned
// IPv4 ip/interface, profile, up to eight modes, expected username/password and
// active. Identity is optional and remains fixed across rebind. Later operations
// are status, rebind, disconnect, set_active, lose_activation_response,
// hold_configuration, release_configuration and stop. Hold acceptance/settlement
// are polled through status. Rebind changes sockets only; the coordinator owns
// DHCP/interface setup. EOF, stop and SIGTERM/SIGINT release holds and close all
// listeners. Existing RTSP sessions keep their immutable profile until closed.
// Synthetic TP-Link activation validates its existing test wire exchange and
// thereafter checks the prepared credential; it does not decrypt firmware RSA
// ciphertext or model an arbitrary generated-password lifecycle.
func TestEdgeCameraProcess(t *testing.T) {
	if os.Getenv(edgeCameraProcessOptIn) != "1" {
		t.Skip("opt-in isolated camera subprocess")
	}
	fd, err := unix.Dup(int(os.Stdout.Fd()))
	if err != nil {
		t.Fatal("prepare camera protocol output")
	}
	unix.CloseOnExec(fd)
	public := os.NewFile(uintptr(fd), "camera-public-json")
	defer public.Close()
	if unix.Dup2(int(os.Stderr.Fd()), int(os.Stdout.Fd())) != nil {
		t.Fatal("separate camera diagnostics")
	}
	ctx, cancel := signal.NotifyContext(t.Context(), syscall.SIGTERM, os.Interrupt)
	defer cancel()
	if err := runEdgeCameraProcess(t, ctx, os.Stdin, public); err != nil {
		t.Error("camera process stopped after a protocol or fixture failure")
	}
}

type edgeCameraRequest struct {
	Version   int                       `json:"version"`
	ID        int64                     `json:"id"`
	Op        string                    `json:"op"`
	IP        string                    `json:"ip,omitempty"`
	Interface string                    `json:"interface,omitempty"`
	Profile   camapi.MediaProfile       `json:"profile,omitempty"`
	Modes     []camapi.MediaProfile     `json:"modes,omitempty"`
	Identity  cameratest.CameraIdentity `json:"identity,omitempty"`
	Username  string                    `json:"username,omitempty"`
	Password  string                    `json:"password,omitempty"`
	Active    *bool                     `json:"active,omitempty"`
}

type edgeCameraStatus struct {
	Endpoints           cameratest.CameraEndpoints `json:"endpoints"`
	Identity            cameratest.CameraIdentity  `json:"identity"`
	Profile             camapi.MediaProfile        `json:"profile"`
	Active              bool                       `json:"active"`
	Sessions            int                        `json:"sessions"`
	ConfigurationWrites int                        `json:"configuration_writes"`
	Activations         int                        `json:"activations"`
	Held                bool                       `json:"held"`
	Accepted            bool                       `json:"accepted"`
	Settled             bool                       `json:"settled"`
}

type edgeCameraResponse struct {
	Version int               `json:"version"`
	ID      int64             `json:"id,omitempty"`
	Event   string            `json:"event"`
	Code    string            `json:"code,omitempty"`
	Status  *edgeCameraStatus `json:"status,omitempty"`
}

type edgeCameraHold struct {
	accepted, settled <-chan struct{}
	release           func()
}

func edgeCameraClosed(ch <-chan struct{}) bool {
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func edgeCameraString(value string, limit int) bool {
	return len(value) <= limit && !strings.ContainsFunc(value, unicode.IsControl)
}

func edgeCameraProfile(profile camapi.MediaProfile) bool {
	return (profile.Codec == "" || profile.Codec == "h264") && profile.Width >= 16 && profile.Width <= 1920 &&
		profile.Height >= 16 && profile.Height <= 1080 && profile.Width%2 == 0 && profile.Height%2 == 0 &&
		profile.FPS >= 1 && profile.FPS <= 60 && !math.IsNaN(profile.FPS) && !math.IsInf(profile.FPS, 0) &&
		profile.Bitrate >= 32_000 && profile.Bitrate <= 8_000_000 && profile.GOP >= 0 && profile.GOP <= int(math.Ceil(2*profile.FPS)) &&
		edgeCameraString(profile.Token, 64) && edgeCameraString(profile.Name, 64) && profile.StreamURI == "" && len(profile.Evidence) == 0
}

func edgeCameraRequestFrom(line []byte) (edgeCameraRequest, bool) {
	var request edgeCameraRequest
	if len(line) >= 64<<10 {
		return request, false
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF || request.Version != 1 || request.ID <= 0 || !edgeCameraString(request.Op, 32) {
		return edgeCameraRequest{}, false
	}
	allowed := map[string]bool{"version": true, "id": true, "op": true}
	switch request.Op {
	case "start":
		for _, name := range []string{"ip", "interface", "profile", "modes", "identity", "username", "password", "active"} {
			allowed[name] = true
		}
	case "rebind":
		allowed["ip"], allowed["interface"] = true, true
	case "set_active":
		allowed["active"] = true
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(line, &fields) != nil {
		return edgeCameraRequest{}, false
	}
	for name := range fields {
		if !allowed[name] {
			return edgeCameraRequest{}, false
		}
	}
	return request, true
}

func edgeCameraAddress(ip, name string) bool {
	return len(ip) <= 15 && net.ParseIP(ip).To4() != nil && name != "" && edgeCameraString(name, 15) &&
		!strings.ContainsAny(name, " /\\")
}

func edgeCameraStartValid(request edgeCameraRequest) bool {
	if request.Op != "start" || !edgeCameraAddress(request.IP, request.Interface) || request.Active == nil ||
		request.Username == "" || request.Password == "" || !edgeCameraString(request.Username, 64) ||
		strings.Contains(request.Username, ":") || !edgeCameraString(request.Password, 256) || !edgeCameraProfile(request.Profile) || len(request.Modes) > 8 {
		return false
	}
	for _, mode := range request.Modes {
		if !edgeCameraProfile(mode) {
			return false
		}
	}
	identity := request.Identity
	for _, value := range []string{identity.Manufacturer, identity.Model, identity.Hardware, identity.Firmware, identity.Serial, identity.MAC, identity.DiscoveryID} {
		if !edgeCameraString(value, 128) {
			return false
		}
	}
	return true
}

func runEdgeCameraProcess(t *testing.T, ctx context.Context, input io.Reader, output io.Writer) error {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	encoder := json.NewEncoder(output)
	respond := func(id int64, event, code string, status *edgeCameraStatus) error {
		return encoder.Encode(edgeCameraResponse{Version: 1, ID: id, Event: event, Code: code, Status: status})
	}
	requests := make(chan []byte)
	readError := make(chan error, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 4096), 64<<10)
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case requests <- line:
			case <-ctx.Done():
				return
			}
		}
		readError <- scanner.Err()
		close(requests)
	}()
	var fixture *cameratest.Camera
	var closeProbe func()
	var hold edgeCameraHold
	defer func() {
		cancel()
		// This process owns private stdin. Blocking inputs must be closers;
		// finite non-closers also leave a pending send through cancellation.
		if closer, ok := input.(io.Closer); ok {
			_ = closer.Close()
		}
		<-readerDone
		if hold.release != nil {
			hold.release()
		}
		if closeProbe != nil {
			closeProbe()
		}
		if fixture != nil {
			fixture.Close()
		}
	}()
	status := func() *edgeCameraStatus {
		configuration, activations := fixture.ONVIF.Writes()
		hint := fixture.ONVIF.Hint("synthetic-process")
		return &edgeCameraStatus{Endpoints: fixture.Endpoints(),
			Identity: cameratest.CameraIdentity{Manufacturer: hint.Manufacturer, Model: hint.Model, Hardware: hint.Hardware,
				Firmware: hint.Firmware, Serial: hint.Serial, MAC: hint.MAC, DiscoveryID: hint.DiscoveryID},
			Profile: fixture.Media.Profile(), Active: fixture.ONVIF.Active(), Sessions: fixture.Media.Sessions(),
			ConfigurationWrites: configuration, Activations: activations, Held: hold.release != nil,
			Accepted: edgeCameraClosed(hold.accepted), Settled: edgeCameraClosed(hold.settled)}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case line, ok := <-requests:
			if !ok {
				if <-readError != nil {
					_ = respond(0, "error", "invalid-input", nil)
					return errors.New("invalid camera input")
				}
				return nil
			}
			request, valid := edgeCameraRequestFrom(line)
			if !valid {
				if err := respond(0, "error", "invalid-input", nil); err != nil {
					return err
				}
				continue
			}
			if fixture == nil {
				if !edgeCameraStartValid(request) {
					if err := respond(request.ID, "error", "invalid-start", nil); err != nil {
						return err
					}
					continue
				}
				identity := request.Identity
				// Force customIdentity so the default stable serial/MAC/EPR are public.
				if identity.DiscoveryID == "" {
					identity.DiscoveryID = "urn:uuid:00000000-0000-4000-8000-000000000001"
				}
				fixture = cameratest.ServeCamera(t, cameratest.CameraOptions{IP: request.IP, RTSPPath: "/main",
					Profile: request.Profile, Modes: request.Modes, Identity: identity, SetupTimeout: 15 * time.Second})
				fixture.RequireCredentials(request.Username, request.Password)
				fixture.ONVIF.SetUnactivated(!*request.Active)
				var err error
				closeProbe, err = fixture.ONVIF.ListenMulticastProbe(request.Interface, request.IP)
				if err != nil {
					_ = respond(request.ID, "error", "discovery-start-failed", nil)
					return errors.New("camera discovery start failed")
				}
				if err = respond(request.ID, "ready", "", status()); err != nil {
					return err
				}
				continue
			}
			code := ""
			switch request.Op {
			case "status":
			case "rebind":
				if !edgeCameraAddress(request.IP, request.Interface) {
					code = "invalid-address"
					break
				}
				closeProbe()
				if _, err := fixture.Rebind(request.IP); err != nil {
					_ = respond(request.ID, "error", "rebind-failed", nil)
					return errors.New("camera rebind failed")
				}
				var err error
				closeProbe, err = fixture.ONVIF.ListenMulticastProbe(request.Interface, request.IP)
				if err != nil {
					_ = respond(request.ID, "error", "discovery-start-failed", nil)
					return errors.New("camera discovery rebind failed")
				}
			case "disconnect":
				fixture.Media.Disconnect()
			case "set_active":
				if request.Active == nil {
					code = "missing-active"
				} else {
					fixture.ONVIF.SetUnactivated(!*request.Active)
				}
			case "lose_activation_response":
				fixture.ONVIF.LoseActivationResponseOnce()
			case "hold_configuration":
				if hold.release != nil && !edgeCameraClosed(hold.settled) {
					code = "configuration-already-held"
				} else {
					hold.accepted, hold.release, hold.settled = fixture.ONVIF.HoldConfigurationResponseOnce(t)
				}
			case "release_configuration":
				if !edgeCameraClosed(hold.accepted) {
					code = "configuration-not-accepted"
				} else {
					hold.release()
				}
			case "stop":
				return respond(request.ID, "stopped", "", nil)
			default:
				code = "unknown-command"
			}
			event := "status"
			if code != "" {
				event = "error"
			}
			if err := respond(request.ID, event, code, status()); err != nil {
				return err
			}
		}
	}
}

func TestEdgeCameraProcessInputBoundary(t *testing.T) {
	active := false
	const secret = "fixture-private-password-sentinel"
	base := edgeCameraRequest{Version: 1, ID: 1, Op: "start", IP: "192.168.250.20", Interface: "eth0",
		Username: "fixture-private-user", Password: secret, Active: &active,
		Profile: camapi.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 64_000, GOP: 10}}
	if !edgeCameraStartValid(base) {
		t.Fatal("valid private start was rejected")
	}
	for _, change := range []func(*edgeCameraRequest){
		func(r *edgeCameraRequest) { r.Password = strings.Repeat("x", 257) },
		func(r *edgeCameraRequest) { r.Username = "user:invalid" },
		func(r *edgeCameraRequest) { r.Modes = make([]camapi.MediaProfile, 9) },
		func(r *edgeCameraRequest) { r.Profile.StreamURI = "rtsp://user:" + secret + "@192.168.250.20/main" },
		func(r *edgeCameraRequest) { r.Profile.Width = 4096 },
		func(r *edgeCameraRequest) { r.Identity.Serial = strings.Repeat("x", 129) },
		func(r *edgeCameraRequest) { r.Active = nil },
		func(r *edgeCameraRequest) { r.IP = secret },
	} {
		request := base
		change(&request)
		if edgeCameraStartValid(request) {
			t.Fatal("invalid private start was accepted")
		}
	}
	input := `{"version":1,"id":1,"op":"status","password":"` + secret + `"}` + "\n" +
		`{"version":1,"id":2,"op":"` + secret + `"}` + "\n" +
		`{"version":1,"id":3,"op":"start","password":"` + secret + `"}` + "\n" +
		`{"version":1,"id":4,"op":"status","` + secret + `":true}` + "\n"
	var output bytes.Buffer
	if err := runEdgeCameraProcess(t, t.Context(), strings.NewReader(input), &output); err != nil {
		t.Fatal("bounded invalid input did not close cleanly")
	}
	if strings.Contains(output.String(), secret) || strings.Contains(output.String(), "password") {
		t.Fatal("private input appeared in public output")
	}
	scanner := bufio.NewScanner(&output)
	count := 0
	for scanner.Scan() {
		var response edgeCameraResponse
		if json.Unmarshal(scanner.Bytes(), &response) != nil || response.Event != "error" || response.Status != nil {
			t.Fatal("invalid input did not produce a bounded public error")
		}
		count++
	}
	if count != 4 {
		t.Fatal("unexpected public response count")
	}
	output.Reset()
	if err := runEdgeCameraProcess(t, t.Context(), strings.NewReader(strings.Repeat("x", 64<<10)), &output); err == nil {
		t.Fatal("oversized input was accepted")
	}
	if output.String() != "{\"version\":1,\"event\":\"error\",\"code\":\"invalid-input\"}\n" {
		t.Fatal("oversized input was echoed")
	}
}

func TestEdgeCameraProcessReaderCancellation(t *testing.T) {
	t.Run("owned-pipe", func(t *testing.T) {
		reader, writer := io.Pipe()
		defer writer.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var output bytes.Buffer
		done := make(chan error, 1)
		go func() { done <- runEdgeCameraProcess(t, ctx, reader, &output) }()
		// The first complete request is consumed before the scanner blocks
		// again on its private pipe. Cancellation must close and join it.
		if _, err := io.WriteString(writer, "{\"version\":1,\"id\":1,\"op\":\"status\"}\n"); err != nil {
			t.Fatal("private request was not consumed")
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal("private pipe cancellation failed")
			}
		case <-time.After(time.Second):
			t.Fatal("private pipe reader did not join")
		}
		if _, err := writer.Write([]byte("private")); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal("private input was not closed")
		}
	})
	t.Run("finite-non-closer", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var output bytes.Buffer
		input := strings.NewReader(strings.Repeat("{\"version\":1,\"id\":1,\"op\":\"status\"}\n", 64))
		if err := runEdgeCameraProcess(t, ctx, input, &output); err != nil {
			t.Fatal("finite input cancellation failed")
		}
	})
}
