package mediaprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/internal/token"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func testOptions(t *testing.T) Options {
	t.Helper()
	o, err := (Options{}).normalized()
	require.NoError(t, err)
	return o
}
func nal(data ...byte) []byte { return append([]byte{0, 0, 0, 1}, data...) }

func TestMalformedNALPacketsAndBudgets(t *testing.T) {
	for name, payload := range map[string][]byte{"empty": nil, "forbidden": {0x81, 1}, "zero-stap": {24, 0, 0}, "short-stap": {24, 0}, "stap-tail": {24, 0, 1, 0x67, 0}, "nested-stap": {24, 0, 1, 28}, "empty-fu": {28, 0x85}, "start-end-fu": {28, 0xc5, 1}, "reserved-fu": {28, 0xa5, 1}, "unsupported": {30, 1}} {
		t.Run(name, func(t *testing.T) {
			r := newReceiver(testOptions(t))
			got, err := r.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: 1, Timestamp: 1, Marker: true}, Payload: payload}, time.Now())
			require.ErrorIs(t, err, ErrMedia)
			require.Nil(t, got)
		})
	}
	o := testOptions(t)
	o.MaxBytes = 1024
	got, err := newReceiver(o).push(&rtp.Packet{Payload: bytes.Repeat([]byte{1}, 1025)}, time.Now())
	require.ErrorIs(t, err, ErrLimit)
	require.Nil(t, got)
	o = testOptions(t)
	o.MaxPackets = 2
	r := newReceiver(o)
	for i := range 3 {
		got, err = r.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(i + 1), Timestamp: 1}, Payload: []byte{0x67, 1}}, time.Now())
	}
	require.ErrorIs(t, err, ErrLimit)
	require.Nil(t, got)
}

func TestRetainedParametersProgressionAndBrokenChain(t *testing.T) {
	c := chain{target: 2, limit: 1024}
	at := time.Now()
	ready, err := c.add(append(nal(0x67, 1), nal(0x68, 1)...), 100, at)
	require.NoError(t, err)
	require.False(t, ready)
	ready, err = c.add(nal(0x65, 1), ^uint32(0)-1000, at.Add(time.Millisecond))
	require.NoError(t, err)
	require.False(t, ready)
	ready, err = c.add(nal(0x41, 1), 2000, at.Add(2*time.Millisecond))
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, 1, c.evidence.RTPAdvances)
	// A repeated RTP clock cannot manufacture a second advancing frame.
	ready, err = c.add(nal(0x41, 1), 2000, at.Add(3*time.Millisecond))
	require.NoError(t, err)
	require.False(t, ready)
	require.Zero(t, c.evidence.AccessUnits)
	c.reset()
	ready, err = c.add(nal(0x41, 1), 3000, at)
	require.NoError(t, err)
	require.False(t, ready)
	require.Empty(t, c.data)
	tiny := chain{target: 2, limit: 8}
	_, err = tiny.add(append(append(nal(0x67, 1), nal(0x68, 1)...), nal(0x65, 1)...), 1, at)
	require.ErrorIs(t, err, ErrLimit)
}

func TestFUCompletenessUsesEveryOriginalHeader(t *testing.T) {
	r := newReceiver(testOptions(t))
	r.facts[1] = packetFacts{timestamp: 10, kind: 28, start: true}
	r.facts[2] = packetFacts{timestamp: 10, kind: 1, marker: true}
	sample := &media.Sample{PacketTimestamp: 10, RTPHeaders: []*rtp.Header{{SequenceNumber: 1, Timestamp: 10}, {SequenceNumber: 2, Timestamp: 10, Marker: true}}}
	complete, _ := r.complete(sample)
	require.False(t, complete, "a whole later NAL cannot complete an unfinished FU")
	r.facts[2] = packetFacts{timestamp: 10, kind: 28, end: true, marker: true}
	complete, _ = r.complete(sample)
	require.True(t, complete)
	changed := r.facts[2]
	changed.nal = 5
	r.facts[2] = changed
	complete, _ = r.complete(sample)
	require.False(t, complete, "FU identity must stay fixed across all fragments")
	sample.RTPHeaders[1].SequenceNumber = 3
	complete, _ = r.complete(sample)
	require.False(t, complete, "all packet sequence numbers must be contiguous")
}

func TestDecodedPixelsRequireCompletePayloadAndBoundDimensions(t *testing.T) {
	p := &pixelFrames{maxFrames: 2, maxPixels: 16}
	_, err := p.Write([]byte("YUV4MPEG2 W4 H4 Cmono\nFRAME\n"))
	require.NoError(t, err)
	_, err = p.Write(make([]byte, 15))
	require.NoError(t, err)
	require.Zero(t, p.frames, "EOF here would be a truncated pixel frame")
	_, err = p.Write([]byte{0})
	require.NoError(t, err)
	require.Equal(t, 1, p.frames)
	_, err = p.Write(append([]byte("FRAME\n"), make([]byte, 16)...))
	require.NoError(t, err)
	require.Equal(t, 2, p.frames)
	require.True(t, p.last.After(p.first))
	_, err = p.Write([]byte("FRAME\n"))
	require.ErrorIs(t, err, ErrLimit)
	for _, header := range []string{"YUV4MPEG2 W8193 H1 Cmono\n", "YUV4MPEG2 W999999999999999999999 H2 Cmono\n", "YUV4MPEG2 W4 H5 Cmono\n", "YUV4MPEG2 W4 H4 C420\n"} {
		_, err = (&pixelFrames{maxFrames: 2, maxPixels: 16}).Write([]byte(header))
		require.Error(t, err)
	}
	_, err = (&pixelFrames{maxFrames: 2, maxPixels: 16}).Write(bytes.Repeat([]byte{'x'}, 4097))
	require.ErrorIs(t, err, ErrLimit)
}

func TestNALMarkersAloneNeverProduceDecodeEvidence(t *testing.T) {
	o := testOptions(t)
	if _, err := exec.LookPath(o.FFmpeg); err != nil {
		t.Skip("ffmpeg unavailable")
	}
	data := append(append(append(nal(0x67, 1), nal(0x68, 1)...), nal(0x65, 1)...), nal(0x41, 1)...)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := decode(ctx, o, data)
	require.ErrorIs(t, err, ErrDecode)
	require.Nil(t, result)
}

func TestDecodeCancellationAndFixedErrors(t *testing.T) {
	o := testOptions(t)
	o.FFmpeg = filepath.Join(t.TempDir(), "synthetic-secret-decoder")
	require.NoError(t, os.WriteFile(o.FFmpeg, []byte("#!/bin/sh\nexec sleep 30\n"), 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := decode(ctx, o, []byte{0})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotContains(t, err.Error(), "synthetic-secret")
	o.FFmpeg = filepath.Join(t.TempDir(), "missing-private-program")
	_, err = decode(context.Background(), o, nil)
	require.ErrorIs(t, err, ErrDecoderUnavailable)
	require.NotContains(t, err.Error(), "missing-private")
}

func TestEndpointRedactionAndValidation(t *testing.T) {
	e := Endpoint{URL: "https://relay.invalid/whep/private-source", Token: "private-view-capability"}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%x", "%X", "%d", "%o", "%f", "%q"} {
		require.Equal(t, redactedEndpoint, fmt.Sprintf(format, e))
	}
	b, err := json.Marshal(e)
	require.NoError(t, err)
	require.Equal(t, `"`+redactedEndpoint+`"`, string(b))
	b, err = yaml.Marshal(e)
	require.NoError(t, err)
	require.NotContains(t, string(b), e.URL)
	require.NotContains(t, string(b), e.Token)
	for _, bad := range []Endpoint{{URL: e.URL}, {URL: "https://user:private@relay/whep", Token: e.Token}, {URL: "file:///private", Token: e.Token}, {URL: e.URL, Token: "private\r\nheader"}, {URL: e.URL, Token: strings.Repeat("x", 16385)}} {
		_, err := Probe(context.Background(), Options{}, bad)
		require.ErrorIs(t, err, ErrEndpoint)
		require.NotContains(t, err.Error(), "private")
	}
	_, err = Probe(context.Background(), Options{Frames: 1}, e)
	require.ErrorIs(t, err, ErrOptions)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := Probe(ctx, Options{}, e)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, Evidence{}, got, "a canceled grant emits no media evidence")
}

func TestWHEPCancellationStopsReaderAndRequest(t *testing.T) {
	entered, done, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		io.Copy(io.Discard, r.Body)
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() { close(release); server.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		e, err := Probe(ctx, Options{Timeout: 2 * time.Second}, Endpoint{URL: server.URL, Token: "private-token"})
		if e != (Evidence{}) {
			finished <- errors.New("failed probe returned evidence")
			return
		}
		finished <- err
	}()
	select {
	case <-entered:
	case err := <-finished:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("probe outlived cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request outlived cancellation")
	}
}

func TestPostAndCleanupNeverFollowRedirects(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		for _, local := range []bool{false, true} {
			t.Run(fmt.Sprintf("cleanup_%t/local_%t", cleanup, local), func(t *testing.T) {
				var redirected, deletes, callerPolicy atomic.Int32
				sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1); w.WriteHeader(204) }))
				defer sink.Close()
				redirect := func(w http.ResponseWriter) {
					target := sink.URL
					if local {
						target = "/unexpected"
					}
					w.Header().Set("Location", target)
					w.WriteHeader(307)
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/":
						if !cleanup {
							redirect(w)
							return
						}
						w.Header().Set("Location", "/session")
						w.WriteHeader(201)
						io.WriteString(w, "invalid-sdp")
					case "/session":
						if r.Method == http.MethodDelete && r.Header.Get("Authorization") == token.Scheme+" private-token" {
							deletes.Add(1)
						}
						redirect(w)
					default:
						redirected.Add(1)
						w.WriteHeader(204)
					}
				}))
				defer server.Close()
				client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { callerPolicy.Add(1); return nil }}
				evidence, err := Probe(context.Background(), Options{Timeout: 2 * time.Second, HTTPClient: client}, Endpoint{URL: server.URL, Token: "private-token"})
				require.ErrorIs(t, err, ErrNegotiation)
				require.Equal(t, Evidence{}, evidence)
				require.Zero(t, redirected.Load())
				require.Zero(t, callerPolicy.Load())
				if cleanup {
					require.Equal(t, int32(1), deletes.Load())
				}
				require.NoError(t, client.CheckRedirect(nil, nil))
				require.Equal(t, int32(1), callerPolicy.Load())
			})
		}
	}
}
