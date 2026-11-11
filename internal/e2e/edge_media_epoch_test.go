package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
	"github.com/stretchr/testify/require"
)

func TestEdgeMediaNativeEpochRequiresExact201Correlation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expected string
		returned []string
		status   int
		want     string
	}{
		{"missing", "capture-current", nil, http.StatusCreated, "epoch"},
		{"different", "capture-current", []string{"capture-previous"}, http.StatusCreated, "epoch"},
		{"case-sensitive", "capture-current", []string{"CAPTURE-CURRENT"}, http.StatusCreated, "epoch"},
		{"duplicate", "capture-current", []string{"capture-current", "capture-current"}, http.StatusCreated, "epoch"},
		{"matching-header-is-not-media", "capture-current", []string{"capture-current"}, http.StatusCreated, "SDP"},
		{"matching-header-needs-201", "capture-current", []string{"capture-current"}, http.StatusOK, "status"},
		{"legacy-empty", "", nil, http.StatusCreated, "SDP"},
		{"legacy-unbound-header", "", []string{"uncorrelated-header"}, http.StatusCreated, "SDP"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const capability = "private-native-view-token"
			var posts, deletes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, token.Scheme+" "+capability, r.Header.Get("Authorization"))
				if r.Method == http.MethodDelete && r.URL.Path == "/session" {
					deletes.Add(1)
					w.WriteHeader(http.StatusNoContent)
					return
				}
				posts.Add(1)
				w.Header().Set("Location", "/session")
				for _, value := range tc.returned {
					w.Header().Add("Shale-Capture-Epoch", value)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, "invalid-sdp")
			}))
			t.Cleanup(server.Close)
			live := api.LiveSource_builder{WhepUrl: server.URL + "/whep", ViewToken: capability, CaptureEpoch: tc.expected}.Build()
			evidence, err := qualifyWHEP(t.Context(), live, edgeMediaOptions{Timeout: time.Second, FFmpeg: "must-not-run-before-media"})
			require.Error(t, err)
			require.Equal(t, edgeMediaEvidence{}, evidence, "HTTP headers and SDP errors cannot prove AUs or decoded pixels")
			require.NotContains(t, err.Error(), capability)
			require.NotContains(t, err.Error(), server.URL)
			switch tc.want {
			case "epoch":
				require.ErrorIs(t, err, errEdgeCaptureEpoch)
			case "SDP":
				require.NotErrorIs(t, err, errEdgeCaptureEpoch)
				require.EqualError(t, err, "set WHEP remote description")
			case "status":
				require.EqualError(t, err, "WHEP returned HTTP 200")
			}
			require.Equal(t, int32(1), posts.Load())
			if tc.status == http.StatusCreated {
				require.Equal(t, int32(1), deletes.Load(), "rejected epoch or SDP must release its same-origin allocation")
			} else {
				require.Zero(t, deletes.Load())
			}
		})
	}
}

func TestEdgeMediaNativeCleanupRejectsForeignOrAmbiguousOrigin(t *testing.T) {
	for _, location := range []string{"foreign", "userinfo", "fragment", "opaque"} {
		t.Run(location, func(t *testing.T) {
			var sinkCalls, localDeletes atomic.Int32
			sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sinkCalls.Add(1); w.WriteHeader(http.StatusNoContent) }))
			t.Cleanup(sink.Close)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					localDeletes.Add(1)
					w.WriteHeader(http.StatusNoContent)
					return
				}
				target := sink.URL + "/session"
				switch location {
				case "userinfo":
					target = "//private-user:private-secret@" + r.Host + "/session"
				case "fragment":
					target = "/session#private-token"
				case "opaque":
					target = "http:session"
				}
				w.Header().Set("Location", target)
				w.Header().Set("Shale-Capture-Epoch", "capture-current")
				w.WriteHeader(http.StatusCreated)
			}))
			t.Cleanup(server.Close)
			evidence, err := qualifyWHEP(t.Context(), api.LiveSource_builder{WhepUrl: server.URL, ViewToken: "private-view-token", CaptureEpoch: "capture-current"}.Build(), edgeMediaOptions{Timeout: time.Second})
			require.Error(t, err)
			require.Equal(t, edgeMediaEvidence{}, evidence)
			require.Zero(t, sinkCalls.Load(), "a session Location cannot transfer the capability to another origin")
			require.Zero(t, localDeletes.Load(), "unsafe session URIs must not be used for cleanup")
			require.NotContains(t, err.Error(), "private")
		})
	}
}

func TestEdgeMediaInvalidCapabilityDoesNoHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	t.Cleanup(server.Close)
	for _, tc := range []struct{ url, token, epoch string }{
		{strings.Replace(server.URL, "://", "://private-user:private-secret@", 1), "valid-token", "capture"},
		{server.URL + "/#fragment", "valid-token", "capture"},
		{server.URL, "token with spaces", "capture"},
		{server.URL, "valid-token", "epoch\nprivate"},
		{server.URL, "", "capture"},
	} {
		evidence, err := qualifyWHEP(t.Context(), api.LiveSource_builder{WhepUrl: tc.url, ViewToken: tc.token, CaptureEpoch: tc.epoch}.Build(), edgeMediaOptions{})
		require.Error(t, err)
		require.Equal(t, edgeMediaEvidence{}, evidence)
		require.NotContains(t, err.Error(), "private")
	}
	require.Zero(t, calls.Load())
}

func TestEdgeMediaParameterSetsAndMissingTrackDoNotProveMedia(t *testing.T) {
	collector := edgeAccessUnits{target: 2, maxBytes: 1024}
	for i := range 3 {
		complete, err := collector.push(accessUnit{ts: uint32(i * 3000), whole: true, marked: true, nals: [][]byte{{0x67}, {0x68}}})
		require.NoError(t, err)
		require.False(t, complete)
	}
	require.Zero(t, collector.evidence.AccessUnits)
	require.Zero(t, collector.evidence.DecodedFrames)
	require.Empty(t, collector.annexB)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	media := readEdgeVideo(ctx, nil, edgeMediaOptions{AccessUnits: 2, MaxBytes: 1024})
	require.ErrorIs(t, media.err, context.Canceled)
	require.Empty(t, media.annexB)
	require.Equal(t, edgeMediaEvidence{}, media.evidence)
}

func TestEdgeMediaFrameChecksumsRequireAdvancingCompleteDecodedFrames(t *testing.T) {
	line := func(pts int64, digest string) string {
		return fmt.Sprintf("0, %d, %d, 1, 115200, %s\n", pts, pts, digest)
	}
	const digest = "d41d8cd98f00b204e9800998ecf8427e"
	t.Run("split-output", func(t *testing.T) {
		frames := &edgeDecodeFrames{maxFrames: 2}
		output := "#format: frame checksums\n" + line(0, digest) + line(1, digest)
		for _, part := range []string{output[:23], output[23:48], output[48:]} {
			_, err := frames.Write([]byte(part))
			require.NoError(t, err)
		}
		require.Equal(t, 2, frames.frames)
		require.Empty(t, frames.pending)
		require.True(t, frames.last.After(frames.first))
		require.True(t, frames.complete())
	})
	for _, tc := range []struct{ name, output string }{
		{"nonhex-checksum", line(0, strings.Repeat("z", 32))},
		{"no-pixels", "0, 0, 0, 1, 0, " + digest + "\n"},
		{"invalid-timestamp", "0, 0, N/A, 1, 115200, " + digest + "\n"},
		{"non-video-stream", "1, 0, 0, 1, 115200, " + digest + "\n"},
		{"no-duration", "0, 0, 0, 0, 115200, " + digest + "\n"},
		{"repeated-pts", line(0, digest) + line(0, digest)},
		{"reversed-pts", line(1, digest) + line(0, digest)},
		{"frame-budget", line(0, digest) + line(1, digest) + line(2, digest)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frames := &edgeDecodeFrames{maxFrames: 2}
			_, err := frames.Write([]byte(tc.output))
			require.Error(t, err)
		})
	}
	frames := &edgeDecodeFrames{maxFrames: 2}
	_, err := frames.Write([]byte("#format: frame checksums\n#header-only\n"))
	require.NoError(t, err)
	require.Zero(t, frames.frames, "decoder headers are not decoded pixel frames")
	require.False(t, frames.complete())
	_, err = frames.Write([]byte(strings.TrimSuffix(line(0, digest), "\n")))
	require.NoError(t, err)
	require.Zero(t, frames.frames, "a partial frame line is never counted")
	require.False(t, frames.complete())
	frames = &edgeDecodeFrames{maxFrames: 3}
	_, err = frames.Write([]byte(line(0, digest) + line(1, digest) + strings.TrimSuffix(line(2, digest), "\n")))
	require.NoError(t, err)
	require.Equal(t, 2, frames.frames)
	require.False(t, frames.complete(), "a truncated trailing frame invalidates the complete decoder output")
}
