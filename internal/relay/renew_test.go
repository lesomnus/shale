package relay

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// A viewer renews its session with a fresh view token instead of opening a
// new one (§39.4): the session then ends with the fresh token, not the one
// it was opened with. A token for another source or another actor renews
// nothing, and neither does a token of another operation.
func TestWhepRenew(t *testing.T) {
	r, err := New(Config{StateDir: t.TempDir(), MaxViewers: 10, ViewersPerActor: 10})
	require.NoError(t, err)
	w, err := newWhepServer(r)
	require.NoError(t, err)
	k, err := token.Generate("k-renew")
	require.NoError(t, err)
	r.keys.Verifier.Add("k-renew", k.Public())

	source := pdid.New(pdid.Domain(8))
	actor := pdid.New(pdid.Domain(2))
	sign := func(op api.TokenOp, source, actor pdid.Id, ttl time.Duration) string {
		now := time.Now()
		s, err := k.Sign(api.TokenClaims_builder{
			Exp: timestamppb.New(now.Add(ttl)), Iat: timestamppb.New(now),
			Aud: r.id.Bytes(), Op: op, Source: source.Bytes(), Actor: actor.Bytes(),
		}.Build())
		require.NoError(t, err)
		return s
	}
	do := func(method, path, tok, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if tok != "" {
			req.Header.Set("Authorization", token.Scheme+" "+tok)
		}
		rec := httptest.NewRecorder()
		w.ServeHTTP(rec, req)
		return rec
	}
	open := func(ttl time.Duration) string {
		pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
		require.NoError(t, err)
		t.Cleanup(func() { pc.Close() })
		_, err = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
		require.NoError(t, err)
		offer, err := pc.CreateOffer(nil)
		require.NoError(t, err)
		gathered := webrtc.GatheringCompletePromise(pc)
		require.NoError(t, pc.SetLocalDescription(offer))
		<-gathered
		rec := do(http.MethodPost, "/whep/"+source.String(), sign(api.TokenOp_TOKEN_OP_VIEW, source, actor, ttl), pc.LocalDescription().SDP)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		return rec.Header().Get("Location")
	}
	alive := func(loc string) bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		_, ok := w.sessions[strings.TrimPrefix(loc, "/whep/")]
		return ok
	}

	renewed := open(2 * time.Second)
	left := open(2 * time.Second)

	require.Equal(t, http.StatusForbidden, do(http.MethodPatch, renewed, sign(api.TokenOp_TOKEN_OP_VIEW, pdid.New(pdid.Domain(8)), actor, time.Hour), "").Code, "another source")
	require.Equal(t, http.StatusForbidden, do(http.MethodPatch, renewed, sign(api.TokenOp_TOKEN_OP_VIEW, source, pdid.New(pdid.Domain(2)), time.Hour), "").Code, "another actor")
	require.Equal(t, http.StatusUnauthorized, do(http.MethodPatch, renewed, sign(api.TokenOp_TOKEN_OP_PUT, source, actor, time.Hour), "").Code, "another operation")
	require.Equal(t, http.StatusUnauthorized, do(http.MethodPatch, renewed, "", "").Code, "no token")
	require.Equal(t, http.StatusNotFound, do(http.MethodPatch, "/whep/nobody.00", sign(api.TokenOp_TOKEN_OP_VIEW, source, actor, time.Hour), "").Code)
	rec := do(http.MethodPatch, renewed, sign(api.TokenOp_TOKEN_OP_VIEW, source, actor, time.Hour), "")
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	require.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))

	require.Eventually(t, func() bool { return !alive(left) }, 5*time.Second, 50*time.Millisecond, "the session not renewed ends with its token")
	require.True(t, alive(renewed), "the renewed one lives on")

	// A renewal is an end like any other: the session it renewed can end.
	require.Equal(t, http.StatusNoContent, do(http.MethodDelete, renewed, "", "").Code)
	require.False(t, alive(renewed))
	require.Equal(t, http.StatusNotFound, do(http.MethodPatch, renewed, sign(api.TokenOp_TOKEN_OP_VIEW, source, actor, time.Hour), "").Code)
}
