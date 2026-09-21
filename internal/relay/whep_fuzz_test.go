package relay

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// The WHEP endpoint is reachable by every viewer (§39.4): any method,
// path, token and body must be answered, never panic, and a token that
// names another source, another operation or another relay must not open
// a session (#57). With a valid token the body is an SDP offer handed to
// pion, which must refuse the garbage as well.
func FuzzWHEP(f *testing.F) {
	dir := f.TempDir()
	r, err := New(Config{StateDir: dir, IngestAddr: "127.0.0.1:0", WhepAddr: "127.0.0.1:0"})
	if err != nil {
		f.Fatal(err)
	}
	w, err := newWhepServer(r)
	if err != nil {
		f.Fatal(err)
	}
	k, err := token.Generate("k-fuzz")
	if err != nil {
		f.Fatal(err)
	}
	r.keys.Verifier.Add("k-fuzz", k.Public())
	source := pdid.New(pdid.Domain(8))
	now := time.Now()
	view, err := k.Sign(api.TokenClaims_builder{
		Exp: timestamppb.New(now.Add(time.Hour)), Iat: timestamppb.New(now),
		Aud: r.id.Bytes(), Op: api.TokenOp_TOKEN_OP_VIEW, Source: source.Bytes(),
		Actor: pdid.New(pdid.Domain(2)).Bytes(),
	}.Build())
	if err != nil {
		f.Fatal(err)
	}
	put, err := k.Sign(api.TokenClaims_builder{
		Exp: timestamppb.New(now.Add(time.Hour)), Iat: timestamppb.New(now),
		Aud: r.id.Bytes(), Op: api.TokenOp_TOKEN_OP_PUT, Source: source.Bytes(),
	}.Build())
	if err != nil {
		f.Fatal(err)
	}
	offer := "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=rtpmap:96 H264/90000\r\na=recvonly\r\n"

	f.Add("POST", "/whep/"+source.String(), 0, offer)
	f.Add("POST", "/whep/"+source.String(), 1, "not sdp")
	f.Add("POST", "/whep/"+source.String(), 2, offer)
	f.Add("DELETE", "/whep/nothing", 0, "")
	f.Add("OPTIONS", "/whep/x", 0, "")
	f.Add("GET", "/healthz", 0, "")
	f.Add("POST", "/whep/", 0, offer)
	f.Add("POST", "/whep/"+pdid.New(pdid.Domain(8)).String(), 0, offer)
	f.Fuzz(func(t *testing.T, method, path string, which int, body string) {
		req, err := http.NewRequest(method, "http://relay"+path, bytes.NewReader([]byte(body)))
		if err != nil {
			// Not a request the net/http server would ever hand over.
			return
		}
		switch which % 4 {
		case 0:
			req.Header.Set("Authorization", token.Scheme+" "+view)
		case 1:
			req.Header.Set("Authorization", token.Scheme+" "+put)
		case 2:
			req.Header.Set("Authorization", token.Scheme+" "+body)
		}
		rec := httptest.NewRecorder()
		w.ServeHTTP(rec, req)
		switch which % 4 {
		case 1, 2:
			if rec.Code == http.StatusCreated {
				t.Fatalf("a session opened on a token that is not a view token for this source")
			}
		}
		// Whatever opened is closed again, so sessions do not pile up.
		w.mu.Lock()
		ids := make([]string, 0, len(w.sessions))
		for id := range w.sessions {
			ids = append(ids, id)
		}
		w.mu.Unlock()
		for _, id := range ids {
			w.del(httptest.NewRecorder(), id)
		}
	})
}
