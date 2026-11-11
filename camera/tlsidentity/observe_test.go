package tlsidentity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestObserveIsCredentialFreeAndDoesNotTrustThePeer(t *testing.T) {
	var requests atomic.Int64
	s := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer s.Close()
	pin, err := Observe(t.Context(), s.URL)
	if err != nil {
		t.Fatal("public peer handshake failed")
	}
	h := sha256.Sum256(s.Certificate().RawSubjectPublicKeyInfo)
	if pin != "sha256:"+hex.EncodeToString(h[:]) || requests.Load() != 0 {
		t.Fatal("peer observation sent HTTP or returned the wrong key")
	}
	cfg, err := NewTLSConfig(pin)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.VerifyConnection == nil {
		t.Fatal("observing a key did not retain exact-key verification")
	}
}

func TestObserveRejectsNonOriginsAndPreservesCancellation(t *testing.T) {
	for _, u := range []string{"http://127.0.0.1/", "https://camera.invalid/", "https://u:p@127.0.0.1/", "https://127.0.0.1/?token=private", "https://127.0.0.1/path", "https://127.0.0.1/#private"} {
		if _, err := Observe(t.Context(), u); !errors.Is(err, ErrObservation) || strings.Contains(err.Error(), "private") {
			t.Fatal("unsafe observation accepted or exposed input")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Observe(ctx, "https://127.0.0.1:1/"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation was not retained")
	}
}
