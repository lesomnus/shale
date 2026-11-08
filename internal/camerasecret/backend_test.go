package camerasecret

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"go.yaml.in/yaml/v3"
)

func testIdentity() Identity {
	return Identity{
		TenantID: pdid.New(1), DeviceID: pdid.New(27), BindingID: pdid.New(29),
		Purpose: Streaming, CredentialID: pdid.New(32), Version: "v1",
	}
}

func testContext(t *testing.T, id Identity) Context {
	t.Helper()
	c, err := NewContext(id)
	if err != nil {
		t.Fatal("create context:", err)
	}
	return c
}

func testBackend(t *testing.T) *AESGCM {
	t.Helper()
	b, err := NewAESGCM(bytes.Repeat([]byte{0x67}, 32))
	if err != nil {
		t.Fatal("create backend:", err)
	}
	return b
}

func TestContextIdentityAndValidation(t *testing.T) {
	id := testIdentity()
	c := testContext(t, id)
	id.Version = "changed"
	copy := c.Identity()
	copy.TenantID[0] ^= 1
	copy.Version = "also-changed"
	if c.Identity().Version != "v1" || c.Identity().TenantID == copy.TenantID {
		t.Fatal("context identity changed through a caller-owned value")
	}
	for _, purpose := range []Purpose{InitialAdmin, Management, Streaming} {
		deviceWide := c.Identity()
		deviceWide.Purpose, deviceWide.BindingID = purpose, pdid.Nil
		if _, err := NewContext(deviceWide); err != nil {
			t.Fatalf("device-wide %s rejected: %v", purpose, err)
		}
	}
	cases := map[string]func(*Identity){
		"zero tenant":       func(id *Identity) { id.TenantID = pdid.Nil },
		"wrong tenant":      func(id *Identity) { id.TenantID = pdid.New(27) },
		"wrong device":      func(id *Identity) { id.DeviceID = pdid.New(29) },
		"wrong binding":     func(id *Identity) { id.BindingID = pdid.New(27) },
		"wrong credential":  func(id *Identity) { id.CredentialID = pdid.New(33) },
		"non payday shape":  func(id *Identity) { id.CredentialID[6] = 0x40 },
		"empty purpose":     func(id *Identity) { id.Purpose = "" },
		"unknown purpose":   func(id *Identity) { id.Purpose = "administrator" },
		"initial binding":   func(id *Identity) { id.Purpose = InitialAdmin },
		"empty version":     func(id *Identity) { id.Version = "" },
		"unbounded version": func(id *Identity) { id.Version = strings.Repeat("v", 129) },
		"invalid version":   func(id *Identity) { id.Version = "v1\x00password" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			id := c.Identity()
			change(&id)
			if _, err := NewContext(id); !errors.Is(err, ErrContext) {
				t.Fatal("invalid identity did not return the fixed context error")
			}
		})
	}
}

func TestProtectOpenPreservesBytesAndImmutableBuffers(t *testing.T) {
	b, c := testBackend(t), testContext(t, testIdentity())
	for _, payload := range [][]byte{nil, {}, []byte("username\x00password\xff\xfe\x80\r\n"), bytes.Repeat([]byte{0xff}, MaxPayload)} {
		envelope, err := b.Protect(context.Background(), c, payload)
		if err != nil {
			t.Fatal("protect:", err)
		}
		stored, err := envelope.MarshalBinary()
		if err != nil {
			t.Fatal("encode:", err)
		}
		parsed, err := ParseEnvelope(stored)
		if err != nil {
			t.Fatal("parse:", err)
		}
		clear(stored)
		opened, err := b.Open(context.Background(), c, parsed)
		if err != nil || !bytes.Equal(opened, payload) {
			t.Fatal("byte payload failed to survive explicit persistence and restart")
		}
		clear(opened)
		opened, err = b.Open(context.Background(), c, parsed)
		if err != nil || !bytes.Equal(opened, payload) {
			t.Fatal("opened material shared storage with its envelope")
		}
	}
	input := []byte("caller-owned synthetic credential")
	want := bytes.Clone(input)
	envelope, err := b.Protect(context.Background(), c, input)
	if err != nil {
		t.Fatal(err)
	}
	clear(input)
	opaque := envelope.Opaque()
	clear(opaque)
	opened, err := b.Open(context.Background(), c, envelope)
	if err != nil || !bytes.Equal(opened, want) {
		t.Fatal("envelope retained mutable input or exposed its internal storage")
	}
	if _, err := b.Protect(context.Background(), c, make([]byte, MaxPayload+1)); !errors.Is(err, ErrPayload) {
		t.Fatal("oversized payload was accepted")
	}
}

func TestPersistedV1Compatibility(t *testing.T) {
	// These fixtures use only synthetic material: KEK = 32 bytes of 0x67,
	// nonce = 00..0b. Keep them readable after refactors; changing the stable
	// AAD, HKDF labels or envelope format requires an explicit version change.
	c := testContext(t, Identity{
		TenantID:     pdid.MustParse("0199c3f4-2a10-8abc-8a01-9f2e1c4d5b6a"),
		DeviceID:     pdid.MustParse("0199c3f4-2a10-8abc-8a1b-9f2e1c4d5b6a"),
		BindingID:    pdid.MustParse("0199c3f4-2a10-8abc-8a1d-9f2e1c4d5b6a"),
		CredentialID: pdid.MustParse("0199c3f4-2a10-8abc-8a20-9f2e1c4d5b6a"),
		Purpose:      Streaming, Version: "v1",
	})
	wire, err := hex.DecodeString("534843414d534543011b0000002f63702d61657332353667636d2d686b64662d7368613235362d7631000102030405060708090a0bc0481572658f7f27aeb3f2b4bfe96cb8963c87f9f979071d60adf4522834c383054dd6")
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := ParseEnvelope(wire)
	if err != nil {
		t.Fatal("persisted v1 envelope no longer parses:", err)
	}
	want := []byte("synthetic-wire-v1\x00\xff")
	b := testBackend(t)
	if opened, err := b.Open(context.Background(), c, envelope); err != nil || !bytes.Equal(opened, want) {
		t.Fatal("persisted v1 envelope no longer opens")
	}
	tag, err := b.ImportTag(context.Background(), c, "import-fixture", want)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := tag.MarshalBinary()
	if err != nil || hex.EncodeToString(stored) != "0146afd144bd483bef4a7a69b243a19ef5e8ed730b64ae43835ab878ae9e6075e5" {
		t.Fatal("persisted v1 import replay tag no longer matches")
	}
}

func TestOpenAuthenticatesEveryStableIdentity(t *testing.T) {
	b, id := testBackend(t), testIdentity()
	id.Purpose = Management
	c := testContext(t, id)
	envelope, err := b.Protect(context.Background(), c, []byte("synthetic opaque secret"))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*Identity){
		"tenant":     func(id *Identity) { id.TenantID = pdid.New(1) },
		"device":     func(id *Identity) { id.DeviceID = pdid.New(27) },
		"binding":    func(id *Identity) { id.BindingID = pdid.New(29) },
		"no binding": func(id *Identity) { id.BindingID = pdid.Nil },
		"purpose":    func(id *Identity) { id.Purpose = Streaming },
		"credential": func(id *Identity) { id.CredentialID = pdid.New(32) },
		"version":    func(id *Identity) { id.Version = "v2" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			other := id
			change(&other)
			opened, err := b.Open(context.Background(), testContext(t, other), envelope)
			if !errors.Is(err, ErrOpen) || opened != nil {
				t.Fatal("changed identity yielded material or a nonfixed error")
			}
		})
	}
	deviceWide := id
	deviceWide.BindingID = pdid.Nil
	envelope, err = b.Protect(context.Background(), testContext(t, deviceWide), []byte("synthetic admin secret"))
	if err != nil {
		t.Fatal(err)
	}
	deviceWide.Purpose = InitialAdmin
	if opened, err := b.Open(context.Background(), testContext(t, deviceWide), envelope); !errors.Is(err, ErrOpen) || opened != nil {
		t.Fatal("device-wide purpose change was not authenticated")
	}
}

func TestEnvelopeTamperAndWrongKey(t *testing.T) {
	b, c := testBackend(t), testContext(t, testIdentity())
	envelope, err := b.Protect(context.Background(), c, []byte("synthetic payload for nonce ciphertext and tag"))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := envelope.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	// Every byte of the versioned header, nonce, ciphertext and GCM tag is
	// either rejected by parsing or authenticated before material is returned.
	for i := range stored {
		corrupt := bytes.Clone(stored)
		corrupt[i] ^= 1
		parsed, err := ParseEnvelope(corrupt)
		if err != nil {
			if !errors.Is(err, ErrEnvelope) {
				t.Fatal("parser returned a diagnostic containing input")
			}
			continue
		}
		if opened, err := b.Open(context.Background(), c, parsed); err == nil || opened != nil {
			t.Fatalf("tampered byte %d returned material", i)
		}
	}
	other, err := NewAESGCM(bytes.Repeat([]byte{0x68}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if opened, err := other.Open(context.Background(), c, envelope); !errors.Is(err, ErrOpen) || opened != nil {
		t.Fatal("wrong KEK returned material")
	}
	for length := 0; length < len(stored); length++ {
		if _, err := ParseEnvelope(stored[:length]); !errors.Is(err, ErrEnvelope) {
			t.Fatalf("truncated envelope of length %d accepted", length)
		}
	}
	if _, err := ParseEnvelope(append(stored, 0)); !errors.Is(err, ErrEnvelope) {
		t.Fatal("trailing data accepted")
	}
	unknown, err := NewEnvelope("other-backend-v1", []byte("opaque reference"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Open(context.Background(), c, unknown); !errors.Is(err, ErrEnvelope) {
		t.Fatal("foreign backend envelope accepted")
	}
}

func TestKeysAreSeparatedAndDoNotAliasKEK(t *testing.T) {
	kek := bytes.Repeat([]byte{0x39}, 32)
	wantKEK := bytes.Clone(kek)
	b, err := NewAESGCM(kek)
	if err != nil {
		t.Fatal(err)
	}
	clear(kek)
	c := testContext(t, testIdentity())
	envelope, err := b.Protect(context.Background(), c, []byte("synthetic secret"))
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewAESGCM(wantKEK)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Open(context.Background(), c, envelope); err != nil {
		t.Fatal("backend retained the caller-owned KEK or could not reopen after restart")
	}
	macKey, err := hkdf.Key(sha256.New, wantKEK, nil, importLabel, 32)
	if err != nil {
		t.Fatal(err)
	}
	for _, wrongKey := range [][]byte{wantKEK, macKey} {
		block, err := aes.NewCipher(wrongKey)
		if err != nil {
			t.Fatal(err)
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			t.Fatal(err)
		}
		opaque := envelope.Opaque()
		aad := appendField(envelopeHeader(envelope.Backend(), len(opaque)), []byte(c.aad))
		if _, err := gcm.Open(nil, opaque[:gcm.NonceSize()], opaque[gcm.NonceSize():], aad); err == nil {
			t.Fatal("encryption reused either the raw signing KEK or the import replay key")
		}
	}
	for _, key := range [][]byte{nil, make([]byte, 16), make([]byte, 31), make([]byte, 33)} {
		if _, err := NewAESGCM(key); !errors.Is(err, ErrUnavailable) {
			t.Fatal("invalid KEK did not fail closed")
		}
	}
}

func TestNonceFreshnessAndConcurrentUse(t *testing.T) {
	b, c := testBackend(t), testContext(t, testIdentity())
	const count = 64
	results := make(chan Envelope, count)
	var wg sync.WaitGroup
	for range count {
		wg.Go(func() {
			envelope, err := b.Protect(context.Background(), c, []byte("same synthetic payload"))
			if err != nil {
				t.Error("concurrent protect:", err)
				return
			}
			if _, err := b.Open(context.Background(), c, envelope); err != nil {
				t.Error("concurrent open:", err)
				return
			}
			results <- envelope
		})
	}
	wg.Wait()
	close(results)
	nonces := map[string]bool{}
	for envelope := range results {
		opaque := envelope.Opaque()
		nonce := string(opaque[:12])
		if nonces[nonce] {
			t.Fatal("random nonce was reused")
		}
		nonces[nonce] = true
	}
	if len(nonces) != count {
		t.Fatal("some concurrent operations failed")
	}
}

func TestFixedErrorsAndCancellation(t *testing.T) {
	b, c := testBackend(t), testContext(t, testIdentity())
	envelope, err := b.Protect(context.Background(), c, []byte("synthetic credential"))
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []*AESGCM{nil, {}} {
		if _, err := backend.Protect(context.Background(), c, nil); !errors.Is(err, ErrUnavailable) {
			t.Fatal("unavailable backend protected material")
		}
		if _, err := backend.Open(context.Background(), c, envelope); !errors.Is(err, ErrUnavailable) {
			t.Fatal("unavailable backend returned material")
		}
		if _, err := backend.ImportTag(context.Background(), c, "intent", nil); !errors.Is(err, ErrUnavailable) {
			t.Fatal("unavailable backend returned an import tag")
		}
	}
	if _, err := b.Protect(context.Background(), Context{}, nil); !errors.Is(err, ErrContext) {
		t.Fatal("zero context protected material")
	}
	if _, err := b.Open(context.Background(), Context{}, envelope); !errors.Is(err, ErrContext) {
		t.Fatal("zero context opened material")
	}
	if _, err := b.Open(context.Background(), c, Envelope{}); !errors.Is(err, ErrEnvelope) {
		t.Fatal("zero envelope was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Protect(ctx, c, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("protect lost cancellation identity")
	}
	if _, err := b.Open(ctx, c, envelope); !errors.Is(err, context.Canceled) {
		t.Fatal("open lost cancellation identity")
	}
	if _, err := b.ImportTag(ctx, c, "intent", nil); !errors.Is(err, context.Canceled) {
		t.Fatal("import replay lost cancellation identity")
	}

	ctx, cancel = context.WithCancel(context.Background())
	aead := &cancelAfterOpen{AEAD: b.aead, cancel: cancel}
	interrupted := *b
	interrupted.aead = aead
	if opened, err := interrupted.Open(ctx, c, envelope); !errors.Is(err, context.Canceled) || opened != nil {
		t.Fatal("open returned material after cancellation")
	}
	if len(aead.opened) == 0 || !bytes.Equal(aead.opened, make([]byte, len(aead.opened))) {
		t.Fatal("canceled open left its discarded plaintext buffer populated")
	}
}

type cancelAfterOpen struct {
	cipher.AEAD
	cancel context.CancelFunc
	opened []byte
}

func (a *cancelAfterOpen) Open(dst, nonce, ciphertext, aad []byte) ([]byte, error) {
	var err error
	a.opened, err = a.AEAD.Open(dst, nonce, ciphertext, aad)
	a.cancel()
	return a.opened, err
}

func TestDiagnosticsAndOrdinarySerializersAreRedacted(t *testing.T) {
	kek := bytes.Repeat([]byte{0x72}, 32)
	b, err := NewAESGCM(kek)
	if err != nil {
		t.Fatal(err)
	}
	c := testContext(t, testIdentity())
	const secret = "synthetic-password-must-stay-private"
	envelope, err := b.Protect(context.Background(), c, []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	tag, err := b.ImportTag(context.Background(), c, "import-1", []byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	for name, item := range map[string]struct {
		value any
		want  string
	}{
		"backend pointer": {b, redactedBackend}, "backend value": {*b, redactedBackend},
		"envelope": {envelope, redactedEnvelope}, "replay": {tag, redactedReplay},
	} {
		t.Run(name, func(t *testing.T) {
			for _, format := range []string{"%v", "%+v", "%#v", "%s", "%d", "%x", "%+x", "%#x", "%q", "%o", "%f"} {
				if got := fmt.Sprintf(format, item.value); got != item.want {
					t.Fatal("ordinary formatting exposed backend state")
				}
			}
			encoded, err := json.Marshal(item.value)
			if err != nil || string(encoded) != `"`+item.want+`"` {
				t.Fatal("JSON exposed backend state")
			}
			encoded, err = yaml.Marshal(item.value)
			if err != nil || strings.TrimSpace(string(encoded)) != "'"+item.want+"'" {
				t.Fatal("YAML exposed backend state")
			}
		})
	}
	encoded, err := json.Marshal(map[string]any{"backend": b, "envelope": envelope, "replay": tag})
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{secret, hex.EncodeToString(kek), hex.EncodeToString(b.importKey[:]), string(envelope.Opaque())} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatal("nested diagnostic exposed material or derived keys")
		}
	}
	for _, target := range []any{new(AESGCM), new(Envelope), new(ReplayTag)} {
		if err := json.Unmarshal([]byte(`{}`), target); err == nil {
			t.Fatal("ordinary JSON was accepted as persisted secret state")
		}
		if err := yaml.Unmarshal([]byte("field: value\n"), target); err == nil {
			t.Fatal("ordinary YAML was accepted as persisted secret state")
		}
	}
}
