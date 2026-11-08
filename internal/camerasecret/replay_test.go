package camerasecret

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
)

func TestImportReplayIsKeyedExactAndPersistent(t *testing.T) {
	b, c := testBackend(t), testContext(t, testIdentity())
	payload := []byte("synthetic-user\x00synthetic-password\xff\x00")
	want := bytes.Clone(payload)
	tag, err := b.ImportTag(context.Background(), c, "import-1", payload)
	if err != nil {
		t.Fatal(err)
	}
	clear(payload)
	stored, err := tag.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseReplayTag(stored)
	if err != nil {
		t.Fatal(err)
	}
	clear(stored)
	if !tag.Equal(parsed) {
		t.Fatal("replay tag lost identity or retained mutable input storage")
	}
	restarted := testBackend(t)
	replayed, err := restarted.ImportTag(context.Background(), c, "import-1", want)
	if err != nil || !tag.Equal(replayed) {
		t.Fatal("same immutable import could not replay after backend restart")
	}
	otherKey, err := NewAESGCM(bytes.Repeat([]byte{0x68}, 32))
	if err != nil {
		t.Fatal(err)
	}
	id := c.Identity()
	id.Version = "v2"
	otherTenant := c.Identity()
	otherTenant.TenantID = testIdentity().TenantID
	otherPurpose := c.Identity()
	otherPurpose.Purpose = Management
	for name, item := range map[string]struct {
		backend *AESGCM
		context Context
		intent  string
		payload []byte
	}{
		"key":           {otherKey, c, "import-1", want},
		"version":       {b, testContext(t, id), "import-1", want},
		"tenant":        {b, testContext(t, otherTenant), "import-1", want},
		"purpose":       {b, testContext(t, otherPurpose), "import-1", want},
		"intent":        {b, c, "import-2", want},
		"case":          {b, c, "import-1", bytes.ToUpper(want)},
		"binary suffix": {b, c, "import-1", append(bytes.Clone(want), 0)},
		"decoded bytes": {b, c, "import-1", []byte("synthetic-user\x00synthetic-password%ff%00")},
	} {
		t.Run(name, func(t *testing.T) {
			other, err := item.backend.ImportTag(context.Background(), item.context, item.intent, item.payload)
			if err != nil || tag.Equal(other) {
				t.Fatal("changed import inputs produced the same equality tag")
			}
		})
	}
	unkeyed := sha256.Sum256(want)
	if bytes.Equal(tag.sum[:], unkeyed[:]) {
		t.Fatal("stored import tag is an unkeyed password hash")
	}
	if (ReplayTag{}).Equal(ReplayTag{}) || tag.Equal(ReplayTag{}) || (ReplayTag{}).Equal(tag) {
		t.Fatal("missing replay tag was treated as a valid match")
	}
	for _, intent := range []string{"", strings.Repeat("i", 129), "password\x00value"} {
		if _, err := b.ImportTag(context.Background(), c, intent, nil); !errors.Is(err, ErrContext) {
			t.Fatal("invalid import intent accepted")
		}
	}
	if _, err := b.ImportTag(context.Background(), Context{}, "intent", nil); !errors.Is(err, ErrContext) {
		t.Fatal("zero context accepted for import replay")
	}
	if _, err := b.ImportTag(context.Background(), c, "intent", make([]byte, MaxPayload+1)); !errors.Is(err, ErrPayload) {
		t.Fatal("unbounded import payload accepted")
	}
}

func TestExplicitBinaryFormatsRejectMalformedInput(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, bytes.Repeat([]byte{0xff}, 32), append([]byte{2}, make([]byte, 32)...), make([]byte, 34)} {
		if _, err := ParseReplayTag(payload); !errors.Is(err, ErrReplayTag) {
			t.Fatal("malformed replay tag accepted")
		}
	}
	if _, err := (ReplayTag{}).MarshalBinary(); !errors.Is(err, ErrReplayTag) {
		t.Fatal("zero replay tag persisted")
	}
	if _, err := (Envelope{}).MarshalBinary(); !errors.Is(err, ErrEnvelope) {
		t.Fatal("zero envelope persisted")
	}
	for _, name := range []string{"", "Uppercase", "userinfo:password", strings.Repeat("x", 64), "invalid\x00backend"} {
		if _, err := NewEnvelope(name, []byte("opaque")); !errors.Is(err, ErrEnvelope) {
			t.Fatal("invalid backend name accepted")
		}
	}
	for _, payload := range [][]byte{nil, make([]byte, maxOpaque+1)} {
		if _, err := NewEnvelope("test-v1", payload); !errors.Is(err, ErrEnvelope) {
			t.Fatal("invalid opaque envelope data accepted")
		}
	}
}

func FuzzParseEnvelope(f *testing.F) {
	envelope, _ := NewEnvelope("external-v1", []byte("opaque-handle"))
	encoded, _ := envelope.MarshalBinary()
	f.Add(encoded)
	f.Add([]byte{})
	f.Add([]byte("SHCAMSEC\x01\xff\xff\xff\xff\xff"))
	f.Fuzz(func(t *testing.T, input []byte) {
		envelope, err := ParseEnvelope(input)
		if err != nil {
			if !errors.Is(err, ErrEnvelope) {
				t.Fatal("parser returned a nonfixed diagnostic")
			}
			return
		}
		encoded, err := envelope.MarshalBinary()
		if err != nil || !bytes.Equal(encoded, input) {
			t.Fatal("accepted binary envelope did not round-trip canonically")
		}
		clear(input)
		again, err := envelope.MarshalBinary()
		if err != nil || !bytes.Equal(again, encoded) {
			t.Fatal("parser retained caller-owned input")
		}
	})
}
