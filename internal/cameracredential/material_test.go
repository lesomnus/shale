package cameracredential

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerasecret"
	"go.yaml.in/yaml/v3"
)

func testReference() Reference {
	return Reference{Subject: Subject{
		TenantID: pdid.New(1), DeviceID: pdid.New(27), BindingID: pdid.New(29), Purpose: camerasecret.Streaming,
	}, CredentialID: pdid.New(32), Version: "v1"}
}

func TestMaterialSurvivesEncryptedPersistenceExactly(t *testing.T) {
	username, password := "user\x00\xff", "password\xfe\x00\r\n"
	m, err := NewMaterial(username, password)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	b, err := camerasecret.NewAESGCM(bytes.Repeat([]byte{0x63}, 32))
	if err != nil {
		t.Fatal(err)
	}
	c, err := testReference().Context()
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := b.Protect(context.Background(), c, plaintext)
	clear(plaintext)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := sealed.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := camerasecret.ParseEnvelope(stored)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := b.Open(context.Background(), c, envelope)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := ParseMaterial(opened)
	clear(opened)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Use(context.Background(), func(v camera.CredentialMaterial) error {
		if v.Username != username || v.Password != password {
			t.Fatal("encrypted material changed bytes or retained mutable parser storage")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := recovered.Use(ctx, func(camera.CredentialMaterial) error { t.Fatal("canceled callback ran"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal("material callback lost cancellation identity")
	}
}

func TestMaterialBoundsAndMalformedBinary(t *testing.T) {
	for _, input := range [][2]string{{"", "p"}, {"u", ""}, {strings.Repeat("u", camerasecret.MaxPayload), "p"}, {"u", strings.Repeat("p", camerasecret.MaxPayload)}} {
		if _, err := NewMaterial(input[0], input[1]); !errors.Is(err, ErrInput) {
			t.Fatal("invalid material accepted or input escaped through an error")
		}
	}
	m, err := NewMaterial("u", "p")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(encoded); i++ {
		if _, err := ParseMaterial(encoded[:i]); !errors.Is(err, ErrInput) {
			t.Fatal("truncated material accepted")
		}
	}
	for i := range len(materialMagic) + 8 {
		corrupt := bytes.Clone(encoded)
		corrupt[i] ^= 1
		if _, err := ParseMaterial(corrupt); !errors.Is(err, ErrInput) {
			t.Fatal("corrupt material header accepted")
		}
	}
	if _, err := ParseMaterial(append(encoded, 0)); !errors.Is(err, ErrInput) {
		t.Fatal("trailing data accepted")
	}
	if _, err := (Material{}).MarshalBinary(); !errors.Is(err, ErrInput) {
		t.Fatal("missing material accepted")
	}
	if err := m.Use(context.Background(), nil); !errors.Is(err, ErrInput) {
		t.Fatal("nil material callback accepted")
	}
	if err := (Material{}).Use(context.Background(), func(camera.CredentialMaterial) error { t.Fatal("empty material exposed"); return nil }); !errors.Is(err, ErrInput) {
		t.Fatal("empty material callback did not fail closed")
	}
}

func TestImportAndMaterialDiagnosticsStayRedacted(t *testing.T) {
	const username, password = "synthetic-private-user", "synthetic-private-password"
	m, err := NewMaterial(username, password)
	if err != nil {
		t.Fatal(err)
	}
	request := ImportRequest{Intent: Intent{Subject: testReference().Subject, Key: "import-1", Access: camera.AccessRead}, Material: m}
	for _, value := range []any{m, &m, request, &request} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%d", "%x", "%q"} {
			// Unsupported format verbs must not recursively expose private
			// strings through a containing request either.
			formatted := fmt.Sprintf(verb, value)
			if strings.Contains(formatted, username) || strings.Contains(formatted, password) {
				t.Fatal("ordinary formatting exposed import material")
			}
		}
		for _, marshal := range []func(any) ([]byte, error){json.Marshal, yaml.Marshal} {
			encoded, err := marshal(value)
			if err != nil || bytes.Contains(encoded, []byte(username)) || bytes.Contains(encoded, []byte(password)) {
				t.Fatal("ordinary serializer exposed import material")
			}
		}
	}
	if err := json.Unmarshal([]byte(`{"password":"`+password+`"}`), new(Material)); !errors.Is(err, ErrInput) {
		t.Fatal("ordinary JSON decoded plaintext credential state")
	}
	if err := yaml.Unmarshal([]byte("password: "+password), new(Material)); !errors.Is(err, ErrInput) {
		t.Fatal("ordinary YAML decoded plaintext credential state")
	}
}

func TestReferenceIdentityValidation(t *testing.T) {
	r := testReference()
	if err := r.Subject.Validate(); err != nil {
		t.Fatal(err)
	}
	c, err := r.Context()
	if err != nil || c.Identity().CredentialID != r.CredentialID || c.Identity().BindingID != r.Subject.BindingID {
		t.Fatal("stable reference could not supply its encryption context")
	}
	for _, change := range []func(*Reference){
		func(r *Reference) { r.Subject.TenantID = pdid.New(27) },
		func(r *Reference) { r.Subject.DeviceID = pdid.New(29) },
		func(r *Reference) { r.Subject.BindingID = pdid.New(27) },
		func(r *Reference) { r.Subject.Purpose = camerasecret.InitialAdmin },
		func(r *Reference) { r.CredentialID = pdid.New(33) },
		func(r *Reference) { r.Version = "" },
	} {
		other := r
		change(&other)
		if _, err := other.Context(); !errors.Is(err, ErrInput) {
			t.Fatal("invalid reference accepted")
		}
	}
	initial := r.Subject
	initial.BindingID, initial.Purpose = pdid.Nil, camerasecret.InitialAdmin
	if err := initial.Validate(); err != nil {
		t.Fatal("device-wide bootstrap subject rejected")
	}
}

func FuzzParseMaterial(f *testing.F) {
	m, _ := NewMaterial("u\xff", "p\xfe\x00")
	encoded, _ := m.MarshalBinary()
	f.Add(encoded)
	f.Add([]byte{})
	f.Add([]byte("SHCAMMAT\x01\xff\xff\xff\xff\xff\xff\xff\xff"))
	f.Fuzz(func(t *testing.T, input []byte) {
		m, err := ParseMaterial(input)
		if err != nil {
			if !errors.Is(err, ErrInput) {
				t.Fatal("parser exposed input through a diagnostic")
			}
			return
		}
		encoded, err := m.MarshalBinary()
		if err != nil || !bytes.Equal(encoded, input) {
			t.Fatal("accepted material did not round-trip exactly")
		}
		clear(input)
		copied, err := m.MarshalBinary()
		if err != nil || !bytes.Equal(copied, encoded) {
			t.Fatal("material parser retained mutable input")
		}
	})
}
