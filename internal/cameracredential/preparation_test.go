package cameracredential

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerasecret"
	"go.yaml.in/yaml/v3"
)

func TestPreparationCopiesAndRedactsNewUsername(t *testing.T) {
	r := testReference()
	intent := Intent{Subject: r.Subject, Key: "prepare-1", Access: camera.AccessRead}
	username := []byte("synthetic-private-user\xff")
	p, err := NewPreparation(intent, username, &r)
	if err != nil {
		t.Fatal(err)
	}
	clear(username)
	r.Version = "changed"
	if string(p.Username()) != "synthetic-private-user\xff" || p.Reuse.Version != "v1" {
		t.Fatal("preparation retained mutable caller-owned input")
	}
	copy := p.Username()
	clear(copy)
	if len(p.Username()) == 0 || p.Username()[0] == 0 {
		t.Fatal("preparation exposed mutable username storage")
	}
	for _, value := range []any{p, &p} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%d", "%x", "%q"} {
			if fmt.Sprintf(verb, value) != preparationMarker {
				t.Fatal("preparation diagnostic exposed request input")
			}
		}
		encoded, err := json.Marshal(value)
		if err != nil || string(encoded) != `"`+preparationMarker+`"` {
			t.Fatal("JSON exposed preparation input")
		}
		if _, err := yaml.Marshal(value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NewPreparation(intent, nil, nil); !errors.Is(err, ErrInput) {
		t.Fatal("empty new-account username accepted")
	}
	for _, key := range []string{"", "\x00input", "../unsafe"} {
		bad := intent
		bad.Key = key
		if _, err := NewPreparation(bad, []byte("admin"), nil); !errors.Is(err, ErrInput) {
			t.Fatal("invalid intent accepted")
		}
	}
	initial := intent
	initial.Subject.BindingID = [16]byte{}
	initial.Subject.Purpose = camerasecret.InitialAdmin
	if _, err := NewPreparation(initial, []byte("admin"), nil); !errors.Is(err, ErrInput) {
		t.Fatal("initial administrator account with nonadministrator access accepted")
	}
}
