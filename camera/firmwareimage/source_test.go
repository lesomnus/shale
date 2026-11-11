package firmwareimage

import (
	"archive/zip"
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/lesomnus/shale/camera"
)

func archive(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := zip.NewWriter(&b)
	for name, data := range files {
		f, err := w.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.Write(data)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestSelect(t *testing.T) {
	image := []byte("signed firmware image")
	digest := hash(image)
	got, err := Select(image, "fw.bin", digest)
	if err != nil || got.Name != "fw.bin" || !bytes.Equal(got.Data, image) || got.SHA256 != digest {
		t.Fatalf("raw image: %+v %v", got, err)
	}
	// Vendors publish the image inside a zip with release notes.
	zipped := archive(t, map[string][]byte{"Release/notes.pdf": []byte("notes"), "Release/Sx25_up_boot_signed.bin": image})
	got, err = Select(zipped, "release.zip", digest)
	if err != nil || got.Name != "Sx25_up_boot_signed.bin" || !bytes.Equal(got.Data, image) {
		t.Fatalf("archive: %+v %v", got, err)
	}
	for name, data := range map[string][]byte{"other content": []byte("not it"), "archive without it": archive(t, map[string][]byte{"a.bin": []byte("other")})} {
		if _, err := Select(data, "x", digest); !errors.Is(err, camera.ErrFirmwareImage) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestFetchVerifiesAndKeepsTheLastImage(t *testing.T) {
	image := []byte("signed firmware image")
	var requests atomic.Int32
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/fw%20release.zip", "/fw release.zip":
			_, _ = w.Write(archive(t, map[string][]byte{"dir/fw.bin": image}))
		case "/moved":
			http.Redirect(w, r, "/fw%20release.zip", http.StatusFound)
		case "/cleartext":
			http.Redirect(w, r, "http://"+r.Host+"/fw%20release.zip", http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(s.Close)
	source := &Source{Client: s.Client()}
	want := camera.FirmwareImage{URL: s.URL + "/fw%20release.zip", SHA256: hash(image)}
	got, err := source.Fetch(t.Context(), want)
	if err != nil || got.Name != "fw.bin" || !bytes.Equal(got.Data, image) {
		t.Fatalf("fetch: %+v %v", got, err)
	}
	if _, err := source.Fetch(t.Context(), want); err != nil || requests.Load() != 1 {
		t.Fatalf("second fetch: %v requests=%d", err, requests.Load())
	}

	fresh := &Source{Client: s.Client()}
	if _, err := fresh.Fetch(t.Context(), camera.FirmwareImage{URL: s.URL + "/moved", SHA256: hash(image)}); err != nil {
		t.Fatalf("https redirect: %v", err)
	}
	for name, image := range map[string]camera.FirmwareImage{
		"wrong digest":       {URL: s.URL + "/fw%20release.zip", SHA256: hash([]byte("other"))},
		"missing":            {URL: s.URL + "/missing", SHA256: hash(image)},
		"cleartext redirect": {URL: s.URL + "/cleartext", SHA256: hash(image)},
	} {
		if _, err := (&Source{Client: s.Client()}).Fetch(t.Context(), image); !errors.Is(err, camera.ErrFirmwareImage) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := fresh.Fetch(t.Context(), camera.FirmwareImage{URL: "http://example.invalid/fw.bin", SHA256: hash(image)}); !errors.Is(err, camera.ErrInvalid) {
		t.Errorf("cleartext URL: %v", err)
	}
}
