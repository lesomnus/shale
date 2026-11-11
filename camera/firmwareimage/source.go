// Package firmwareimage fetches a firmware file named by a camera preset and
// returns it only when its SHA-256 matches. The URL may name the file itself
// or a zip archive that contains it, as vendors publish their images.
package firmwareimage

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/lesomnus/shale/camera"
)

const (
	// MaxDownload bounds a fetched file or archive held in memory.
	MaxDownload = 128 << 20
	// MaxImage bounds the firmware file itself, inside an archive or not.
	MaxImage = 64 << 20
)

// Source fetches over HTTPS. It keeps the last verified image so a retried
// job does not download it again.
type Source struct {
	// Client defaults to one with the system roots, proxy settings from the
	// environment and a five minute timeout. Redirects must stay on HTTPS.
	Client *http.Client

	mu   sync.Mutex
	last camera.FirmwareFile
}

var _ camera.FirmwareSource = (*Source)(nil)

func (s *Source) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 30 * time.Second},
		Timeout:   5 * time.Minute,
	}
}

func (s *Source) Fetch(ctx context.Context, image camera.FirmwareImage) (camera.FirmwareFile, error) {
	upgrade := camera.FirmwareUpgrade{From: []string{"-"}, To: "+", Image: image}
	if err := upgrade.Validate(); err != nil {
		return camera.FirmwareFile{}, err
	}
	s.mu.Lock()
	if s.last.SHA256 == image.SHA256 {
		out := s.last
		s.mu.Unlock()
		return out, nil
	}
	s.mu.Unlock()

	data, name, err := s.download(ctx, image.URL)
	if err != nil {
		return camera.FirmwareFile{}, err
	}
	out, err := Select(data, name, image.SHA256)
	if err != nil {
		return camera.FirmwareFile{}, err
	}
	s.mu.Lock()
	s.last = out
	s.mu.Unlock()
	return out, nil
}

func (s *Source) download(ctx context.Context, raw string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, "", camera.ErrInvalid
	}
	client := *s.client()
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) >= 5 || next.URL.Scheme != "https" || next.URL.User != nil {
			return errors.New("firmware image redirect refused")
		}
		return nil
	}
	response, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		return nil, "", errors.Join(camera.ErrFirmwareImage, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, "", camera.ErrFirmwareImage
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, MaxDownload+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		return nil, "", errors.Join(camera.ErrFirmwareImage, err)
	}
	if len(data) > MaxDownload {
		return nil, "", camera.ErrFirmwareImage
	}
	name := path.Base(response.Request.URL.Path)
	if unescaped, err := url.PathUnescape(name); err == nil {
		name = unescaped
	}
	return data, name, nil
}

// Select returns data itself when it has digest, or else a regular file in a
// zip archive that has it. Any other content is refused.
func Select(data []byte, name, digest string) (camera.FirmwareFile, error) {
	if len(data) <= MaxImage && hash(data) == digest {
		return camera.FirmwareFile{Name: name, SHA256: digest, Data: data}, nil
	}
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return camera.FirmwareFile{}, camera.ErrFirmwareImage
	}
	for _, entry := range archive.File {
		if !entry.Mode().IsRegular() || entry.UncompressedSize64 > MaxImage {
			continue
		}
		r, err := entry.Open()
		if err != nil {
			continue
		}
		content, err := io.ReadAll(io.LimitReader(r, MaxImage+1))
		r.Close()
		if err != nil || len(content) > MaxImage || hash(content) != digest {
			continue
		}
		return camera.FirmwareFile{Name: path.Base(entry.Name), SHA256: digest, Data: content}, nil
	}
	return camera.FirmwareFile{}, camera.ErrFirmwareImage
}

func hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
