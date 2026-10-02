// Package k8s is the little of the Kubernetes API that `shale init` needs
// inside a cluster: putting what it made into Secrets (§34.5). It speaks
// to the API server with the pod's service account and needs no client
// library.
package k8s

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// Client is the API server as this pod.
type Client struct {
	base      string
	namespace string
	token     string
	http      *http.Client
}

// InCluster answers a client from the pod's environment, or false outside
// a cluster.
func InCluster() (*Client, bool) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, false
	}
	token, err := os.ReadFile(saDir + "/token")
	if err != nil {
		return nil, false
	}
	ns, err := os.ReadFile(saDir + "/namespace")
	if err != nil {
		return nil, false
	}
	pool := x509.NewCertPool()
	if ca, err := os.ReadFile(saDir + "/ca.crt"); err == nil {
		pool.AppendCertsFromPEM(ca)
	}

	return &Client{
		base:      "https://" + host + ":" + port,
		namespace: strings.TrimSpace(string(ns)),
		token:     strings.TrimSpace(string(token)),
		http:      &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}},
	}, true
}

// New is a client of the API server at base, as the holder of token, in
// namespace: what [InCluster] reads from the pod, given instead.
func New(base, namespace, token string, hc *http.Client) *Client {
	return &Client{base: strings.TrimSuffix(base, "/"), namespace: namespace, token: token, http: hc}
}

// Namespace is the pod's namespace.
func (c *Client) Namespace() string { return c.namespace }

func (c *Client) do(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, r)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, b, nil
}

// SecretExists says whether the named Secret is there.
func (c *Client) SecretExists(ctx context.Context, name string) (bool, error) {
	code, b, err := c.do(ctx, http.MethodGet, "/api/v1/namespaces/"+c.namespace+"/secrets/"+name, nil)
	if err != nil {
		return false, err
	}
	switch code {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}

	return false, fmt.Errorf("GET secret %s: %d: %s", name, code, strings.TrimSpace(string(b)))
}

// ErrExists is a Secret that is already there.
var ErrExists = errors.New("already exists")

// Secret is an Opaque Secret to make.
type Secret struct {
	Name        string
	Labels      map[string]string
	Annotations map[string]string
	Data        map[string][]byte
}

// CreateSecret makes an Opaque Secret from the files; it refuses one that
// is already there, since that is somebody else's state.
func (c *Client) CreateSecret(ctx context.Context, name string, files map[string][]byte) error {
	return c.Create(ctx, Secret{Name: name, Data: files}, false)
}

// Create makes the Secret, and never replaces one: one that is there is
// [ErrExists]. A dry run is the API server's (`dryRun=All`): everything a
// create is checked against -- the name, the keys, the service account's
// right to it, a Secret by that name -- with nothing written.
func (c *Client) Create(ctx context.Context, s Secret, dryRun bool) error {
	data := map[string]string{}
	for k, v := range s.Data {
		data[k] = base64.StdEncoding.EncodeToString(v)
	}
	meta := map[string]any{"name": s.Name, "namespace": c.namespace}
	if len(s.Labels) > 0 {
		meta["labels"] = s.Labels
	}
	if len(s.Annotations) > 0 {
		meta["annotations"] = s.Annotations
	}
	body := map[string]any{
		"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
		"metadata": meta,
		"data":     data,
	}
	path := "/api/v1/namespaces/" + c.namespace + "/secrets"
	if dryRun {
		path += "?dryRun=All"
	}
	code, b, err := c.do(ctx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	switch code {
	case http.StatusCreated, http.StatusOK:
		return nil
	case http.StatusConflict:
		return fmt.Errorf("secret %s/%s: %w", c.namespace, s.Name, ErrExists)
	}

	return fmt.Errorf("POST secret %s: %d: %s", s.Name, code, strings.TrimSpace(string(b)))
}
