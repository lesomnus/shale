// Package k8s is the little of the Kubernetes API that `shale init` needs
// inside a cluster: putting what it made into a Secret (§34.5). It speaks
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

// CreateSecret makes an Opaque Secret from the files; it refuses one that
// is already there, since that is somebody else's state.
func (c *Client) CreateSecret(ctx context.Context, name string, files map[string][]byte) error {
	data := map[string]string{}
	for k, v := range files {
		data[k] = base64.StdEncoding.EncodeToString(v)
	}
	body := map[string]any{
		"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
		"metadata": map[string]any{"name": name, "namespace": c.namespace},
		"data":     data,
	}
	code, b, err := c.do(ctx, http.MethodPost, "/api/v1/namespaces/"+c.namespace+"/secrets", body)
	if err != nil {
		return err
	}
	switch code {
	case http.StatusCreated, http.StatusOK:
		return nil
	case http.StatusConflict:
		return errors.New("secret " + name + " already exists")
	}

	return fmt.Errorf("POST secret %s: %d: %s", name, code, strings.TrimSpace(string(b)))
}
