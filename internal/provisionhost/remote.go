package provisionhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
)

type ExecutionResult struct {
	Run  provision.Run `json:"run"`
	Code string        `json:"code,omitempty"`
}

// RemoteExecutor calls an authenticated worker which resolves its own provider
// references locally. Requests contain no callback, lease or credential material.
type RemoteExecutor struct {
	URL               string
	Token             string
	HTTPClient        *http.Client
	AllowLoopbackHTTP bool
}

func (e *RemoteExecutor) Execute(ctx context.Context, q provision.Request, apply bool) (provision.Run, error) {
	u, err := url.Parse(e.URL)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(e.Token) < 32 || (u.Scheme != "https" && !(e.AllowLoopbackHTTP && u.Scheme == "http" && loopbackURL(e.URL))) {
		return provision.Run{}, camera.ErrForbidden
	}
	data, err := json.Marshal(struct {
		Request provision.Request `json:"request"`
		Apply   bool              `json:"apply"`
	}{q, apply})
	if err != nil {
		return provision.Run{}, camera.ErrInvalid
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(e.URL, "/")+"/v1/execute", bytes.NewReader(data))
	if err != nil {
		return provision.Run{}, camera.ErrInvalid
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.Token)
	client := e.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	client = safeRemoteClient(client)
	response, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return provision.Run{}, ctx.Err()
		}
		return provision.Run{}, errors.New("camera worker unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusUnauthorized {
			return provision.Run{}, camera.ErrForbidden
		}
		return provision.Run{}, errors.New("camera worker request failed")
	}
	var result ExecutionResult
	if err = json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&result); err != nil {
		return provision.Run{}, camera.ErrInvalid
	}
	if result.Code == "" && !reflectRequest(result.Run.Request, q) {
		return provision.Run{}, camera.ErrInvalid
	}
	if result.Run.Request.ID != "" && (!reflectRequest(result.Run.Request, q) || result.Run.Request.Scope != q.Scope) {
		return provision.Run{}, camera.ErrForbidden
	}
	return result.Run, codeError(result.Code)
}

func (e *RemoteExecutor) Checkpoint(ctx context.Context, q provision.Request) (provision.Run, error) {
	var run provision.Run
	connection := ConnectionConfig{URL: e.URL, AllowLoopbackHTTP: e.AllowLoopbackHTTP}
	if _, err := validateConnection(connection); err != nil {
		return run, err
	}
	if len(e.Token) < 32 {
		return run, camera.ErrForbidden
	}
	data, _ := json.Marshal(struct {
		Request provision.Request `json:"request"`
	}{q})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(e.URL, "/")+"/v1/checkpoint", bytes.NewReader(data))
	if err != nil {
		return run, camera.ErrInvalid
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.Token)
	client := e.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	client = safeRemoteClient(client)
	response, err := client.Do(req)
	if err != nil {
		return run, errors.New("camera worker unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return run, provision.ErrNotFound
	}
	if response.StatusCode != http.StatusOK {
		return run, camera.ErrForbidden
	}
	if json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&run) != nil || !reflectRequest(run.Request, q) {
		return provision.Run{}, camera.ErrForbidden
	}
	return run, nil
}

func (e *RemoteExecutor) Discover(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	if _, err := validateConnection(ConnectionConfig{URL: e.URL, AllowLoopbackHTTP: e.AllowLoopbackHTTP}); err != nil {
		return nil, err
	}
	if len(e.Token) < 32 {
		return nil, camera.ErrForbidden
	}
	data, _ := json.Marshal(struct {
		NetworkID string            `json:"network_id"`
		Seeds     []camera.Endpoint `json:"seeds,omitempty"`
	}{scope.ID, scope.Seeds})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(e.URL, "/")+"/v1/discovery-execute", bytes.NewReader(data))
	if err != nil {
		return nil, camera.ErrInvalid
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.Token)
	client := e.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	response, err := safeRemoteClient(client).Do(req)
	if err != nil {
		return nil, errors.New("camera discovery worker unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, camera.ErrForbidden
	}
	var observations []camera.Observation
	if json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&observations) != nil {
		return nil, camera.ErrInvalid
	}
	return observations, nil
}

func safeRemoteClient(client *http.Client) *http.Client {
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if copy.Timeout == 0 || copy.Timeout > 5*time.Minute {
		copy.Timeout = 5 * time.Minute
	}
	return &copy
}

// Call is the CLI's authenticated HTTP entry point. Remote controllers use the
// stricter RemoteExecutor checkpoint/execution methods above.
func Call(ctx context.Context, connection ConnectionConfig, method, path string, input, output any) error {
	client, token, err := connectionHTTP(connection)
	if err != nil {
		return err
	}
	if transport, ok := client.Transport.(*http.Transport); ok {
		defer transport.CloseIdleConnections()
	}
	if !strings.HasPrefix(path, "/v1/") || strings.Contains(path, "..") {
		return camera.ErrInvalid
	}
	var data []byte
	if input != nil {
		data, err = json.Marshal(input)
		if err != nil {
			return camera.ErrInvalid
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(connection.URL, "/")+path, bytes.NewReader(data))
	if err != nil {
		return camera.ErrInvalid
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(req)
	if err != nil {
		return errors.New("camera provisioner unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
		var result struct {
			Code string `json:"code"`
		}
		if json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result) != nil {
			return errors.New("camera provisioner request failed")
		}
		return codeError(result.Code)
	}
	if output == nil {
		return nil
	}
	if json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(output) != nil {
		return camera.ErrInvalid
	}
	return nil
}

func reflectRequest(a, b provision.Request) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func codeError(code string) error {
	switch code {
	case "":
		return nil
	case "forbidden":
		return camera.ErrForbidden
	case "credentials_required":
		return camera.ErrAuthRequired
	case "input_required":
		return camera.ErrNeedsUserInput
	case "unsupported":
		return camera.ErrUnsupported
	case "invalid":
		return camera.ErrInvalid
	case "conflict":
		return provision.ErrConflict
	case "not_found":
		return provision.ErrNotFound
	case "media_not_ready":
		return provision.ErrNotReady
	case "interrupted":
		return context.Canceled
	default:
		return errors.New("camera worker operation failed")
	}
}
