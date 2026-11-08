package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Only the privileged disposable coordinator can traverse this directory. The
// protocol has fixed operations; application actors cannot choose shell commands,
// interfaces, namespace names, PIDs or filesystem paths.
type isolatedNetworkControl struct {
	directory string
	prefix    string
	mu        sync.Mutex
	sequence  uint64
}

var isolatedNetworkControlSequence atomic.Uint64

type isolatedNetworkCamera struct {
	MAC         string   `json:"mac"`
	Addresses   []string `json:"addresses"`
	ClientPID   int      `json:"client_pid"`
	ClientAlive bool     `json:"client_alive"`
	Lease       struct {
		IP string `json:"ip"`
	} `json:"lease"`
}

type isolatedNetworkStatus struct {
	Cameras   map[string]isolatedNetworkCamera `json:"cameras"`
	DHCPPID   int                              `json:"dhcp_pid"`
	DHCPAlive bool                             `json:"dhcp_alive"`
	UplinkUp  bool                             `json:"uplink_up"`
}

func newIsolatedNetworkControl(t *testing.T) *isolatedNetworkControl {
	t.Helper()
	file, err := os.Open(os.Getenv("SHALE_TOPOLOGY_FILE"))
	require.NoError(t, err)
	defer file.Close()
	var manifest struct {
		Control struct {
			Directory string `json:"directory"`
		} `json:"control"`
	}
	require.NoError(t, json.NewDecoder(io.LimitReader(file, 64<<10)).Decode(&manifest))
	require.True(t, filepath.IsAbs(manifest.Control.Directory))
	info, err := os.Lstat(manifest.Control.Directory)
	require.NoError(t, err)
	require.True(t, info.IsDir())
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	return &isolatedNetworkControl{directory: manifest.Control.Directory, prefix: fmt.Sprintf("go-%d-%d", os.Getpid(), isolatedNetworkControlSequence.Add(1))}
}

func (c *isolatedNetworkControl) call(ctx context.Context, operation, role, address string) (isolatedNetworkStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var empty isolatedNetworkStatus
	allowed := map[string]bool{"status": true, "move_camera": true, "renew_camera": true, "restart_dhcp": true, "uplink_down": true, "uplink_up": true}
	if !allowed[operation] || ((operation == "move_camera" || operation == "renew_camera") != (role != "")) || (operation == "move_camera") != (address != "") {
		return empty, errors.New("invalid isolated network operation")
	}
	c.sequence++
	id := fmt.Sprintf("%s-%d", c.prefix, c.sequence)
	request := map[string]any{"version": 1, "id": id, "op": operation}
	if role != "" {
		request["role"] = role
	}
	if address != "" {
		request["address"] = address
	}
	data, err := json.Marshal(request)
	if err != nil {
		return empty, err
	}
	responsePath := filepath.Join(c.directory, "response.json")
	if err := os.Remove(responsePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return empty, err
	}
	temporary := filepath.Join(c.directory, "request.go-tmp")
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return empty, err
	}
	defer os.Remove(temporary)
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return empty, err
	}
	if err := os.Rename(temporary, filepath.Join(c.directory, "request.json")); err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		file, err := os.Open(responsePath)
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(file, 65537))
			closeErr := file.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				return empty, err
			}
			if len(data) > 65536 {
				return empty, errors.New("isolated network response exceeds budget")
			}
			var response struct {
				Version int                   `json:"version"`
				ID      string                `json:"id"`
				OK      bool                  `json:"ok"`
				Code    string                `json:"code"`
				Status  isolatedNetworkStatus `json:"status"`
			}
			if json.Unmarshal(data, &response) != nil || response.Version != 1 {
				return empty, errors.New("invalid isolated network response")
			}
			if response.ID == id {
				if !response.OK {
					return empty, errors.New("isolated network operation failed")
				}
				return response.Status, nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return empty, err
		}
		select {
		case <-ctx.Done():
			return empty, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *isolatedNetworkControl) Call(t *testing.T, op, role, address string) isolatedNetworkStatus {
	t.Helper()
	status, err := c.call(t.Context(), op, role, address)
	require.NoError(t, err)
	require.True(t, status.DHCPAlive)
	return status
}
