package satellite

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// assigned are the producers this satellite serves (§41.3): the adopted
// producers the CP assigned to it, each with the serial of the certificate
// its row records. Every heartbeat's answer replaces the whole list; it is
// kept in the state directory too, so a satellite that restarts while the
// CP is away still serves the producers it served.
//
// A caller it does not know, or whose certificate is not the one the CP
// knows, may only be news the next heartbeat brings: a producer assigned a
// moment ago, or one that renewed its certificate. Such a caller has the
// list asked for again at once, at most every refreshEvery, before it is
// refused.
type assigned struct {
	dir string
	// refresh asks the CP for the list now: one heartbeat.
	refresh func(ctx context.Context) error

	mu   sync.RWMutex
	byId map[pdid.Id]string

	refreshMu sync.Mutex
	refreshed time.Time
}

const (
	// refreshEvery bounds how often callers it does not know make the
	// satellite ask the CP.
	refreshEvery = time.Second
	// refreshTimeout bounds how long such a caller waits for the answer.
	refreshTimeout = 3 * time.Second
)

func (a *assigned) file() string { return filepath.Join(a.dir, "producers.json") }

// load reads the list the satellite last held.
func (a *assigned) load() {
	if a.dir == "" {
		return
	}
	b, err := os.ReadFile(a.file())
	if err != nil {
		return
	}
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return
	}
	byId := map[pdid.Id]string{}
	for k, v := range m {
		if id, err := pdid.Parse(k); err == nil && id.Domain() == DomProducer {
			byId[id] = v
		}
	}
	a.mu.Lock()
	a.byId = byId
	a.mu.Unlock()
}

// set replaces the list with what the CP answered.
func (a *assigned) set(vs []*api.SatelliteProducer) {
	byId := make(map[pdid.Id]string, len(vs))
	for _, v := range vs {
		id, err := pdid.From(v.GetProducerId())
		if err != nil || id.Domain() != DomProducer {
			continue
		}
		byId[id] = v.GetCertSerial()
	}
	a.mu.Lock()
	same := len(byId) == len(a.byId)
	if same {
		for k, v := range byId {
			if a.byId[k] != v {
				same = false
				break
			}
		}
	}
	a.byId = byId
	a.mu.Unlock()
	if same || a.dir == "" {
		return
	}
	m := make(map[string]string, len(byId))
	for k, v := range byId {
		m[k.String()] = v
	}
	if b, err := json.Marshal(m); err == nil {
		tmp := a.file() + ".tmp"
		if os.WriteFile(tmp, b, 0o600) == nil {
			os.Rename(tmp, a.file())
		}
	}
}

// len is how many producers it serves.
func (a *assigned) len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return len(a.byId)
}

// has says whether the list names producer `id`, with certificate `serial`
// unless serial is empty (development mode, which has no certificates).
func (a *assigned) has(id pdid.Id, serial string, dev bool) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	want, ok := a.byId[id]
	if !ok {
		return false
	}

	return dev || (serial != "" && want == serial)
}

// serves says whether the satellite serves this caller, asking the CP
// again first when the list it holds says no (§41.3).
func (a *assigned) serves(ctx context.Context, id pdid.Id, serial string, dev bool) bool {
	if a.has(id, serial, dev) {
		return true
	}
	if a.refresh == nil {
		return false
	}
	asked := time.Now()
	a.refreshMu.Lock()
	if a.refreshed.Before(asked) && time.Since(a.refreshed) >= refreshEvery {
		rctx, cancel := context.WithTimeout(ctx, refreshTimeout)
		a.refresh(rctx)
		cancel()
		a.refreshed = time.Now()
	}
	a.refreshMu.Unlock()

	return a.has(id, serial, dev)
}
