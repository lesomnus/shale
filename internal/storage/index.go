package storage

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// Entry is one complete object file as the index knows it (§29).
type Entry struct {
	Key        string
	ObjectId   pdid.Id
	AttemptId  pdid.Id
	Size       int64
	Started    time.Time
	Ended      time.Time
	Expired    time.Time
	Deleted    time.Time
	Incomplete bool
	Committed  time.Time
	Record     *api.ObjectRecord
}

// Index is a sink's in-memory index of complete files, rebuilt from the
// xattrs at startup (§29).
type Index struct {
	mu      sync.RWMutex
	byKey   map[string]*Entry
	bytes   int64
	newest  time.Time
	scanned bool
}

// NewIndex makes an empty index.
func NewIndex() *Index {
	return &Index{byKey: map[string]*Entry{}}
}

// Add puts or replaces an entry.
func (x *Index) Add(e *Entry) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if old, ok := x.byKey[e.Key]; ok {
		x.bytes -= old.Size
	}
	x.byKey[e.Key] = e
	x.bytes += e.Size
	if e.Committed.After(x.newest) {
		x.newest = e.Committed
	}
}

// Remove forgets a key.
func (x *Index) Remove(key string) *Entry {
	x.mu.Lock()
	defer x.mu.Unlock()
	e, ok := x.byKey[key]
	if !ok {
		return nil
	}
	delete(x.byKey, key)
	x.bytes -= e.Size

	return e
}

// Get answers an entry.
func (x *Index) Get(key string) (*Entry, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	e, ok := x.byKey[key]

	return e, ok
}

// Len is how many objects the index holds.
func (x *Index) Len() int {
	x.mu.RLock()
	defer x.mu.RUnlock()

	return len(x.byKey)
}

// Bytes is the sum of the sizes.
func (x *Index) Bytes() int64 {
	x.mu.RLock()
	defer x.mu.RUnlock()

	return x.bytes
}

// Newest is the latest commit time known.
func (x *Index) Newest() time.Time {
	x.mu.RLock()
	defer x.mu.RUnlock()

	return x.newest
}

// Scanned says whether the startup scan has finished.
func (x *Index) Scanned() bool {
	x.mu.RLock()
	defer x.mu.RUnlock()

	return x.scanned
}

// Snapshot copies every entry, for a walk that must not hold the lock.
func (x *Index) Snapshot() []*Entry {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make([]*Entry, 0, len(x.byKey))
	for _, e := range x.byKey {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}

// EntryOf builds an entry from a record and the file's size.
func EntryOf(key string, r *api.ObjectRecord, size int64, committed time.Time) *Entry {
	e := &Entry{Key: key, Size: size, Record: r, Incomplete: r.GetIncomplete(), Committed: committed}
	e.ObjectId, _ = pdid.From(r.GetObjectId())
	e.AttemptId, _ = pdid.From(r.GetAttemptId())
	if r.GetDateStartedMs() > 0 {
		e.Started = time.UnixMilli(r.GetDateStartedMs()).UTC()
	}
	if r.GetDateEndedMs() > 0 {
		e.Ended = time.UnixMilli(r.GetDateEndedMs()).UTC()
	}
	if r.GetDateExpiredMs() > 0 {
		e.Expired = time.UnixMilli(r.GetDateExpiredMs()).UTC()
	}
	if r.GetDateDeletedMs() > 0 {
		e.Deleted = time.UnixMilli(r.GetDateDeletedMs()).UTC()
	}

	return e
}

// ScanResult is what the startup scan found besides complete files.
type ScanResult struct {
	Complete int
	Open     []OpenFile
	Damaged  []string
	Unknown  int
}

// OpenFile is an upload the scan found still open.
type OpenFile struct {
	Key      string
	Record   *api.ObjectRecord
	Size     int64
	Modified time.Time
}

// Scan walks the sink's objects and rebuilds the index from their records
// (§29): inodes only, never data. Complete files whose size disagrees with
// the record are damaged; open files are handed back for the abandon rule
// (§12.2).
func (x *Index) Scan(ctx context.Context, sink *Sink, progress func(n int), yield func() error) (ScanResult, error) {
	var res ScanResult
	root := filepath.Join(sink.Path, "objects")
	n := 0
	seen := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		// A MAINT step every so many files: the walk is inode reads, and
		// the device's writes and reads get their turns between (§24).
		seen++
		if yield != nil && seen%256 == 0 {
			if err := yield(); err != nil {
				return err
			}
		}
		rel, err := filepath.Rel(sink.Path, p)
		if err != nil {
			return nil
		}
		key := filepath.ToSlash(rel)
		info, err := d.Info()
		if err != nil {
			return nil
		}
		r, err := ReadRecordPath(p)
		if err != nil {
			// Not ours, or unreadable: left alone (§23.1).
			res.Unknown++
			return nil
		}
		n++
		if progress != nil && n%1000 == 0 {
			progress(n)
		}
		switch r.GetState() {
		case api.RecordState_RECORD_STATE_COMPLETE:
			if r.GetSize() != info.Size() {
				res.Damaged = append(res.Damaged, key)
				return nil
			}
			x.Add(EntryOf(key, r, info.Size(), info.ModTime()))
			res.Complete++
		case api.RecordState_RECORD_STATE_OPEN:
			res.Open = append(res.Open, OpenFile{Key: key, Record: r, Size: info.Size(), Modified: info.ModTime()})
		default:
			res.Unknown++
		}

		return nil
	})

	x.mu.Lock()
	x.scanned = true
	x.mu.Unlock()

	if err != nil && !os.IsNotExist(err) {
		return res, err
	}

	return res, nil
}
