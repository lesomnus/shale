package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// The HTTP data plane (§35.6): resumable uploads (§12.2), idempotent
// (§12.5), commits (§12.4), partial objects (§15), and reads with Range
// (§17).

// Header names.
const (
	HdrUploadOffset   = "Upload-Offset"
	HdrUploadComplete = "Upload-Complete"
	HdrUploadLength   = "Upload-Length"
	HdrSizeHint       = "Shale-Size-Hint"
	HdrDateStarted    = "Shale-Date-Started"
	HdrDateEnded      = "Shale-Date-Ended"
	HdrIncomplete     = "Shale-Incomplete"
	HdrRetryAfter     = "Retry-After"
)

// Limits are the node's admission limits (§12.2, §17.4).
type Limits struct {
	MaxUploads       int
	UploadsPerActor  int
	MaxReadSessions  int
	SessionsPerActor int
	IdleTimeoutMax   time.Duration
	// Chunk is the buffer a request body is copied through.
	Chunk int
}

// DefaultLimits are §36.1's.
var DefaultLimits = Limits{
	MaxUploads:       64,
	UploadsPerActor:  64,
	MaxReadSessions:  64,
	SessionsPerActor: 16,
	IdleTimeoutMax:   5 * time.Minute,
	Chunk:            1 << 20,
}

// upload is one open upload the node tracks in RAM: the file is the only
// durable state, this is bookkeeping for the abandon rule and admission.
type upload struct {
	mu       sync.Mutex
	key      string
	sink     *Sink
	actor    pdid.Id
	claims   *api.TokenClaims
	record   *api.ObjectRecord
	last     time.Time
	inflight bool
	hint     int64
	started  *time.Time
}

// DataPlane serves the object paths of every sink on this node.
type DataPlane struct {
	node   *Node
	limits Limits
	log    *slog.Logger

	mu       sync.Mutex
	uploads  map[string]*upload // by sink+key
	perActor map[pdid.Id]int
	readers  map[pdid.Id]int
	nReaders int
}

func newDataPlane(n *Node, l Limits) *DataPlane {
	if l.Chunk <= 0 {
		l.Chunk = DefaultLimits.Chunk
	}

	return &DataPlane{node: n, limits: l, log: n.log, uploads: map[string]*upload{}, perActor: map[pdid.Id]int{}, readers: map[pdid.Id]int{}}
}

func (d *DataPlane) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/healthz":
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
		return
	case strings.HasPrefix(r.URL.Path, "/objects/"):
	default:
		http.NotFound(w, r)
		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/")

	tok := token.FromHeader(r.Header.Get("Authorization"))
	if tok == "" {
		tok = r.URL.Query().Get("token")
	}
	if tok == "" {
		w.Header().Set("WWW-Authenticate", token.Scheme)
		http.Error(w, "no token", http.StatusUnauthorized)
		return
	}
	c, err := d.node.verifier.Verify(tok)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	var ops []api.TokenOp
	switch r.Method {
	case http.MethodPut:
		ops = []api.TokenOp{api.TokenOp_TOKEN_OP_PUT}
	case http.MethodGet:
		ops = []api.TokenOp{api.TokenOp_TOKEN_OP_GET}
	case http.MethodHead:
		ops = []api.TokenOp{api.TokenOp_TOKEN_OP_PUT, api.TokenOp_TOKEN_OP_GET}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := token.Check(c, d.node.id.Bytes(), ops...); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	if c.GetObjectKey() != key {
		http.Error(w, "the token names another key", http.StatusForbidden)
		return
	}

	sinkId, err := pdid.From(c.GetSinkId())
	if err != nil {
		http.Error(w, "the token names no sink", http.StatusForbidden)
		return
	}
	sink := d.node.sinkOf(sinkId)
	if sink == nil || !sink.Serves() {
		http.Error(w, "this node does not serve that sink", http.StatusServiceUnavailable)
		return
	}

	actor, _ := pdid.From(c.GetActor())

	switch r.Method {
	case http.MethodPut:
		d.put(w, r, sink, key, c, actor)
	case http.MethodHead:
		d.head(w, r, sink, key, c)
	case http.MethodGet:
		d.get(w, r, sink, key, c, actor)
	}
}

// parseBool reads a structured-field boolean: ?1 or ?0.
func parseBool(v string) (bool, bool) {
	switch strings.TrimSpace(v) {
	case "?1", "1", "true":
		return true, true
	case "?0", "0", "false":
		return false, true
	}

	return false, false
}

func setBool(h http.Header, name string, v bool) {
	if v {
		h.Set(name, "?1")
	} else {
		h.Set(name, "?0")
	}
}

func (d *DataPlane) uploadKey(sink *Sink, key string) string {
	return sink.Id.String() + "/" + key
}

// admit counts an upload against the sink and the actor (§12.2).
func (d *DataPlane) admit(sink *Sink, actor pdid.Id) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if int(sink.uploads.Load()) >= d.limits.MaxUploads || d.perActor[actor] >= d.limits.UploadsPerActor {
		return false
	}
	sink.uploads.Add(1)
	d.perActor[actor]++

	return true
}

func (d *DataPlane) release(sink *Sink, actor pdid.Id) {
	d.mu.Lock()
	defer d.mu.Unlock()
	sink.uploads.Add(-1)
	if d.perActor[actor] > 0 {
		d.perActor[actor]--
	}
}

// put is the resumable upload (§12.2).
func (d *DataPlane) put(w http.ResponseWriter, r *http.Request, sink *Sink, key string, c *api.TokenClaims, actor pdid.Id) {
	path, err := sink.FilePath(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get(HdrUploadOffset)), 10, 64)
	if err != nil || offset < 0 {
		http.Error(w, HdrUploadOffset+" is required", http.StatusBadRequest)
		return
	}
	complete := true
	if v := r.Header.Get(HdrUploadComplete); v != "" {
		b, ok := parseBool(v)
		if !ok {
			http.Error(w, HdrUploadComplete+" must be ?1 or ?0", http.StatusBadRequest)
			return
		}
		complete = b
	}
	var length int64 = -1
	if v := r.Header.Get(HdrUploadLength); v != "" {
		length, err = strconv.ParseInt(v, 10, 64)
		if err != nil || length < 0 {
			http.Error(w, HdrUploadLength+" is not a size", http.StatusBadRequest)
			return
		}
	}
	if length < 0 && r.ContentLength >= 0 && complete {
		length = offset + r.ContentLength
	}
	maxLen := c.GetMaxLength()
	if maxLen > 0 && length > maxLen {
		http.Error(w, fmt.Sprintf("the upload would exceed max_length %d", maxLen), http.StatusRequestEntityTooLarge)
		return
	}

	var hint int64
	if v := r.Header.Get(HdrSizeHint); v != "" {
		hint, _ = strconv.ParseInt(v, 10, 64)
	}
	if length >= 0 {
		hint = length
	}
	if maxLen > 0 && hint > maxLen {
		hint = maxLen
	}

	var started *time.Time
	if v := r.Header.Get(HdrDateStarted); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			http.Error(w, HdrDateStarted+" is not RFC 3339", http.StatusBadRequest)
			return
		}
		started = &t
	}

	// One writer per key: a second request waits for the first to finish
	// or to be cut off (§12.2).
	uk := d.uploadKey(sink, key)
	d.mu.Lock()
	u, ok := d.uploads[uk]
	if !ok {
		u = &upload{key: key, sink: sink, actor: actor, claims: c}
		d.uploads[uk] = u
	}
	d.mu.Unlock()

	u.mu.Lock()
	defer u.mu.Unlock()
	u.last = time.Now()
	u.inflight = true
	defer func() {
		u.inflight = false
		u.last = time.Now()
	}()

	// What is on disk already.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	var cur int64
	var rec *api.ObjectRecord
	switch {
	case err == nil:
		rec, err = ReadRecord(f)
		if err != nil {
			f.Close()
			http.Error(w, "the file has no record", http.StatusConflict)
			return
		}
		if rec.GetState() == api.RecordState_RECORD_STATE_COMPLETE {
			f.Close()
			// Already complete: the producer may have missed the 201 (§12.5).
			setBool(w.Header(), HdrUploadComplete, true)
			w.Header().Set(HdrUploadOffset, strconv.FormatInt(rec.GetSize(), 10))
			if rec.GetIncomplete() {
				setBool(w.Header(), HdrIncomplete, true)
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		cur = st.Size()
		if length >= 0 && rec.GetSizeHint() > 0 && r.Header.Get(HdrUploadLength) != "" && rec.GetSizeHint() != length && rec.GetMode() == api.UploadMode_UPLOAD_MODE_BUFFERED {
			f.Close()
			http.Error(w, "a different Upload-Length for the same key", http.StatusConflict)
			return
		}
	case errors.Is(err, os.ErrNotExist):
		if offset != 0 {
			w.Header().Set(HdrUploadOffset, "0")
			http.Error(w, "no upload at this key yet", http.StatusConflict)
			return
		}
		if !sink.AcceptsWrites() {
			w.Header().Set(HdrRetryAfter, "30")
			http.Error(w, "the sink takes no writes now", http.StatusServiceUnavailable)
			return
		}
		if !d.admit(sink, actor) {
			w.Header().Set(HdrRetryAfter, "10")
			http.Error(w, "at the upload limit", http.StatusServiceUnavailable)
			return
		}
		defer d.release(sink, actor)

		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rec = RecordFromToken(c, hint)
		if started != nil {
			rec.SetDateStartedMs(started.UnixMilli())
		}
		if err := WriteRecord(f, rec); err != nil {
			f.Close()
			os.Remove(path)
			http.Error(w, "cannot write the record: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if sink.Caps.GetFallocate() && hint > 0 {
			reserve(f, hint)
		}
		sink.Reserve(hint)
		u.hint = hint
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	u.record = rec

	if offset != cur {
		w.Header().Set(HdrUploadOffset, strconv.FormatInt(cur, 10))
		setBool(w.Header(), HdrUploadComplete, false)
		http.Error(w, "offset mismatch", http.StatusConflict)
		return
	}

	// Copy the body in, with the idle timeout on every read (§12.2).
	idle := time.Duration(c.GetIdleTimeoutSeconds()) * time.Second
	if idle <= 0 || idle > d.limits.IdleTimeoutMax {
		idle = d.limits.IdleTimeoutMax
	}
	rc := http.NewResponseController(w)
	buf := make([]byte, d.limits.Chunk)
	written := cur
	tooLarge := false
	var readErr error
	for {
		rc.SetReadDeadline(time.Now().Add(idle))
		n, err := r.Body.Read(buf)
		if n > 0 {
			if maxLen > 0 && written+int64(n) > maxLen {
				tooLarge = true
				n = int(maxLen - written)
			}
			if n > 0 {
				if _, werr := f.WriteAt(buf[:n], written); werr != nil {
					readErr = werr
					break
				}
				written += int64(n)
				u.last = time.Now()
			}
			if tooLarge {
				break
			}
		}
		if err != nil {
			if err != io.EOF {
				readErr = err
			}
			break
		}
	}

	if tooLarge {
		// Beyond max_length: the node treats the upload as abandoned at once
		// (§12.5).
		d.abandon(u, f, written, "413")
		http.Error(w, "the upload exceeds max_length", http.StatusRequestEntityTooLarge)
		return
	}
	if readErr != nil {
		// A broken connection leaves the upload open and resumable.
		d.log.Debug("upload interrupted", "key", key, "offset", written, "err", readErr.Error())
		return
	}

	if !complete {
		// The flush point: what is held is written; the offset is on the
		// device (§12.2).
		if err := f.Sync(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set(HdrUploadOffset, strconv.FormatInt(written, 10))
		setBool(w.Header(), HdrUploadComplete, false)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Complete: the end of this body is the end of the object (§12.4).
	var ended *time.Time
	if v := r.Header.Get(HdrDateEnded); v != "" {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			ended = &t
		}
	}
	if ended == nil {
		if v := r.Trailer.Get(HdrDateEnded); v != "" {
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				ended = &t
			}
		}
	}
	if err := d.commit(u, f, written, ended, false); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(HdrUploadOffset, strconv.FormatInt(written, 10))
	setBool(w.Header(), HdrUploadComplete, true)
	w.WriteHeader(http.StatusCreated)
}

// commit finalizes an open file: the record says complete with the final
// size, one fsync, the index, the event (§12.1 steps 8-10).
func (d *DataPlane) commit(u *upload, f *os.File, size int64, ended *time.Time, incomplete bool) error {
	sink := u.sink
	rec := u.record
	if err := f.Truncate(size); err != nil {
		return err
	}
	if sink.Caps.GetFallocate() && u.hint > size {
		unreserve(f, size, u.hint)
	}
	now := time.Now().UTC()
	rec.SetState(api.RecordState_RECORD_STATE_COMPLETE)
	rec.SetSize(size)
	rec.SetIncomplete(incomplete)
	if ended != nil && !incomplete {
		rec.SetDateEndedMs(ended.UnixMilli())
	}
	if err := WriteRecord(f, rec); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	sink.Reserve(-u.hint)
	u.hint = 0

	e := EntryOf(u.key, rec, size, now)
	sink.Index.Add(e)

	d.mu.Lock()
	delete(d.uploads, d.uploadKey(sink, u.key))
	d.mu.Unlock()

	d.node.outbox.Push(api.Event_builder{Stored: storedEvent(sink, e, now)}.Build())

	return nil
}

func storedEvent(sink *Sink, e *Entry, committed time.Time) *api.ObjectStored {
	ev := api.ObjectStored_builder{
		ObjectId:      e.Record.GetObjectId(),
		AttemptId:     e.Record.GetAttemptId(),
		SinkId:        sink.Id.Bytes(),
		ObjectKey:     e.Key,
		Size:          e.Size,
		Incomplete:    e.Incomplete,
		DateCommitted: timestamppb.New(committed),
		SourceId:      e.Record.GetSourceId(),
		Checksum:      e.Record.GetChecksum(),
		Record:        e.Record,
	}
	if !e.Started.IsZero() {
		ev.DateStarted = timestamppb.New(e.Started)
	}
	if !e.Ended.IsZero() {
		ev.DateEnded = timestamppb.New(e.Ended)
	}

	return ev.Build()
}

// abandon applies the abandon rule to an open upload (§15, §12.5): a live
// one is finalized incomplete, a buffered one is deleted and reported.
func (d *DataPlane) abandon(u *upload, f *os.File, size int64, why string) {
	sink := u.sink
	if u.record.GetMode() == api.UploadMode_UPLOAD_MODE_BUFFERED || size == 0 {
		path, _ := sink.FilePath(u.key)
		if f != nil {
			f.Close()
		}
		os.Remove(path)
		sink.Reserve(-u.hint)
		d.mu.Lock()
		delete(d.uploads, d.uploadKey(sink, u.key))
		d.mu.Unlock()
		d.node.outbox.Push(api.Event_builder{Missing: api.ObjectMissing_builder{
			SinkId:       sink.Id.Bytes(),
			ObjectKey:    u.key,
			ObjectId:     u.record.GetObjectId(),
			AttemptId:    u.record.GetAttemptId(),
			Reason:       api.MissingReason_MISSING_REASON_ABANDONED,
			DateObserved: timestamppb.Now(),
		}.Build()}.Build())
		d.log.Info("abandoned buffered upload deleted", "key", u.key, "why", why)
		return
	}

	own := false
	if f == nil {
		path, _ := sink.FilePath(u.key)
		var err error
		f, err = os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return
		}
		own = true
	}
	if own {
		defer f.Close()
	}
	if err := d.commit(u, f, size, nil, true); err != nil {
		d.log.Warn("finalize incomplete", "key", u.key, "err", err.Error())
		return
	}
	d.log.Info("abandoned live upload finalized incomplete", "key", u.key, "size", size, "why", why)
}

// head answers the upload offset and completeness, or the object's
// metadata (§12.2).
func (d *DataPlane) head(w http.ResponseWriter, r *http.Request, sink *Sink, key string, c *api.TokenClaims) {
	path, err := sink.FilePath(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	st, err := os.Stat(path)
	if err != nil {
		w.Header().Set(HdrUploadOffset, "0")
		setBool(w.Header(), HdrUploadComplete, false)
		if c.GetOp() == api.TokenOp_TOKEN_OP_GET {
			d.missing(sink, key, c)
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	rec, err := ReadRecordPath(path)
	if err != nil {
		http.Error(w, "the file has no record", http.StatusConflict)
		return
	}
	complete := rec.GetState() == api.RecordState_RECORD_STATE_COMPLETE
	setBool(w.Header(), HdrUploadComplete, complete)
	w.Header().Set(HdrUploadOffset, strconv.FormatInt(st.Size(), 10))
	if complete {
		w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
		w.Header().Set(HdrUploadLength, strconv.FormatInt(st.Size(), 10))
		if rec.GetIncomplete() {
			setBool(w.Header(), HdrIncomplete, true)
		}
	} else if rec.GetSizeHint() > 0 {
		w.Header().Set(HdrUploadLength, strconv.FormatInt(rec.GetSizeHint(), 10))
	}
	w.WriteHeader(http.StatusOK)
}

// get serves a complete object with Range (§17).
func (d *DataPlane) get(w http.ResponseWriter, r *http.Request, sink *Sink, key string, c *api.TokenClaims, actor pdid.Id) {
	path, err := sink.FilePath(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	e, ok := sink.Index.Get(key)
	var size int64
	var mod time.Time
	if ok {
		size, mod = e.Size, e.Committed
	} else {
		st, err := os.Stat(path)
		if err != nil {
			d.missing(sink, key, c)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		rec, err := ReadRecordPath(path)
		if err != nil || rec.GetState() != api.RecordState_RECORD_STATE_COMPLETE {
			http.Error(w, "not complete", http.StatusNotFound)
			return
		}
		size, mod = st.Size(), st.ModTime()
	}

	d.mu.Lock()
	if d.nReaders >= d.limits.MaxReadSessions || d.readers[actor] >= d.limits.SessionsPerActor {
		d.mu.Unlock()
		w.Header().Set(HdrRetryAfter, "5")
		http.Error(w, "at the read session limit", http.StatusServiceUnavailable)
		return
	}
	d.nReaders++
	d.readers[actor]++
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.nReaders--
		d.readers[actor]--
		d.mu.Unlock()
	}()

	f, err := os.Open(path)
	if err != nil {
		d.missing(sink, key, c)
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "video/mp2t")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "private, max-age=0")
	_ = size
	http.ServeContent(w, r, "", mod, f)
}

// missing reports a key a valid token named that this node does not have
// (§14).
func (d *DataPlane) missing(sink *Sink, key string, c *api.TokenClaims) {
	d.node.outbox.Push(api.Event_builder{Missing: api.ObjectMissing_builder{
		SinkId:       sink.Id.Bytes(),
		ObjectKey:    key,
		ObjectId:     c.GetRecord().GetObjectId(),
		AttemptId:    c.GetAttemptId(),
		Reason:       api.MissingReason_MISSING_REASON_NOT_FOUND,
		DateObserved: timestamppb.Now(),
	}.Build()}.Build())
}

// sweepAbandoned applies the abandon rule to uploads idle past their
// abandon_timeout with no request open (§12.2, §15).
func (d *DataPlane) sweepAbandoned(ctx context.Context) {
	d.mu.Lock()
	var us []*upload
	for _, u := range d.uploads {
		us = append(us, u)
	}
	d.mu.Unlock()

	now := time.Now()
	for _, u := range us {
		if !u.mu.TryLock() {
			continue
		}
		if u.inflight || u.record == nil {
			u.mu.Unlock()
			continue
		}
		timeout := time.Duration(u.record.GetAbandonTimeoutSeconds()) * time.Second
		if timeout <= 0 {
			timeout = 5 * time.Minute
		}
		if now.Sub(u.last) < timeout {
			u.mu.Unlock()
			continue
		}
		path, _ := u.sink.FilePath(u.key)
		st, err := os.Stat(path)
		if err != nil {
			d.mu.Lock()
			delete(d.uploads, d.uploadKey(u.sink, u.key))
			d.mu.Unlock()
			u.mu.Unlock()
			continue
		}
		d.abandon(u, nil, st.Size(), "abandon_timeout")
		u.mu.Unlock()
	}
}

// adoptOpen applies the abandon rule to open files the startup scan found
// (§12.2): they are left alone until idle for their abandon_timeout.
func (d *DataPlane) adoptOpen(sink *Sink, open []OpenFile) {
	for _, o := range open {
		u := &upload{key: o.Key, sink: sink, record: o.Record, last: o.Modified, hint: o.Record.GetSizeHint()}
		if id, err := pdid.From(o.Record.GetAttemptId()); err == nil {
			_ = id
		}
		sink.Reserve(u.hint)
		d.mu.Lock()
		d.uploads[d.uploadKey(sink, o.Key)] = u
		d.mu.Unlock()
	}
}
