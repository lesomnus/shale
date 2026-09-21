package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// TestScan is the startup scan over a sink laid out by hand (§29, §23):
// complete files are indexed, an open one is handed back to the abandon
// rule, a file whose size disagrees with its record is damaged, and a
// stranger's file is left alone.
func TestScan(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenSink(SinkConfig{Path: dir, Capacity: 1 << 30, Device: "test"}, DefaultWatermarks)
	if err != nil {
		t.Skip("no xattrs here:", err)
	}
	write := func(key string, size int64, state api.RecordState, recSize int64) {
		p, _ := s.FilePath(key)
		os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
		rec := api.LaminaRecord_builder{
			FormatVersion: FormatVersion, State: state, Size: recSize, LaminaId: pdid.New(9).Bytes(), AttemptId: pdid.New(10).Bytes(),
			DateStartedMs: time.Now().Add(-time.Hour).UnixMilli(), DateExpiredMs: time.Now().Add(24 * time.Hour).UnixMilli(),
		}.Build()
		if err := WriteRecordPath(p, rec); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 300 {
		write(fmt.Sprintf("laminae/2026/09/20/01/%032x.%032x", i, i), 1000+int64(i), api.RecordState_RECORD_STATE_COMPLETE, 1000+int64(i))
	}
	write("laminae/2026/09/20/02/open.open", 4096, api.RecordState_RECORD_STATE_OPEN, 0)
	write("laminae/2026/09/20/02/short.short", 500, api.RecordState_RECORD_STATE_COMPLETE, 1000)
	p, _ := s.FilePath("laminae/2026/09/20/03/stranger.file")
	os.MkdirAll(filepath.Dir(p), 0o755)
	os.WriteFile(p, []byte("not ours"), 0o644)

	steps := 0
	res, err := s.Index.Scan(context.Background(), s, nil, func() error { steps++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if res.Complete != 300 || s.Index.Len() != 300 {
		t.Fatalf("complete %d, indexed %d", res.Complete, s.Index.Len())
	}
	if len(res.Open) != 1 || res.Open[0].Key != "laminae/2026/09/20/02/open.open" || res.Open[0].Size != 4096 {
		t.Fatalf("open: %+v", res.Open)
	}
	if len(res.Damaged) != 1 || res.Damaged[0] != "laminae/2026/09/20/02/short.short" {
		t.Fatalf("damaged: %v", res.Damaged)
	}
	if res.Unknown != 1 {
		t.Fatalf("unknown %d", res.Unknown)
	}
	if steps == 0 {
		t.Fatal("the scan never yielded to the device")
	}
	if !s.Index.Scanned() {
		t.Fatal("not marked scanned")
	}
	// The same result every time: a hundred scans agree.
	for range 100 {
		x := NewIndex()
		r, err := x.Scan(context.Background(), s, nil, nil)
		if err != nil || r.Complete != 300 || len(r.Open) != 1 || len(r.Damaged) != 1 {
			t.Fatalf("scan disagreed: %+v %v", r, err)
		}
	}
}
