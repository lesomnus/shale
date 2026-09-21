package storage

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
)

// The data plane is the node's public face (§12.2, §17): any method,
// path, token, header and body must be answered with one of the codes the
// contract names, never a panic, and a token for another key or another
// node must open nothing (#57, #80). With a put token the body is
// uploaded, in one request or left open; with a get token the upload is
// read back through a fuzzed Range.
func FuzzDataPlane(f *testing.F) {
	dir := f.TempDir()
	n, err := New(Config{StateDir: dir, Log: slog.Default(), Limits: DefaultLimits})
	if err != nil {
		f.Fatal(err)
	}
	s, err := OpenSink(SinkConfig{Path: filepath.Join(dir, "sink"), Capacity: 1 << 30, Device: "disk-fuzz"}, DefaultWatermarks)
	if err != nil {
		f.Fatal(err)
	}
	n.sinks = append(n.sinks, s)
	n.byId[s.Id] = s
	n.id = pdid.New(DomNode)
	k, err := token.Generate("k-fuzz")
	if err != nil {
		f.Fatal(err)
	}
	n.verifier.Add("k-fuzz", k.Public())
	dp := n.dp
	var seq atomic.Int64

	sign := func(key string, op api.TokenOp, aud []byte) string {
		lamina, attempt := pdid.New(pdid.Domain(9)), pdid.New(pdid.Domain(10))
		now := time.Now()
		tok, err := k.Sign(api.TokenClaims_builder{
			Exp: timestamppb.New(now.Add(time.Hour)), Iat: timestamppb.New(now),
			Aud: aud, Op: op, SinkId: s.Id.Bytes(), LaminaKey: key, AttemptId: attempt.Bytes(),
			Record: api.LaminaRecord_builder{
				FormatVersion: FormatVersion, LaminaId: lamina.Bytes(), AttemptId: attempt.Bytes(),
				DateStartedMs: now.UnixMilli(), SizeHint: 4096,
			}.Build(),
			MaxLength: 1 << 20, Mode: api.UploadMode_UPLOAD_MODE_LIVE,
			IdleTimeoutSeconds: 5, AbandonTimeoutSeconds: 10,
		}.Build())
		if err != nil {
			f.Fatal(err)
		}

		return tok
	}
	known := map[int]bool{200: true, 201: true, 204: true, 206: true, 400: true, 401: true, 403: true, 404: true, 405: true, 409: true, 413: true, 416: true, 429: true, 503: true}

	f.Add("PUT", "", 0, "0", "?1", "", "", "2026-09-21T05:38:42Z", "2026-09-21T05:39:42Z", "", []byte("hello"))
	f.Add("PUT", "", 0, "0", "?0", "1024", "2048", "", "", "", []byte("part"))
	f.Add("GET", "", 1, "", "", "", "", "", "", "bytes=1-3", []byte("read me back"))
	f.Add("GET", "", 1, "", "", "", "", "", "", "bytes=-2", []byte("tail"))
	f.Add("HEAD", "", 0, "", "", "", "", "", "", "", []byte{})
	f.Add("PUT", "/../etc", 0, "0", "?1", "", "", "", "", "", []byte("x"))
	f.Add("PUT", "", 2, "0", "?1", "", "", "", "", "", []byte("other node's token"))
	f.Add("DELETE", "", 0, "", "", "", "", "", "", "", []byte{})
	f.Add("PUT", "", 0, "-5", "?1", "x", "y", "not a date", "", "", []byte{})
	f.Fuzz(func(t *testing.T, method, suffix string, which int, offset, complete, length, hint, started, ended, rng string, body []byte) {
		i := seq.Add(1)
		key := fmt.Sprintf("laminae/2026/09/21/09/%s.%d", pdid.New(pdid.Domain(9)).String(), i)
		aud := n.id.Bytes()
		op := api.TokenOp_TOKEN_OP_PUT
		switch which % 3 {
		case 1:
			op = api.TokenOp_TOKEN_OP_GET
		case 2:
			aud = pdid.New(DomNode).Bytes()
		}
		tok := sign(key, op, aud)
		do := func(method, path string, hdr map[string]string, body []byte) int {
			req, err := http.NewRequest(method, "http://node"+path, bytes.NewReader(body))
			if err != nil {
				return 0
			}
			req.Header.Set("Authorization", token.Scheme+" "+tok)
			req.ContentLength = int64(len(body))
			for k, v := range hdr {
				if v != "" {
					req.Header.Set(k, v)
				}
			}
			rec := httptest.NewRecorder()
			dp.ServeHTTP(rec, req)
			if !known[rec.Code] {
				t.Fatalf("%s %s: an answer nobody defined: %d %s", method, path, rec.Code, rec.Body.String())
			}

			return rec.Code
		}
		path := "/" + key + suffix
		if which%3 == 1 {
			// A complete upload first, with a put token, then the read.
			put := sign(key, api.TokenOp_TOKEN_OP_PUT, n.id.Bytes())
			tok, put = put, tok
			do(http.MethodPut, "/"+key, map[string]string{HdrUploadOffset: "0", HdrUploadComplete: "?1"}, body)
			tok = put
			do(method, path, map[string]string{"Range": rng}, nil)

			return
		}
		do(method, path, map[string]string{
			HdrUploadOffset: offset, HdrUploadComplete: complete, HdrUploadLength: length, HdrSizeHint: hint,
			HdrDateStarted: started, HdrDateEnded: ended, "Range": rng,
		}, body)
	})
}
