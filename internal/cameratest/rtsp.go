// Package cameratest provides bounded synthetic camera fixtures for conformance tests.
package cameratest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func ServeRTSP(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "synthetic.h264")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "color=c=black:s=160x90:r=10", "-t", "2", "-an", "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency", "-g", "10", "-bf", "0", "-x264-params", "repeat-headers=1:aud=1", "-f", "h264", "-y", path)
	if out, e := command.CombinedOutput(); e != nil {
		t.Fatalf("synthetic encoder: %v %s", e, out)
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	units, sps, pps := accessUnits(raw)
	if len(units) < 10 || len(sps) == 0 || len(pps) == 0 {
		t.Fatal("invalid synthetic access units")
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var mu sync.Mutex
	connections := map[net.Conn]bool{}
	var workers sync.WaitGroup
	done := make(chan struct{})
	t.Cleanup(func() {
		close(done)
		listener.Close()
		mu.Lock()
		for c := range connections {
			c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			connections[c] = true
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer c.Close()
				defer func() { mu.Lock(); delete(connections, c); mu.Unlock() }()
				serveRTSPConnection(c, listener.Addr().String(), units, sps, pps, done)
			}()
		}
	}()
	return "rtsp://" + listener.Addr().String() + "/camera"
}
func accessUnits(raw []byte) (units [][][]byte, sps, pps []byte) {
	// Normalize both Annex B start-code lengths, then split at access-unit
	// delimiters inserted by the deterministic encoder above.
	raw = bytes.ReplaceAll(raw, []byte{0, 0, 0, 1}, []byte{0, 0, 1})
	var current [][]byte
	for _, n := range bytes.Split(raw, []byte{0, 0, 1}) {
		if len(n) == 0 {
			continue
		}
		kind := n[0] & 31
		if kind == 9 && len(current) > 0 {
			units = append(units, current)
			current = nil
		}
		current = append(current, n)
		if kind == 7 {
			sps = n
		}
		if kind == 8 {
			pps = n
		}
	}
	if len(current) > 0 {
		units = append(units, current)
	}
	return
}
func serveRTSPConnection(conn net.Conn, host string, units [][][]byte, sps, pps []byte, done <-chan struct{}) {
	reader := bufio.NewReader(conn)
	var write sync.Mutex
	started := false
	var streamWorkers sync.WaitGroup
	defer streamWorkers.Wait()
	streamDone := make(chan struct{})
	defer close(streamDone)
	response := func(cseq string, extra string, body string) error {
		write.Lock()
		defer write.Unlock()
		_, e := fmt.Fprintf(conn, "RTSP/1.0 200 OK\r\nCSeq: %s\r\n%sContent-Length: %d\r\n\r\n%s", cseq, extra, len(body), body)
		return e
	}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(time.Minute))
		line, e := reader.ReadString('\n')
		if e != nil {
			return
		}
		if len(line) == 0 {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 3 {
			return
		}
		header, e := textproto.NewReader(reader).ReadMIMEHeader()
		if e != nil {
			return
		}
		if n, _ := strconv.Atoi(header.Get("Content-Length")); n > 0 {
			if _, e = io.CopyN(io.Discard, reader, int64(n)); e != nil {
				return
			}
		}
		cseq := header.Get("Cseq")
		switch parts[0] {
		case "OPTIONS":
			e = response(cseq, "Public: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN\r\n", "")
		case "DESCRIBE":
			body := fmt.Sprintf("v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=synthetic\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\na=control:*\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1;sprop-parameter-sets=%s,%s\r\na=control:trackID=0\r\n", base64.StdEncoding.EncodeToString(sps), base64.StdEncoding.EncodeToString(pps))
			e = response(cseq, "Content-Type: application/sdp\r\nContent-Base: rtsp://"+host+"/camera/\r\n", body)
		case "SETUP":
			e = response(cseq, "Session: fixture\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n", "")
		case "PLAY":
			e = response(cseq, "Session: fixture\r\nRange: npt=0.000-\r\n", "")
			if !started && e == nil {
				started = true
				streamWorkers.Add(1)
				go func() {
					defer streamWorkers.Done()
					ticker := time.NewTicker(100 * time.Millisecond)
					defer ticker.Stop()
					seq := uint16(0)
					stamp := uint32(0)
					for i := 0; ; i++ {
						select {
						case <-done:
							return
						case <-streamDone:
							return
						case <-ticker.C:
						}
						frame := units[i%len(units)]
						for j, nal := range frame {
							packet := make([]byte, 12+len(nal))
							packet[0] = 0x80
							packet[1] = 96
							if j == len(frame)-1 {
								packet[1] |= 0x80
							}
							binary.BigEndian.PutUint16(packet[2:4], seq)
							binary.BigEndian.PutUint32(packet[4:8], stamp)
							binary.BigEndian.PutUint32(packet[8:12], 123)
							copy(packet[12:], nal)
							seq++
							prefix := []byte{'$', 0, byte(len(packet) >> 8), byte(len(packet))}
							write.Lock()
							_, e := conn.Write(append(prefix, packet...))
							write.Unlock()
							if e != nil {
								return
							}
						}
						stamp += 9000
					}
				}()
			}
		case "TEARDOWN":
			response(cseq, "Session: fixture\r\n", "")
			return
		default:
			e = response(cseq, "Session: fixture\r\n", "")
		}
		if e != nil {
			return
		}
	}
}
