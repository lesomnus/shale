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
	"math"
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

func ServeRTSP(t testing.TB) string { return serveRTSP(t, "", "") }

// ServeRTSPAuth requires actual RTSP Basic authentication. Passwords may contain
// URI-reserved characters; clients must resolve and encode them correctly.
func ServeRTSPAuth(t testing.TB, username, password string) string {
	return serveRTSP(t, username, password)
}
func serveRTSP(t testing.TB, username, password string) string {
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
				serveRTSPConnection(c, listener.Addr().String(), units, sps, pps, done, username, password)
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
func serveRTSPConnection(conn net.Conn, host string, units [][][]byte, sps, pps []byte, done <-chan struct{}, username, password string) {
	serveRTSPMedia(conn, host, "/camera", &rtspMedia{units: units, sps: sps, pps: pps, fps: 10}, done, username, password)
}

func serveRTSPMedia(conn net.Conn, host, path string, media *rtspMedia, done <-chan struct{}, username, password string) {
	reader := bufio.NewReader(conn)
	var write sync.Mutex
	started := false
	var streamWorkers sync.WaitGroup
	defer streamWorkers.Wait()
	defer conn.Close()
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
		if username != "" && parts[0] != "OPTIONS" && header.Get("Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte(username+":"+password)) {
			write.Lock()
			_, err := fmt.Fprintf(conn, "RTSP/1.0 401 Unauthorized\r\nCSeq: %s\r\nWWW-Authenticate: Basic realm=\"synthetic-camera\"\r\nContent-Length: 0\r\n\r\n", cseq)
			write.Unlock()
			if err != nil {
				return
			}
			continue
		}
		switch parts[0] {
		case "OPTIONS":
			e = response(cseq, "Public: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN\r\n", "")
		case "DESCRIBE":
			ip, _, _ := net.SplitHostPort(host)
			body := fmt.Sprintf("v=0\r\no=- 1 1 IN IP4 %s\r\ns=synthetic\r\nc=IN IP4 %s\r\nt=0 0\r\na=control:*\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1;sprop-parameter-sets=%s,%s\r\na=control:trackID=0\r\n", ip, ip, base64.StdEncoding.EncodeToString(media.sps), base64.StdEncoding.EncodeToString(media.pps))
			e = response(cseq, "Content-Type: application/sdp\r\nContent-Base: rtsp://"+host+path+"/\r\n", body)
		case "SETUP":
			e = response(cseq, "Session: fixture\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n", "")
		case "PLAY":
			e = response(cseq, "Session: fixture\r\nRange: npt=0.000-\r\nRTP-Info: url=rtsp://"+host+path+"/trackID=0;seq=0;rtptime=0\r\n", "")
			if !started && e == nil {
				started = true
				streamWorkers.Add(1)
				go func() {
					defer streamWorkers.Done()
					ticker := time.NewTicker(time.Duration(float64(time.Second) / media.fps))
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
						frame := media.units[i%len(media.units)]
						for j, nal := range frame {
							parts := packetizeNAL(nal)
							for part, payload := range parts {
								packet := make([]byte, 12+len(payload))
								packet[0] = 0x80
								packet[1] = 96
								if j == len(frame)-1 && part == len(parts)-1 {
									packet[1] |= 0x80
								}
								binary.BigEndian.PutUint16(packet[2:4], seq)
								binary.BigEndian.PutUint32(packet[4:8], stamp)
								binary.BigEndian.PutUint32(packet[8:12], 123)
								copy(packet[12:], payload)
								seq++
								prefix := []byte{'$', 0, byte(len(packet) >> 8), byte(len(packet))}
								write.Lock()
								_, e := conn.Write(append(prefix, packet...))
								write.Unlock()
								if e != nil {
									return
								}
							}
						}
						stamp = uint32(math.Round(float64(i+1) * 90000 / media.fps))
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

// Larger dynamic modes need legal FU-A packets rather than overflowing RTSP's
// two-byte interleaved frame length. Marker belongs to the final NAL fragment.
func packetizeNAL(nal []byte) [][]byte {
	const limit = 1200
	if len(nal) <= limit {
		return [][]byte{nal}
	}
	var out [][]byte
	header := nal[0]
	for offset := 1; offset < len(nal); {
		end := min(offset+limit-2, len(nal))
		p := make([]byte, 2+end-offset)
		p[0] = (header & 0xe0) | 28
		p[1] = header & 31
		if offset == 1 {
			p[1] |= 0x80
		}
		if end == len(nal) {
			p[1] |= 0x40
		}
		copy(p[2:], nal[offset:end])
		out = append(out, p)
		offset = end
	}
	return out
}
