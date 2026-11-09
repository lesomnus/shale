package onvif

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
)

var clockMaterial = camera.CredentialMaterial{Username: "fixture-user", Password: "fixture-password"}

type deviceClockTransport func(*http.Request) (*http.Response, error)

func (f deviceClockTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDeviceTimeSeparatesCaseSensitiveIPv6ZoneAuthorities(t *testing.T) {
	var probes int
	c := &Client{HTTPClient: &http.Client{Transport: deviceClockTransport(func(r *http.Request) (*http.Response, error) {
		probes++
		year := 2024
		if strings.Contains(r.URL.Host, "Eth0") {
			year = 2025
		}
		body := wrapResponse("GetSystemDateAndTime", deviceNS, clockUTC(time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)))
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}}
	for i, authority := range []string{"[fe80::1%25Eth0]", "[fe80::1%25eth0]"} {
		at, err := c.authenticationTime(t.Context(), "http://"+authority+"/device")
		if err != nil || at.Year() != 2025-i {
			t.Fatal("distinct zone authorities borrowed each other's device clock")
		}
	}
	if probes != 2 {
		t.Fatal("case-sensitive interface zones shared a cache entry")
	}
}

func clockUTC(at time.Time) string {
	return fmt.Sprintf(`<tt:SystemDateAndTime><tt:UTCDateTime><tt:Date><tt:Year>%d</tt:Year><tt:Month>%d</tt:Month><tt:Day>%d</tt:Day></tt:Date><tt:Time><tt:Hour>%d</tt:Hour><tt:Minute>%d</tt:Minute><tt:Second>%d</tt:Second></tt:Time></tt:UTCDateTime></tt:SystemDateAndTime>`, at.Year(), at.Month(), at.Day(), at.Hour(), at.Minute(), at.Second())
}

func TestDeviceUTCParserRejectsInvalidCalendarInsteadOfNormalizing(t *testing.T) {
	at := time.Date(2025, 10, 21, 1, 13, 0, 0, time.UTC)
	valid := clockUTC(at)
	for name, body := range map[string]string{
		"missing_UTC":    strings.ReplaceAll(valid, "UTCDateTime", "LocalDateTime"),
		"old_year":       strings.Replace(valid, "2025", "1969", 1),
		"large_year":     strings.Replace(valid, "2025", "10000", 1),
		"signed_year":    strings.Replace(valid, "2025", "+2025", 1),
		"invalid_month":  strings.Replace(valid, "<tt:Month>10", "<tt:Month>13", 1),
		"invalid_day":    strings.Replace(valid, "<tt:Day>21", "<tt:Day>32", 1),
		"non_leap_Feb29": strings.Replace(strings.Replace(valid, "<tt:Month>10", "<tt:Month>2", 1), "<tt:Day>21", "<tt:Day>29", 1),
		"hour24":         strings.Replace(valid, "<tt:Hour>1", "<tt:Hour>24", 1),
		"minute60":       strings.Replace(valid, "<tt:Minute>13", "<tt:Minute>60", 1),
		"second60":       strings.Replace(valid, "<tt:Second>0", "<tt:Second>60", 1),
		"missing_second": strings.Replace(valid, "<tt:Second>0</tt:Second>", "", 1),
		"duplicate_year": strings.Replace(valid, "</tt:Year>", "</tt:Year><tt:Year>2025</tt:Year>", 1),
		"duplicate_UTC":  strings.Replace(valid, "</tt:SystemDateAndTime>", "<tt:UTCDateTime/></tt:SystemDateAndTime>", 1),
	} {
		t.Run(name, func(t *testing.T) {
			n, err := parseXML([]byte(wrapResponse("GetSystemDateAndTime", deviceNS, body)))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = parseDeviceUTC(n.find("GetSystemDateAndTimeResponse")); !errors.Is(err, ErrDeviceTime) {
				t.Fatal("invalid device clock accepted")
			}
		})
	}
	for _, at := range []time.Time{at, time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)} {
		n, err := parseXML([]byte(wrapResponse("GetSystemDateAndTime", deviceNS, clockUTC(at))))
		if err != nil {
			t.Fatal(err)
		}
		got, err := parseDeviceUTC(n.find("GetSystemDateAndTimeResponse"))
		if err != nil || !got.Equal(at) || got.Location() != time.UTC {
			t.Fatalf("valid UTC not preserved: %v", err)
		}
	}
}

type authClockFixture struct {
	server  *httptest.Server
	utc     time.Time
	mu      sync.Mutex
	probes  int
	created []time.Time
}

func newAuthClockFixture(t *testing.T, utc time.Time) *authClockFixture {
	t.Helper()
	f := &authClockFixture{utc: utc}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		n, err := parseXML(data)
		if err != nil || n.find("Body") == nil || len(n.find("Body").Children) != 1 {
			t.Error("invalid fixture request")
			w.WriteHeader(400)
			return
		}
		op := n.find("Body").Children[0].Name.Local
		f.mu.Lock()
		defer f.mu.Unlock()
		if op == "GetSystemDateAndTime" {
			f.probes++
			assertPublicClockRequest(t, r, n, data)
			_, _ = io.WriteString(w, wrapResponse(op, deviceNS, clockUTC(f.utc)))
			return
		}
		if op != "GetDeviceInformation" && op != "GetProfiles" {
			t.Error("unexpected or mutating clock fixture operation")
			w.WriteHeader(400)
			return
		}
		token := n.find("UsernameToken")
		if token == nil {
			t.Error("missing WS-Security")
			w.WriteHeader(400)
			return
		}
		created, err := time.Parse(time.RFC3339Nano, token.text("Created"))
		if err != nil || created.Sub(f.utc).Abs() > 3*time.Second {
			t.Error("WS-Security did not use camera UTC")
			w.WriteHeader(401)
			return
		}
		nonce, err := base64.StdEncoding.DecodeString(token.text("Nonce"))
		digest := sha1.New()
		digest.Write(nonce)
		digest.Write([]byte(token.text("Created")))
		digest.Write([]byte(clockMaterial.Password))
		if err != nil || token.text("Username") != clockMaterial.Username || token.text("Password") != base64.StdEncoding.EncodeToString(digest.Sum(nil)) {
			t.Error("invalid camera-time password digest")
			w.WriteHeader(401)
			return
		}
		f.created = append(f.created, created)
		_, _ = io.WriteString(w, wrapResponse(op, deviceNS, ""))
	}))
	t.Cleanup(f.server.Close)
	return f
}

func assertPublicClockRequest(t *testing.T, r *http.Request, n *node, data []byte) {
	t.Helper()
	if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || n.find("Security") != nil || n.find("UsernameToken") != nil || strings.Contains(string(data), clockMaterial.Username) || strings.Contains(string(data), clockMaterial.Password) {
		t.Error("public clock probe contained authentication")
	}
}

func TestDeviceTimeAuthenticationUsesPublicUTCAndCachesMediaAuthority(t *testing.T) {
	f := newAuthClockFixture(t, time.Date(2025, 10, 21, 1, 13, 0, 0, time.UTC))
	jar, _ := cookiejar.New(nil)
	u, _ := url.Parse(f.server.URL)
	jar.SetCookies(u, []*http.Cookie{{Name: "session", Value: "fixture-cookie"}})
	c := &Client{HTTPClient: &http.Client{Jar: jar}, UseDeviceTimeForAuthentication: true}
	for _, call := range []struct{ path, ns, op string }{{"/device", deviceNS, "GetDeviceInformation"}, {"/media", media2NS, "GetProfiles"}} {
		if _, err := c.request(t.Context(), f.server.URL+call.path, call.ns, call.op, "", clockMaterial); err != nil {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.probes != 1 || len(f.created) != 2 || !f.created[1].After(f.created[0]) {
		t.Fatal("camera sample was not cached with advancing host elapsed time")
	}
}

func TestDeviceTimeCachesSeparateAuthoritiesConcurrently(t *testing.T) {
	a := newAuthClockFixture(t, time.Date(2025, 10, 21, 1, 13, 0, 0, time.UTC))
	b := newAuthClockFixture(t, time.Date(2024, 8, 10, 10, 0, 0, 0, time.UTC))
	c := &Client{UseDeviceTimeForAuthentication: true}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := range 16 {
		wg.Go(func() {
			target := a
			if i%2 == 1 {
				target = b
			}
			_, err := c.request(t.Context(), target.server.URL+"/device", deviceNS, "GetDeviceInformation", "", clockMaterial)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []*authClockFixture{a, b} {
		f.mu.Lock()
		probes, requests := f.probes, len(f.created)
		f.mu.Unlock()
		if probes != 1 || requests != 8 {
			t.Fatal("per-authority camera clock was mixed or redundantly read")
		}
	}
}

func TestInvalidPublicTimeStopsCredentialsAndCanRetry(t *testing.T) {
	var probes, credentials atomic.Int32
	var valid atomic.Bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		n, _ := parseXML(data)
		if n.find("GetSystemDateAndTime") != nil {
			probes.Add(1)
			assertPublicClockRequest(t, r, n, data)
			if valid.Load() {
				_, _ = io.WriteString(w, wrapResponse("GetSystemDateAndTime", deviceNS, clockUTC(time.Now().UTC())))
			} else {
				_, _ = io.WriteString(w, wrapResponse("GetSystemDateAndTime", deviceNS, `<tt:SystemDateAndTime><tt:LocalDateTime/></tt:SystemDateAndTime>`))
			}
			return
		}
		credentials.Add(1)
		_, _ = io.WriteString(w, wrapResponse("GetDeviceInformation", deviceNS, ""))
	}))
	defer s.Close()
	c := &Client{UseDeviceTimeForAuthentication: true}
	if _, err := c.request(t.Context(), s.URL, deviceNS, "GetDeviceInformation", "", clockMaterial); !errors.Is(err, ErrDeviceTime) || credentials.Load() != 0 {
		t.Fatal("malformed UTC allowed an authenticated request")
	}
	valid.Store(true)
	if _, err := c.request(t.Context(), s.URL, deviceNS, "GetDeviceInformation", "", clockMaterial); err != nil || probes.Load() != 2 || credentials.Load() != 1 {
		t.Fatal("failed public time was cached or never retried")
	}
}

func TestFailedClockProbeNeverFallsBackToHostTimeOrAuthentication(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusInternalServerError} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var probes, authenticated atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, _ := io.ReadAll(r.Body)
				n, _ := parseXML(data)
				if n.find("GetSystemDateAndTime") == nil {
					authenticated.Add(1)
				}
				probes.Add(1)
				assertPublicClockRequest(t, r, n, data)
				w.Header().Set("WWW-Authenticate", `Digest realm="fixture", nonce="fixture"`)
				w.WriteHeader(code)
				_, _ = io.WriteString(w, clockMaterial.Password)
			}))
			defer s.Close()
			c := &Client{UseDeviceTimeForAuthentication: true}
			_, err := c.request(t.Context(), s.URL, deviceNS, "GetDeviceInformation", "", clockMaterial)
			if err != ErrDeviceTime || authenticated.Load() != 0 || probes.Load() != 1 {
				t.Fatal("failed public clock allowed a credential request or retry")
			}
		})
	}
}

func TestOptInAnonymousRequestDoesNotReadClockRecursively(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		n, _ := parseXML(data)
		assertPublicClockRequest(t, r, n, data)
		if n.find("GetDeviceInformation") == nil {
			t.Error("anonymous request gained a clock dependency")
			w.WriteHeader(400)
			return
		}
		calls.Add(1)
		_, _ = io.WriteString(w, wrapResponse("GetDeviceInformation", deviceNS, ""))
	}))
	defer s.Close()
	c := &Client{UseDeviceTimeForAuthentication: true}
	if _, err := c.request(t.Context(), s.URL, deviceNS, "GetDeviceInformation", "", camera.CredentialMaterial{}); err != nil || calls.Load() != 1 {
		t.Fatal("anonymous operation changed")
	}
}

func TestDeviceTimeWaitingForAnotherProbeHonorsCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var credentials atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		n, _ := parseXML(data)
		if n.find("GetSystemDateAndTime") == nil {
			credentials.Add(1)
			return
		}
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer s.Close()
	defer close(release)
	c := &Client{UseDeviceTimeForAuthentication: true}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	owner := make(chan error, 1)
	go func() {
		_, err := c.request(ctx, s.URL, deviceNS, "GetDeviceInformation", "", clockMaterial)
		owner <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("clock probe did not start")
	}
	waitCtx, done := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer done()
	_, err := c.request(waitCtx, s.URL, deviceNS, "GetDeviceInformation", "", clockMaterial)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiting clock consumer ignored its deadline")
	}
	cancel()
	select {
	case err := <-owner:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("owner cancellation lost")
		}
	case <-time.After(time.Second):
		t.Fatal("clock owner leaked")
	}
	if credentials.Load() != 0 {
		t.Fatal("canceled clock read sent credentials")
	}
}

func TestDefaultClientNeverReadsOrChangesDeviceTime(t *testing.T) {
	var probes, calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		n, _ := parseXML(data)
		if n.find("GetSystemDateAndTime") != nil || n.find("SetSystemDateAndTime") != nil {
			probes.Add(1)
			w.WriteHeader(400)
			return
		}
		calls.Add(1)
		created, err := time.Parse(time.RFC3339Nano, n.find("UsernameToken").text("Created"))
		if err != nil || time.Since(created).Abs() > 3*time.Second {
			t.Error("default authentication changed its host clock")
		}
		_, _ = io.WriteString(w, wrapResponse("GetDeviceInformation", deviceNS, ""))
	}))
	defer s.Close()
	if _, err := new(Client).request(t.Context(), s.URL, deviceNS, "GetDeviceInformation", "", clockMaterial); err != nil || probes.Load() != 0 || calls.Load() != 1 {
		t.Fatal("default client gained a device-time dependency")
	}
}
