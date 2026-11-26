package core

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/identity"
)

// accountApp stands in for roster's account app, its four routes of a
// terminal's sign-in (roster's `account/device.go` at 5436801f), each
// answering what roster answers -- read there, and said beside each -- so
// that Shale's side of the road is held to roster's own answers without a
// roster. `yes` and `no` are the person at the front door: what
// `/device/approve` does to the row a poll then reads.
type accountApp struct {
	mu       sync.Mutex
	off      bool                // account.terminal is off
	codes    map[string]*waiting // by device code
	began    []map[string]json.RawMessage
	polled   int
	lapseNow bool
}

type waiting struct {
	user   string
	token  string
	denied bool
}

func (a *accountApp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.off {
		// `App.offered`, device.go:381-389: every route of the four, with
		// the switch off.
		http.Error(w, "this deployment does not sign terminals in", http.StatusNotImplemented)
		return
	}
	switch r.URL.Path {
	case "/device/begin":
		var in map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			// device.go:458.
			http.Error(w, "a name and what to allow", http.StatusBadRequest)
			return
		}
		for k := range in {
			switch k {
			case "name", "app", "allow", "scope", "date_expired", "delegate":
			default:
				// device.go:453-456, `strictly`: a field it does not know.
				http.Error(w, fmt.Sprintf("%s: this request has no such field", k), http.StatusBadRequest)
				return
			}
		}
		if string(in["date_expired"]) != "" {
			var v string
			_ = json.Unmarshal(in["date_expired"], &v)
			if t, err := time.Parse(time.RFC3339, v); err == nil && !t.After(time.Now()) {
				// device.go:512-514.
				http.Error(w, "date_expired: a key cannot have already expired", http.StatusBadRequest)
				return
			}
		}
		a.began = append(a.began, in)
		code := fmt.Sprintf("device-%d", len(a.began))
		user := fmt.Sprintf("BCDF-%04d", len(a.began))
		a.codes[code] = &waiting{user: user}
		// device.go:575-591: RFC 8628 §3.2's answer, the address being the
		// account app's own `/device`.
		at := "http://" + r.Host + "/device"
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code":               code,
			"user_code":                 user,
			"verification_uri":          at,
			"verification_uri_complete": at + "?user_code=" + user,
			"expires_in":                900,
			"interval":                  5,
		})
	case "/device/poll":
		a.polled++
		var in struct {
			DeviceCode string `json:"device_code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.DeviceCode == "" {
			// device.go:620-623.
			http.Error(w, "device_code", http.StatusBadRequest)
			return
		}
		v, ok := a.codes[in.DeviceCode]
		switch {
		case !ok || a.lapseNow:
			// `devices.polled`, device.go:276-279: no such row, or past its
			// fifteen minutes; and so is a code whose key was carried away.
			deviceErr(w, "expired_token")
		case v.denied:
			// device.go:286-288: the row goes with the answer.
			delete(a.codes, in.DeviceCode)
			deviceErr(w, "access_denied")
		case v.token == "":
			// device.go:289-294.
			deviceErr(w, "authorization_pending")
		default:
			// device.go:297-302 and 631-633: once, and the row goes.
			delete(a.codes, in.DeviceCode)
			w.Header().Set("content-type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"token": v.token})
		}
	default:
		http.NotFound(w, r)
	}
}

// deviceErr is roster's own, device.go:636-641.
func deviceErr(w http.ResponseWriter, why string) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": why})
}

// answer is the person at the front door saying yes or no to the code
// the page was shown: `/device/approve`, device.go:737-879, which mints as
// them on a yes (`settle` with the token) and settles the row denied on a
// no (device.go:791-796).
func (a *accountApp) answer(user string, yes bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, v := range a.codes {
		if v.user == user {
			if yes {
				v.token = "rt_minted-for-" + user
			} else {
				v.denied = true
			}
		}
	}
}

// Making a key where roster is elsewhere (§33.1) is a terminal's sign-in
// at roster's account app, held to its answers: begin names the app, the
// key's name, methods, end and scope; the caller is answered where to say
// yes and a handle, never the device code; a poll answers not yet, the
// token once on a yes, refused on a no, lapsed for a code past its time;
// a handle presented by another session, or another person, is refused;
// roster's words for what it refuses at begin are the page's; the switch
// off, and no account app named, are each a sentence saying what to do.
// Listing and ending need no account app.
func TestOwnKeyWhereRosterIsElsewhere(t *testing.T) {
	ctx := context.Background()
	app := &accountApp{codes: map[string]*waiting{}}
	srv := httptest.NewServer(app)
	defer srv.Close()

	open := func(account string) *identity.Store {
		t.Helper()
		s, err := identity.Open(ctx, identity.Config{Addr: "127.0.0.1:1", Insecure: true, Account: account}, t.TempDir(), nil)
		require.NoError(t, err)
		t.Cleanup(func() { s.Close() })
		return s
	}
	kek := Kek(make([]byte, 32))
	for i := range kek {
		kek[i] = byte(i)
	}
	h := coreHolder{Core: Core{d: &Deps{Identity: open(srv.URL), Kek: func() Kek { return kek }}}}

	tenant, bob, carol := pdid.New(DomTenant), pdid.New(DomHolder), pdid.New(DomHolder)
	as := func(who pdid.Id, credential string) context.Context {
		f := frame.New(who, tenant, frame.Grant{})
		v := *f
		v.Authentication = frame.Authentication{Method: "session"}
		return WithCredential(frame.Into(ctx, &v), sha256.Sum256([]byte(credential)))
	}
	bobs := as(bob, "bob's session")
	ends := time.Now().Add(30 * 24 * time.Hour).Truncate(time.Second)
	begin := func(ctx context.Context) (*api.HolderBeginKeyResponse, error) {
		return h.BeginKey(ctx, api.HolderBeginKeyRequest_builder{
			Name: "ci", Methods: []string{"/shale.SetService/List", "/shale.SetService/Get"},
			DateExpired: timestamppb.New(ends), Scope: "cams:1",
		}.Build())
	}
	poll := func(ctx context.Context, handle string) (*api.HolderPollKeyResponse, error) {
		return h.PollKey(ctx, api.HolderPollKeyRequest_builder{Handle: handle}.Build())
	}

	// Begun: what roster's begin was told, and what the caller is.
	b, err := begin(bobs)
	require.NoError(t, err)
	require.Len(t, app.began, 1)
	said := app.began[0]
	require.JSONEq(t, `"ci"`, string(said["name"]))
	require.JSONEq(t, `"shale"`, string(said["app"]), "shale's name at roster")
	require.JSONEq(t, `["/shale.SetService/List","/shale.SetService/Get"]`, string(said["allow"]))
	require.JSONEq(t, `"`+ends.UTC().Format(time.RFC3339)+`"`, string(said["date_expired"]))
	require.JSONEq(t, `"cams:1"`, string(said["scope"]))
	require.Equal(t, srv.URL+"/device?user_code=BCDF-0001", b.GetApproveAt())
	require.Equal(t, "BCDF-0001", b.GetUserCode())
	require.Equal(t, 5*time.Second, b.GetInterval().AsDuration())
	require.WithinDuration(t, time.Now().Add(15*time.Minute), b.GetDateLapses().AsTime(), 5*time.Second)
	require.Empty(t, b.GetToken())
	require.NotContains(t, b.GetHandle(), "device-1", "the device code is never in the clear")

	// Not yet; then refused to another session of the same person, and to
	// somebody else; then, on the yes, the token once.
	p, err := poll(bobs, b.GetHandle())
	require.NoError(t, err)
	require.Equal(t, api.HolderKeyState_HOLDER_KEY_STATE_PENDING, p.GetState())
	require.Empty(t, p.GetToken())
	n := app.polled
	_, err = poll(as(bob, "bob's other session"), b.GetHandle())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	_, err = poll(as(carol, "bob's session"), b.GetHandle())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Equal(t, n, app.polled, "roster is not asked for them")
	_, err = poll(bobs, b.GetHandle()+"x")
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a handle nobody sealed: %v", err)
	app.answer("BCDF-0001", true)
	p, err = poll(bobs, b.GetHandle())
	require.NoError(t, err)
	require.Equal(t, api.HolderKeyState_HOLDER_KEY_STATE_MADE, p.GetState())
	require.Equal(t, "rt_minted-for-BCDF-0001", p.GetToken())
	p, err = poll(bobs, b.GetHandle())
	require.NoError(t, err)
	require.Equal(t, api.HolderKeyState_HOLDER_KEY_STATE_LAPSED, p.GetState(), "carried away once")
	require.Empty(t, p.GetToken())

	// The no.
	b, err = begin(bobs)
	require.NoError(t, err)
	app.answer(b.GetUserCode(), false)
	p, err = poll(bobs, b.GetHandle())
	require.NoError(t, err)
	require.Equal(t, api.HolderKeyState_HOLDER_KEY_STATE_REFUSED, p.GetState())

	// A code past its time.
	b, err = begin(bobs)
	require.NoError(t, err)
	app.lapseNow = true
	p, err = poll(bobs, b.GetHandle())
	require.NoError(t, err)
	require.Equal(t, api.HolderKeyState_HOLDER_KEY_STATE_LAPSED, p.GetState())
	app.lapseNow = false

	// A key begins one too: its yes is the person's, at roster's front
	// door, and the handle is bound to the key.
	byKey := func(token string) context.Context {
		f := frame.New(bob, tenant, frame.Grant{})
		v := *f
		v.Authentication = frame.Authentication{Method: auth.MethodBearer}
		return WithCredential(frame.Into(ctx, &v), sha256.Sum256([]byte("key\x00"+token)))
	}
	b, err = begin(byKey("rt_one"))
	require.NoError(t, err)
	_, err = poll(byKey("rt_two"), b.GetHandle())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = poll(byKey("rt_one"), b.GetHandle())
	require.NoError(t, err)

	// What roster refuses at begin is said in its words; what Shale knows
	// roster would refuse is refused before it is asked.
	began := len(app.began)
	_, err = h.BeginKey(bobs, api.HolderBeginKeyRequest_builder{Name: "Not A Name", Methods: []string{"/shale.SetService/List"}}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = h.BeginKey(bobs, api.HolderBeginKeyRequest_builder{Name: "x", Methods: []string{"/roster.MeService/Get"}}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = h.BeginKey(bobs, api.HolderBeginKeyRequest_builder{Name: "x", Methods: []string{"/shale.SetService/List"}, Scope: strings.Repeat("s", 257)}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "at most 256 bytes, and this is 257")
	require.Equal(t, began, len(app.began), "none of them asked roster")

	// The switch off at roster: the same status as no account app, and a
	// sentence that names the switch.
	app.off = true
	_, err = begin(bobs)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "account.terminal")
	app.off = false

	// No account app named: a sentence saying so, and where a key can still
	// be made.
	none := coreHolder{Core: Core{d: &Deps{Identity: open(""), Kek: func() Kek { return kek }}}}
	_, err = none.BeginKey(bobs, api.HolderBeginKeyRequest_builder{Name: "ci", Methods: []string{"/shale.SetService/List"}}.Build())
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "auth.roster.account")
	require.Contains(t, status.Convert(err).Message(), "account page")
}

// What roster's account app refuses at begin is answered in its words,
// with the status that says whose it is to mend.
func TestOwnKeyRefusalsAreRostersWords(t *testing.T) {
	for _, tc := range []struct {
		code  int
		words string
		want  codes.Code
	}{
		{http.StatusBadRequest, "scope: at most 256 bytes, and this is 300", codes.InvalidArgument},
		{http.StatusBadRequest, "date_expired: a key cannot have already expired", codes.InvalidArgument},
		{http.StatusNotFound, "no operator here serves this name", codes.FailedPrecondition},
		{http.StatusServiceUnavailable, "too many terminals are waiting to be approved here; try again in a minute", codes.Unavailable},
		{http.StatusInternalServerError, "cannot begin", codes.Unavailable},
	} {
		err := keyErr(&identity.AccountRefusal{Status: tc.code, Words: tc.words})
		require.Equal(t, tc.want, status.Code(err), "%d %s", tc.code, tc.words)
		require.Contains(t, status.Convert(err).Message(), tc.words)
	}
	require.Equal(t, codes.FailedPrecondition, status.Code(keyErr(identity.ErrNoTerminal)))
	require.Equal(t, codes.NotFound, status.Code(keyErr(identity.ErrNoKeyHere)))
	require.Equal(t, codes.FailedPrecondition, status.Code(keyErr(fmt.Errorf("%w (the keys: no)", identity.ErrKeysUnanswered))))
	require.Equal(t, codes.PermissionDenied, status.Code(keyErr(status.Error(codes.PermissionDenied, `methods: you do not hold "/shale.SetService/Add"`))))
}

// What a person holds is said as the tenant API's methods it covers, one
// pattern at a time: what roster lets a key of theirs be given. Nothing
// of the cluster API's, nothing nobody may call, nothing of an app's.
func TestHeldMethodsAreTheTenantApis(t *testing.T) {
	all := heldMethods([]string{"/shale.*/*"})
	require.Contains(t, all, api.SetService_List_FullMethodName)
	require.Contains(t, all, api.HolderService_ListKeys_FullMethodName)
	require.NotContains(t, all, api.NodeService_List_FullMethodName, "the cluster API's")
	require.NotContains(t, all, api.LaminaService_Add_FullMethodName, "nobody's")
	require.NotContains(t, all, api.DelegationService_Live_FullMethodName, "an app's")
	require.Equal(t, []string{api.SetService_Get_FullMethodName, api.SetService_List_FullMethodName},
		heldMethods([]string{api.SetService_List_FullMethodName, "/shale.SetService/Get", "/roster.MeService/Get"}))
	require.Empty(t, heldMethods(nil))
	for m := range personMay {
		require.Contains(t, all, m, "a read every person makes is one a key may name")
	}
}
