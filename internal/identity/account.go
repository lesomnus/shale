package identity

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
)

// A person's keys for Shale, as Shale makes, lists and ends them for the
// person (§33.1, *A person's key for Shale*; roster's `docs/apps.md`,
// *Giving an app's users API tokens*).
//
// Making one is the person's own at roster, since a key acts as them. Where
// roster is elsewhere it is a terminal's sign-in for one key at roster's
// account app, the front door, which asks the person there and mints it as
// them when they say yes ([Store.BeginTerminal], [Store.PollTerminal]).
// Where roster runs in this process there is no other front door, so the
// key is minted at once as the person ([Store.IssueKeyAs]). Listing and
// ending are Shale's own at roster, as the app the keys are for: roster's
// `app-administrator`, which `app install` binds the app for itself, names
// `ApiKeyService`'s `Get`, `List` and `Erase`, and answers it the keys that
// are for it and no other.

var (
	// ErrNoAccount is a deployment that named no account app
	// (`auth.roster.account`), where roster is elsewhere.
	ErrNoAccount = errors.New("this deployment does not name the identity service's account app (auth.roster.account), " +
		"so a key cannot be made here; one can still be made on the identity service's account page, for the app shale")
	// ErrNoTerminal is an account app whose deployment did not turn the
	// terminal's sign-in on (`account.terminal`): `501` at begin and poll.
	ErrNoTerminal = errors.New("the identity service does not sign terminals in, which is how a key is made from here: " +
		"its operator turns account.terminal on in roster's file; a key can still be made on the identity service's account page, for the app shale")
	// ErrKeysUnanswered is roster refusing Shale the keys that are for it:
	// its holder there is not bound `app-administrator` for itself, or one
	// that names fewer verbs than reach a key.
	ErrKeysUnanswered = errors.New("roster does not answer shale the keys that are for it: `roster app install --serves` binds shale's holder app-administrator for itself, " +
		"which names ApiKeyService's Get, List and Erase; a tenant installed before those were among them has its app-administrator widened to them")
	// ErrNoKeyHere is a key that is not the person's, not for Shale, or
	// gone: one answer for each.
	ErrNoKeyHere = errors.New("no such key of yours for shale")
)

// AccountRefusal is what the account app refused, in its words.
type AccountRefusal struct {
	Status int
	Words  string
}

func (e *AccountRefusal) Error() string {
	return fmt.Sprintf("the identity service refused it (%d): %s", e.Status, e.Words)
}

// Terminal is a terminal's sign-in begun at the account app, as RFC 8628
// §3.2 answers one.
type Terminal struct {
	DeviceCode string
	UserCode   string
	// ApproveAt is where the person says yes, the code in it.
	ApproveAt string
	Lapses    time.Time
	Interval  time.Duration
}

// TerminalKey is what a terminal asks to be made: a key for Shale.
type TerminalKey struct {
	Name    string
	Methods []string
	Ends    time.Time
	Scope   string
}

// The RFC's words for a poll that carries no key (§3.5), as the account
// app answers them (roster's `account/device.go`, `devices.polled`), and
// one of roster's own: [PollRefused], the person's yes that roster would
// not make a key of, with roster's sentence as `error_description`.
// [PollDenied] is the person's own no, and carries no sentence.
const (
	PollPending  = "authorization_pending"
	PollSlowDown = "slow_down"
	PollDenied   = "access_denied"
	PollExpired  = "expired_token"
	PollRefused  = "refused"
)

// Account says whether this deployment names roster's account app, where
// a key is made from here when roster is elsewhere.
func (s *Store) Account() bool { return s.account != nil }

// accountClient is roster's account app as Shale's server reaches it.
type accountClient struct {
	base *url.URL
	http *http.Client
}

func newAccountClient(cfg Config) (*accountClient, error) {
	u, err := url.Parse(strings.TrimSuffix(cfg.Account, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("auth.roster.account: %q is not an address, such as https://account.example.com", cfg.Account)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.CaFile != "" {
		// roster's CA, which may have signed the account app's certificate
		// too, beside the system's.
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		pem, err := os.ReadFile(cfg.CaFile)
		if err != nil {
			return nil, fmt.Errorf("auth.roster.ca_file: %w", err)
		}
		pool.AppendCertsFromPEM(pem)
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}

	return &accountClient{base: u, http: &http.Client{Transport: tr, Timeout: 10 * time.Second}}, nil
}

// post is one JSON request to the account app and its answer: the body
// of a 2xx, or the account app's refusal in its own words.
func (a *accountClient) post(ctx context.Context, path string, in, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base.String()+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("content-type", "application/json")
	res, err := a.http.Do(req)
	if err != nil {
		return fmt.Errorf("the identity service's account app at %s: %w", a.base, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 16<<10))
	if err != nil {
		return fmt.Errorf("the identity service's account app at %s: %w", a.base, err)
	}
	if res.StatusCode/100 == 2 {
		return json.Unmarshal(body, out)
	}
	if res.StatusCode == http.StatusNotImplemented && strings.Contains(string(body), "does not sign terminals in") {
		return ErrNoTerminal
	}
	// A poll's refusal is JSON naming the RFC's word, which the caller
	// reads; anything else is the account app's sentence.
	if out != nil && res.StatusCode == http.StatusBadRequest && strings.HasPrefix(strings.TrimSpace(string(body)), "{") {
		if err := json.Unmarshal(body, out); err == nil {
			return nil
		}
	}

	return &AccountRefusal{Status: res.StatusCode, Words: strings.TrimSpace(string(body))}
}

// BeginTerminal begins a terminal's sign-in for one key for Shale at the
// account app (`POST /device/begin`, roster's `docs/usage/ways-in.md`, *For
// an app*): the key's name, what it may call, its end and its scope, for
// the app Shale by its name at roster. Nothing of the person is said,
// since nothing is minted until they say yes at the front door, signed in
// there. The device code it answers is the whole of what a poll is held
// to, and is for this server alone.
func (s *Store) BeginTerminal(ctx context.Context, k TerminalKey) (Terminal, error) {
	if s.account == nil {
		return Terminal{}, ErrNoAccount
	}
	in := map[string]any{"name": k.Name, "app": Agent, "allow": k.Methods}
	if k.Scope != "" {
		in["scope"] = k.Scope
	}
	if !k.Ends.IsZero() {
		in["date_expired"] = k.Ends.UTC().Format(time.RFC3339)
	}
	var out struct {
		DeviceCode string `json:"device_code"`
		UserCode   string `json:"user_code"`
		Uri        string `json:"verification_uri"`
		Complete   string `json:"verification_uri_complete"`
		ExpiresIn  int    `json:"expires_in"`
		Interval   int    `json:"interval"`
	}
	if err := s.account.post(ctx, "/device/begin", in, &out); err != nil {
		return Terminal{}, err
	}
	if out.DeviceCode == "" {
		return Terminal{}, fmt.Errorf("the identity service's account app at %s answered no device code", s.account.base)
	}
	at := out.Complete
	if at == "" {
		at = out.Uri
	}
	interval := time.Duration(out.Interval) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}

	return Terminal{
		DeviceCode: out.DeviceCode,
		UserCode:   out.UserCode,
		ApproveAt:  at,
		Lapses:     s.now().Add(time.Duration(out.ExpiresIn) * time.Second),
		Interval:   interval,
	}, nil
}

// PollTerminal asks the account app whether the person said yes
// (`POST /device/poll`): the key's token, once, or the word for why not
// ([PollPending], [PollSlowDown], [PollDenied], [PollExpired],
// [PollRefused]), and for [PollRefused] roster's sentence for it, which
// the approval page showed the person.
func (s *Store) PollTerminal(ctx context.Context, code string) (token, why, said string, err error) {
	if s.account == nil {
		return "", "", "", ErrNoAccount
	}
	var out struct {
		Token       string `json:"token"`
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	if err := s.account.post(ctx, "/device/poll", map[string]string{"device_code": code}, &out); err != nil {
		return "", "", "", err
	}
	if out.Error != "" {
		return "", out.Error, out.Description, nil
	}
	if out.Token == "" {
		return "", "", "", fmt.Errorf("the identity service's account app at %s answered neither a key nor why not", s.account.base)
	}

	return out.Token, "", "", nil
}

// ownKeys is what a person's keys for Shale are read by at roster: theirs,
// and for the app Shale, by its name in their tenant.
func ownKeys(tenant, holder pdid.Id, key []byte) *rstr.ApiKeyFilter {
	f := rstr.ApiKeyFilter_builder{
		Holder: rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
		App: rstr.HolderRef_builder{Slug: rstr.HolderRefBySlug_builder{
			Alias: z.Ptr(Agent), Tenant: rstr.TenantRef_builder{Id: tenant.Bytes()}.Build(),
		}.Build()}.Build(),
	}
	if key != nil {
		f.Ref = rstr.ApiKeyRef_builder{Id: key}.Build()
	}

	return f.Build()
}

// Keys is a person's keys for Shale, as roster holds them, asked as Shale
// in their tenant (`ApiKeyService/List` by holder and app). A key of
// roster's own, or for another app, is not among them.
func (s *Store) Keys(ctx context.Context, tenant, holder pdid.Id) ([]*rstr.ApiKey, error) {
	as, err := s.asIn(ctx, tenant)
	if err != nil {
		return nil, err
	}
	var vs []*rstr.ApiKey
	after := ""
	for {
		res, err := rstr.NewApiKeyServiceClient(s.conn).List(as, rstr.ApiKeyListRequest_builder{
			Filters: []*rstr.ApiKeyFilter{ownKeys(tenant, holder, nil)},
			Select:  keySelect,
			Size:    100,
			After:   after,
		}.Build())
		if err != nil {
			return nil, keysErr("the keys for shale", err)
		}
		for _, v := range res.GetItems() {
			if theirs(v, holder) {
				vs = append(vs, v)
			}
		}
		if res.GetNext() == "" || len(res.GetItems()) == 0 {
			return vs, nil
		}
		after = res.GetNext()
	}
}

// keySelect is a key's row and the name of the app it is for.
var keySelect = rstr.ApiKeySelect_builder{All: z.Ptr(true), App: rstr.HolderSelect_builder{Alias: z.Ptr(true)}.Build()}.Build()

// theirs says whether a key roster answered is the person's and for
// Shale: asked again of what came back, since the filter is what roster
// was asked and not what it is held to.
func theirs(v *rstr.ApiKey, holder pdid.Id) bool {
	id, err := pdid.From(v.GetHolder().GetId())

	return err == nil && id == holder && v.GetApp().GetAlias() == Agent
}

// EraseKey ends a person's key for Shale at roster, as Shale: read first,
// by its identifier, as theirs and for Shale, and erased only if it is.
// Anything else is [ErrNoKeyHere].
func (s *Store) EraseKey(ctx context.Context, tenant, holder pdid.Id, key []byte) error {
	as, err := s.asIn(ctx, tenant)
	if err != nil {
		return err
	}
	if _, err := pdid.From(key); err != nil {
		return ErrNoKeyHere
	}
	res, err := rstr.NewApiKeyServiceClient(s.conn).List(as, rstr.ApiKeyListRequest_builder{
		Filters: []*rstr.ApiKeyFilter{ownKeys(tenant, holder, key)},
		Select:  keySelect,
		Size:    1,
	}.Build())
	if err != nil {
		return keysErr("the key", err)
	}
	if len(res.GetItems()) != 1 || !theirs(res.GetItems()[0], holder) || !bytes.Equal(res.GetItems()[0].GetId(), key) {
		return ErrNoKeyHere
	}
	if _, err := rstr.NewApiKeyServiceClient(s.conn).Erase(as, rstr.ApiKeyRef_builder{Id: key}.Build()); err != nil {
		if status.Code(err) == codes.NotFound {
			return ErrNoKeyHere
		}

		return keysErr("ending the key", err)
	}

	return nil
}

// keysErr is roster refusing Shale a key that is for it, or a roster that
// could not be asked.
func keysErr(what string, err error) error {
	if status.Code(err) == codes.PermissionDenied {
		return fmt.Errorf("%w (%s: %s)", ErrKeysUnanswered, what, status.Convert(err).Message())
	}

	return fmt.Errorf("roster: %s: %w", what, err)
}

// asIn is [Store.as] by the tenant's identifier.
func (s *Store) asIn(ctx context.Context, tenant pdid.Id) (context.Context, error) {
	alias, err := s.alias(ctx, tenant)
	if err != nil {
		return nil, err
	}

	return s.as(ctx, alias)
}

// issueRequest is the person's `ApiKeyService/Issue` of a key for Shale.
func issueRequest(tenant, holder pdid.Id, k TerminalKey) *rstr.ApiKeyIssueRequest {
	tref := rstr.TenantRef_builder{Id: tenant.Bytes()}.Build()
	req := rstr.ApiKeyIssueRequest_builder{
		Holder:  rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
		App:     rstr.HolderRef_builder{Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(Agent), Tenant: tref}.Build()}.Build(),
		Alias:   k.Name,
		Methods: k.Methods,
		Scope:   k.Scope,
	}
	if !k.Ends.IsZero() {
		req.DateExpired = timestamppb.New(k.Ends)
	}

	return req.Build()
}
