package core

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/slug"

	"github.com/lesomnus/roster/rstr"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/identity"
)

// A person's keys for Shale (§33.1, *A person's key for Shale*): API tokens
// for a script, which are roster's rows, made, listed and ended here by the
// person for themselves -- whatever roles they hold, since a person's own
// keys are theirs to see and end -- and about nobody else: no request names
// a person, and each is about the caller. The cluster API serves none of
// them.
//
// Making one is the person's at roster. Where roster is elsewhere it is a
// terminal's sign-in for one key at roster's account app: BeginKey begins
// it and answers where the person's browser says yes, and a handle; PollKey
// asks, with the handle, until roster answers. Nothing of the person's is
// held here between the two: what the account app answered is sealed into
// the handle, under a key of the KEK's, with the person, their tenant and a
// digest of the credential that began it, so the device code never reaches
// the browser in the clear and another session presenting the handle is
// refused. Where roster runs in this process the key is minted at once as
// the person, since this console is roster's front door there.
//
// Listing and ending are Shale's own at roster, as the app the keys are for
// (roster's `app-administrator`): the person's keys for Shale, by their
// identifier, and none of anybody else's. The token is answered once, and
// never kept, logged or put on the trail here.

// maxKeyScope is roster's bound on a key's scope (`core.MaxKeyScope`),
// said again so that a key is refused before anybody is asked to approve
// what roster would refuse.
const maxKeyScope = 256

// keyHandleLabel is what the key a handle is sealed under is derived by.
const keyHandleLabel = "shale key handle"

// keyHandleVersion is the first byte of a sealed handle.
const keyHandleVersion = 1

type credentialKey struct{}

// WithCredential says which credential a call came with, by a digest of
// it: a session's cookie, a key. A handle BeginKey seals is bound to it,
// and PollKey answers only a call that came with the same one.
func WithCredential(ctx context.Context, digest [sha256.Size]byte) context.Context {
	return context.WithValue(ctx, credentialKey{}, digest)
}

// CredentialFrom is what WithCredential said, where it said anything.
func CredentialFrom(ctx context.Context) ([sha256.Size]byte, bool) {
	v, ok := ctx.Value(credentialKey{}).([sha256.Size]byte)

	return v, ok
}

// sealedKey is what a handle carries.
type sealedKey struct {
	tenant, holder pdid.Id
	credential     [sha256.Size]byte
	lapses         time.Time
	code           string
}

func (s Core) handleKey() (cipher.AEAD, error) {
	if s.d.Kek == nil {
		return nil, status.Error(codes.FailedPrecondition, "this control plane has no key to seal a handle with yet; `shale init` makes it")
	}
	kek := s.d.Kek()
	if len(kek) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "this control plane has no key to seal a handle with yet; `shale init` makes it")
	}
	h := hmac.New(sha256.New, kek)
	h.Write([]byte(keyHandleLabel))
	block, err := aes.NewCipher(h.Sum(nil))
	if err != nil {
		return nil, err
	}

	return cipher.NewGCM(block)
}

func (s Core) seal(v sealedKey) (string, error) {
	aead, err := s.handleKey()
	if err != nil {
		return "", err
	}
	b := make([]byte, 0, 1+16+16+sha256.Size+8+len(v.code))
	b = append(b, keyHandleVersion)
	b = append(b, v.tenant.Bytes()...)
	b = append(b, v.holder.Bytes()...)
	b = append(b, v.credential[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(v.lapses.Unix()))
	b = append(b, v.code...)
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(aead.Seal(nonce, nonce, b, []byte(keyHandleLabel))), nil
}

var errHandle = status.Error(codes.InvalidArgument, "handle: not one this deployment answered BeginKey with")

func (s Core) unseal(handle string) (sealedKey, error) {
	aead, err := s.handleKey()
	if err != nil {
		return sealedKey{}, err
	}
	raw, err := base64.RawURLEncoding.DecodeString(handle)
	if err != nil || len(raw) < aead.NonceSize() {
		return sealedKey{}, errHandle
	}
	b, err := aead.Open(nil, raw[:aead.NonceSize()], raw[aead.NonceSize():], []byte(keyHandleLabel))
	if err != nil || len(b) < 1+16+16+sha256.Size+8+1 || b[0] != keyHandleVersion {
		return sealedKey{}, errHandle
	}
	b = b[1:]
	var v sealedKey
	if v.tenant, err = pdid.From(b[:16]); err != nil {
		return sealedKey{}, errHandle
	}
	if v.holder, err = pdid.From(b[16:32]); err != nil {
		return sealedKey{}, errHandle
	}
	copy(v.credential[:], b[32:32+sha256.Size])
	b = b[32+sha256.Size:]
	v.lapses = time.Unix(int64(binary.BigEndian.Uint64(b[:8])), 0)
	v.code = string(b[8:])

	return v, nil
}

// keyAsked is what a key is asked to be, held to roster's rules for it
// before roster is asked: so a key the person would be shown and then
// refused is refused here, in roster's words.
func keyAsked(req *api.HolderBeginKeyRequest, now time.Time) (identity.TerminalKey, error) {
	k := identity.TerminalKey{Name: req.GetName(), Methods: req.GetMethods(), Scope: req.GetScope()}
	if k.Name == "" {
		return k, invalid("name", "a name for the key, which is what it is told apart by when somebody ends it")
	}
	w, err := slug.ParseAlias(k.Name)
	if err != nil {
		return k, invalid("name", err.Error())
	}
	if w != k.Name {
		return k, invalid("name", fmt.Sprintf("written as it is read, so %q rather than %q", w, k.Name))
	}
	if len(k.Methods) == 0 {
		return k, invalid("methods", "a key that allows nothing opens no door")
	}
	for _, m := range k.Methods {
		if !strings.HasPrefix(m, "/shale.") {
			return k, invalid("methods", fmt.Sprintf("%q is not one of Shale's: a key for Shale calls Shale", m))
		}
	}
	if n := len(k.Scope); n > maxKeyScope {
		return k, invalid("scope", fmt.Sprintf("at most %d bytes, and this is %d", maxKeyScope, n))
	}
	if v := req.GetDateExpired(); v != nil {
		k.Ends = v.AsTime()
		if !k.Ends.After(now) {
			return k, invalid("date_expired", "a key cannot have already expired")
		}
	}

	return k, nil
}

// ownCaller is the person calling, by identifier and tenant: the one each
// of these is about.
func (s coreHolder) ownCaller(ctx context.Context) (tenant, holder pdid.Id, method string, err error) {
	f, err := actor(ctx)
	if err != nil {
		return pdid.Nil, pdid.Nil, "", err
	}
	if kindOf(f.Actor) != DomHolder {
		return pdid.Nil, pdid.Nil, "", status.Error(codes.PermissionDenied, "a person's keys are a person's: nothing else has any")
	}
	if s.d.Identity == nil {
		return pdid.Nil, pdid.Nil, "", status.Error(codes.FailedPrecondition, "no identity store: a key for Shale is roster's, and this deployment has no roster")
	}

	return f.Tenant, f.Actor, f.Authentication.Method, nil
}

// BeginKey begins making the caller a key for Shale; see the top of this
// file.
func (s coreHolder) BeginKey(ctx context.Context, req *api.HolderBeginKeyRequest) (*api.HolderBeginKeyResponse, error) {
	tenant, holder, method, err := s.ownCaller(ctx)
	if err != nil {
		return nil, err
	}
	k, err := keyAsked(req, s.d.now())
	if err != nil {
		return nil, err
	}
	// A scope is a list of cameras of the caller's, and a key that carries
	// one is given only what a list confines (§33.1).
	if err := s.keyReach(ctx, tenant, holder, k); err != nil {
		return nil, err
	}

	if s.d.Identity.Embedded() {
		// This console is roster's front door, so the person's yes is the
		// call itself -- which a key cannot say for them: there is no
		// second yes, and a key that mints keys is as wide as its person.
		if method == auth.MethodBearer {
			return nil, status.Error(codes.PermissionDenied, "a key does not make keys here: the identity service runs inside Shale, so making one is the person's own yes, said signed in at the console")
		}
		token, key, err := s.d.Identity.IssueKeyAs(ctx, tenant, holder, k)
		if err != nil {
			return nil, keyErr(err)
		}

		return api.HolderBeginKeyResponse_builder{Token: token, Key: holderKey(key)}.Build(), nil
	}

	cred, ok := CredentialFrom(ctx)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "a key is begun from a signed-in session, or with a key, which the handle is bound to")
	}
	t, err := s.d.Identity.BeginTerminal(ctx, k)
	if err != nil {
		return nil, keyErr(err)
	}
	handle, err := s.seal(sealedKey{tenant: tenant, holder: holder, credential: cred, lapses: t.Lapses, code: t.DeviceCode})
	if err != nil {
		return nil, err
	}

	return api.HolderBeginKeyResponse_builder{
		ApproveAt:  t.ApproveAt,
		UserCode:   t.UserCode,
		Handle:     handle,
		Interval:   durationpb.New(t.Interval),
		DateLapses: timestamppb.New(t.Lapses),
	}.Build(), nil
}

// PollKey asks roster's account app whether the person said yes to the
// key a handle names; see the top of this file.
func (s coreHolder) PollKey(ctx context.Context, req *api.HolderPollKeyRequest) (*api.HolderPollKeyResponse, error) {
	tenant, holder, _, err := s.ownCaller(ctx)
	if err != nil {
		return nil, err
	}
	v, err := s.unseal(req.GetHandle())
	if err != nil {
		return nil, err
	}
	cred, ok := CredentialFrom(ctx)
	if !ok || v.tenant != tenant || v.holder != holder || !hmac.Equal(v.credential[:], cred[:]) {
		return nil, status.Error(codes.PermissionDenied, "handle: another session began this key, and only the session or the key that began one asks after it")
	}
	if !s.d.now().Before(v.lapses) {
		return api.HolderPollKeyResponse_builder{State: api.HolderKeyState_HOLDER_KEY_STATE_LAPSED}.Build(), nil
	}
	token, why, said, err := s.d.Identity.PollTerminal(ctx, v.code)
	if err != nil {
		return nil, keyErr(err)
	}
	switch why {
	case "":
		return api.HolderPollKeyResponse_builder{State: api.HolderKeyState_HOLDER_KEY_STATE_MADE, Token: token}.Build(), nil
	case identity.PollPending:
		return api.HolderPollKeyResponse_builder{State: api.HolderKeyState_HOLDER_KEY_STATE_PENDING, Interval: durationpb.New(5 * time.Second)}.Build(), nil
	case identity.PollSlowDown:
		return api.HolderPollKeyResponse_builder{State: api.HolderKeyState_HOLDER_KEY_STATE_PENDING, Interval: durationpb.New(10 * time.Second)}.Build(), nil
	case identity.PollDenied:
		// The person's own no, which carries no sentence.
		return api.HolderPollKeyResponse_builder{State: api.HolderKeyState_HOLDER_KEY_STATE_REFUSED}.Build(), nil
	case identity.PollRefused:
		// Their yes, and roster's no to it -- a method they do not hold at
		// Shale, a name they already use -- in the sentence roster's
		// approval page showed them. The code is over at roster too.
		if said == "" {
			said = "the identity service would not make the key"
		}
		return api.HolderPollKeyResponse_builder{State: api.HolderKeyState_HOLDER_KEY_STATE_REFUSED, Refusal: said}.Build(), nil
	default:
		// `expired_token`: lapsed, or carried away by an earlier poll.
		return api.HolderPollKeyResponse_builder{State: api.HolderKeyState_HOLDER_KEY_STATE_LAPSED}.Build(), nil
	}
}

// ListKeys is the caller's keys for Shale, as roster holds them.
func (s coreHolder) ListKeys(ctx context.Context, _ *api.HolderListKeysRequest) (*api.HolderListKeysResponse, error) {
	tenant, holder, _, err := s.ownCaller(ctx)
	if err != nil {
		return nil, err
	}
	vs, err := s.d.Identity.Keys(ctx, tenant, holder)
	if err != nil {
		return nil, keyErr(err)
	}
	keys := make([]*api.HolderKey, 0, len(vs))
	for _, v := range vs {
		keys = append(keys, holderKey(v))
	}
	held, err := s.d.Identity.Reaches(ctx, tenant, holder)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "cannot ask roster what you hold at Shale right now: %v", err)
	}
	holds := heldMethods(held)
	var reads, narrowed []string
	for _, m := range holds {
		if personMay[m] {
			reads = append(reads, m)
		}
		if keyScopeMay[m] {
			narrowed = append(narrowed, m)
		}
	}

	b := api.HolderListKeysResponse_builder{Keys: keys, Holds: holds, Reads: reads, Narrowed: narrowed, Remembered: durationpb.New(s.remembered())}
	if len(holds) == 0 && s.d.Identity.Embedded() && s.d.Operators == nil {
		// Nobody here can hold a role of Shale's: they are written at the
		// roster in this process only where operators are named (`shale
		// init`), so asking an administrator for one is no answer.
		b.NoneHeld = noneHeld
	}

	return b.Build(), nil
}

// noneHeld is why nobody holds anything a token could be given, where roster
// runs in this process and no operators are named.
const noneHeld = "This deployment names no operators (auth.operators), so no role of Shale's exists for anybody to hold, " +
	"and a token can do only what its person holds. An operator sets auth.operators.tenant to the tenant whose people operate Shale, " +
	"and gives people roles of Shale's from there."

func (s coreHolder) remembered() time.Duration {
	if s.d.KeyRemembered > 0 {
		return s.d.KeyRemembered
	}

	return identity.OperatorsTtl
}

// heldMethods is every method the tenant API serves to a person that one
// of the patterns held covers on its own, sorted: what roster lets a key
// be given (`mayGrant`: one grant has to cover a method by itself).
func heldMethods(held []string) []string {
	var out []string
	protoregistry.GlobalFiles.RangeFilesByPackage("shale", func(fd protoreflect.FileDescriptor) bool {
		for i := range fd.Services().Len() {
			sd := fd.Services().Get(i)
			for j := range sd.Methods().Len() {
				m := "/" + string(sd.FullName()) + "/" + string(sd.Methods().Get(j).Name())
				if !IsTenantService(m) || closedToEveryone(m) || strings.HasPrefix(m, "/shale.DelegationService/") {
					continue
				}
				for _, h := range held {
					if frame.Covers(h, m) {
						out = append(out, m)
						break
					}
				}
			}
		}
		return true
	})
	slices.Sort(out)

	return slices.Compact(out)
}

// EndKey ends one of the caller's keys for Shale at roster, and says how
// long what was opened with it may still run.
func (s coreHolder) EndKey(ctx context.Context, req *api.HolderEndKeyRequest) (*api.HolderEndKeyResponse, error) {
	tenant, holder, _, err := s.ownCaller(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.d.Identity.EraseKey(ctx, tenant, holder, req.GetId()); err != nil {
		return nil, keyErr(err)
	}
	return api.HolderEndKeyResponse_builder{Remembered: durationpb.New(s.remembered())}.Build(), nil
}

// holderKey is a key as roster holds it, as this answers it.
func holderKey(v *rstr.ApiKey) *api.HolderKey {
	if v == nil {
		return nil
	}

	return api.HolderKey_builder{
		Id:          v.GetId(),
		Name:        v.GetAlias(),
		Methods:     v.GetMethods(),
		DateExpired: v.GetDateExpired(),
		DateUsed:    v.GetDateUsed(),
		DateCreated: v.GetDateCreated(),
		Scope:       v.GetScope(),
	}.Build()
}

// keyErr is what roster, or its account app, said, as this answers it:
// what roster refused in roster's words, what an operator has to mend in a
// sentence that names it, and a roster that could not be asked as
// Unavailable.
func keyErr(err error) error {
	var refusal *identity.AccountRefusal
	switch {
	case errors.Is(err, identity.ErrNoAccount), errors.Is(err, identity.ErrNoTerminal):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, identity.ErrKeysUnanswered):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, identity.ErrNoKeyHere):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, identity.ErrNoPerson), errors.Is(err, identity.ErrNoTenant):
		return status.Error(codes.NotFound, err.Error())
	case errors.As(err, &refusal):
		switch refusal.Status {
		case http.StatusBadRequest:
			return status.Error(codes.InvalidArgument, refusal.Words)
		case http.StatusNotFound:
			return status.Errorf(codes.FailedPrecondition, "the identity service's account app serves no tenant at the name it is reached by from here (auth.roster.account), "+
				"so it cannot begin a key; its operator adds that name to the tenant at roster (a Host), or this deployment names the account app by one: %s", refusal.Words)
		case http.StatusServiceUnavailable, http.StatusTooManyRequests:
			return status.Error(codes.Unavailable, refusal.Words)
		case http.StatusNotImplemented:
			return status.Error(codes.FailedPrecondition, refusal.Words)
		}

		return status.Errorf(codes.Unavailable, "the identity service's account app: %s", refusal.Words)
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		switch st.Code() {
		case codes.InvalidArgument, codes.PermissionDenied, codes.AlreadyExists, codes.NotFound:
			// roster's refusal of what was asked, in its words: a method the
			// person does not hold at Shale, a name they have used.
			return status.Error(st.Code(), st.Message())
		}
	}
	return status.Errorf(codes.Unavailable, "cannot ask roster about your keys right now: %v", err)
}
