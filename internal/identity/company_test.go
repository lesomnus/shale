package identity

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/roster/rstr"
)

// companyFixture is a Company over an outside roster's double, its log
// kept, on a clock of the test's.
type companyFixture struct {
	f      *fakeRoster
	c      *Company
	log    *bytes.Buffer
	now    time.Time
	tenant pdid.Id
	errs   []error
}

func newCompanyFixture(t *testing.T) *companyFixture {
	t.Helper()
	x := &companyFixture{f: newFakeRoster(t, "acme"), log: &bytes.Buffer{}, now: time.Unix(1_700_000_000, 0)}
	log := slog.New(slog.NewTextHandler(x.log, nil))
	s := x.f.openWith(t, Config{Keys: map[string]string{"acme": x.f.tenantKey}})
	s.log = log
	x.c = NewCompany(s, CompanyConfig{}, log)
	x.c.Now = func() time.Time { return x.now }
	x.tenant = pdid.Id(x.f.tenant.GetId())

	return x
}

// of is the answer for a person, the error kept for the test that no
// address is in one.
func (x *companyFixture) of(t *testing.T, holder []byte) (bool, error) {
	t.Helper()
	is, err := x.c.Of(context.Background(), x.tenant, pdid.Id(holder))
	if err != nil {
		x.errs = append(x.errs, err)
		require.False(t, is, "a failure is no")
	}

	return is, err
}

func (x *companyFixture) is(t *testing.T, holder []byte) bool {
	t.Helper()
	is, err := x.of(t, holder)
	require.NoError(t, err)

	return is
}

// later is past what is kept.
func (x *companyFixture) later() { x.now = x.now.Add(CompanyTtl + time.Second) }

// Of the company is a proved address at a mail domain the tenant names,
// the address's domain lower-cased and equal to the domain's name: one of
// two addresses is enough; an address elsewhere, one not proved, one
// erased, one at a subdomain or a suffix, no address and no mail domain
// are no (§33.1).
func TestCompanyIsAProvedAddressAtANamedDomain(t *testing.T) {
	x := newCompanyFixture(t)
	f := x.f
	f.addDomain("acme", "acme.example")

	alice := f.addHolder("alice")
	f.addEmail(alice, "alice@acme.example", true)
	bob := f.addHolder("bob")
	f.addEmail(bob, "bob@elsewhere.example", true)
	carol := f.addHolder("carol")
	f.addEmail(carol, "carol@acme.example", false)
	dave := f.addHolder("dave")
	f.addEmail(dave, "dave@elsewhere.example", true)
	f.addEmail(dave, "dave@acme.example", false)
	f.addEmail(dave, "d@acme.example", true)
	erin := f.addHolder("erin")
	frank := f.addHolder("frank")
	f.addEmail(frank, "Frank@ACME.Example", true)
	grace := f.addHolder("grace")
	f.addEmail(grace, "grace@mail.acme.example", true)
	f.addEmail(grace, "grace@notacme.example", true)
	f.addEmail(grace, "grace@acme.example.org", true)
	heidi := f.addHolder("heidi")
	f.addEmail(heidi, "heidi@acme.example", true)
	f.eraseEmails(heidi)

	require.True(t, x.is(t, alice), "a proved address at the domain")
	require.False(t, x.is(t, bob), "a proved address elsewhere")
	require.False(t, x.is(t, carol), "an address at the domain nobody proved")
	require.True(t, x.is(t, dave), "one of several is enough")
	require.False(t, x.is(t, erin), "no address")
	require.True(t, x.is(t, frank), "the domain compared lower-cased")
	require.False(t, x.is(t, grace), "a subdomain, a suffix and a longer name are other domains")
	require.False(t, x.is(t, heidi), "an erased address is none")

	// Only the address and date_verified were asked for, of one person
	// each time.
	want := rstr.EmailSelect_builder{Address: ptr(true), DateVerified: ptr(true)}.Build()
	require.NotEmpty(t, f.emailSelects)
	for _, sel := range f.emailSelects {
		require.True(t, proto.Equal(want, sel), "selected: %v", sel)
	}

	// A person of another tenant's is nobody here: the wall.
	other := f.addTenant("other")
	mallory := f.addHolderIn(other, "mallory")
	f.mu.Lock()
	f.emails = append(f.emails, fakeEmail{holder: mallory, tenant: other.GetId(), address: "mallory@acme.example", verified: f.emails[0].verified})
	f.mu.Unlock()
	require.False(t, x.is(t, mallory))
}

func ptr[T any](v T) *T { return &v }

// A tenant that names no mail domain has nobody of the company, and
// nobody's addresses are read.
func TestCompanyWithNoMailDomain(t *testing.T) {
	x := newCompanyFixture(t)
	alice := x.f.addHolder("alice")
	x.f.addEmail(alice, "alice@acme.example", true)
	require.False(t, x.is(t, alice))
	require.Zero(t, x.f.emailLists.Load(), "no address is read")
}

// The answer is kept for the TTL and not asked again within it, and asked
// again after it; the tenant's mail domains as long.
func TestCompanyIsKept(t *testing.T) {
	x := newCompanyFixture(t)
	x.f.addDomain("acme", "acme.example")
	alice := x.f.addHolder("alice")
	x.f.addEmail(alice, "alice@acme.example", true)
	bob := x.f.addHolder("bob")

	require.True(t, x.is(t, alice))
	require.False(t, x.is(t, bob))
	require.Equal(t, int32(1), x.f.domainLists.Load(), "the tenant's domains, once for both")
	require.Equal(t, int32(2), x.f.emailLists.Load())

	x.now = x.now.Add(CompanyTtl - time.Second)
	x.f.eraseEmails(alice)
	x.f.addEmail(bob, "bob@acme.example", true)
	require.True(t, x.is(t, alice), "kept")
	require.False(t, x.is(t, bob), "kept")
	require.Equal(t, int32(1), x.f.domainLists.Load())
	require.Equal(t, int32(2), x.f.emailLists.Load(), "not asked within the TTL")

	x.later()
	require.False(t, x.is(t, alice), "asked again after it")
	require.True(t, x.is(t, bob))
	require.Equal(t, int32(2), x.f.domainLists.Load())
	require.Equal(t, int32(4), x.f.emailLists.Load())
}

// A roster that refuses the two reads -- Shale's holder is not allowed
// them -- is no, and one line says which methods an operator of roster
// adds; a roster that is down is no for five seconds, not asked again
// within them, and asked again after.
func TestCompanyFailsClosed(t *testing.T) {
	x := newCompanyFixture(t)
	x.f.addDomain("acme", "acme.example")
	alice := x.f.addHolder("alice")
	x.f.addEmail(alice, "alice@acme.example", true)

	// The mail domains refused.
	x.f.refuse(rstr.MailDomainService_List_FullMethodName, true)
	_, err := x.of(t, alice)
	require.Error(t, err)
	_, err = x.of(t, alice)
	require.Error(t, err)
	lines := strings.Count(x.log.String(), "\n")
	require.Equal(t, 1, lines, "once per remembered failure, not per call")
	require.Contains(t, x.log.String(), rstr.EmailService_List_FullMethodName)
	require.Contains(t, x.log.String(), rstr.MailDomainService_List_FullMethodName)
	require.Contains(t, x.log.String(), "roster app install --allow")

	// The addresses refused.
	x.f.refuse(rstr.MailDomainService_List_FullMethodName, false)
	x.f.refuse(rstr.EmailService_List_FullMethodName, true)
	x.now = x.now.Add(companyRetry + time.Second)
	x.log.Reset()
	_, err = x.of(t, alice)
	require.Error(t, err)
	_, err = x.of(t, alice)
	require.Error(t, err)
	require.Equal(t, 1, strings.Count(x.log.String(), "\n"))
	require.Contains(t, x.log.String(), rstr.EmailService_List_FullMethodName)
	require.Contains(t, x.log.String(), rstr.MailDomainService_List_FullMethodName)

	// Allowed: yes.
	x.f.refuse(rstr.EmailService_List_FullMethodName, false)
	x.now = x.now.Add(companyRetry + time.Second)
	require.True(t, x.is(t, alice))

	// Down: no, remembered for five seconds, and asked again after them.
	x.later()
	x.f.down.Store(true)
	x.log.Reset()
	_, err = x.of(t, alice)
	require.Error(t, err)
	calls := x.f.calls.Load()
	_, err = x.of(t, alice)
	require.Error(t, err)
	require.Equal(t, calls, x.f.calls.Load(), "not asked within five seconds")
	require.Equal(t, 1, strings.Count(x.log.String(), "\n"))
	x.f.down.Store(false)
	x.now = x.now.Add(companyRetry - time.Second)
	_, err = x.of(t, alice)
	require.Error(t, err, "still within them")
	x.now = x.now.Add(2 * time.Second)
	require.True(t, x.is(t, alice), "asked again after them")
}

// No address is in a log line or an error of this path, whatever it
// answered: yes, no, refused, down.
func TestCompanyKeepsNoAddress(t *testing.T) {
	x := newCompanyFixture(t)
	x.f.addDomain("acme", "acme.example")
	addresses := []string{"alice@acme.example", "alice@elsewhere.example", "bob@acme.example"}
	alice := x.f.addHolder("alice")
	x.f.addEmail(alice, addresses[0], true)
	x.f.addEmail(alice, addresses[1], true)
	bob := x.f.addHolder("bob")
	x.f.addEmail(bob, addresses[2], false)

	x.is(t, alice)
	x.is(t, bob)
	x.later()
	x.f.refuse(rstr.EmailService_List_FullMethodName, true)
	x.of(t, alice)
	x.of(t, bob)
	x.f.refuse(rstr.EmailService_List_FullMethodName, false)
	x.later()
	x.f.down.Store(true)
	x.of(t, alice)
	x.of(t, bob)
	require.NotEmpty(t, x.errs)
	require.NotEmpty(t, x.log.String())

	for _, a := range addresses {
		local := a[:strings.IndexByte(a, '@')]
		require.NotContains(t, x.log.String(), a)
		require.NotContains(t, x.log.String(), local+"@")
		for _, err := range x.errs {
			require.NotContains(t, err.Error(), a)
			require.NotContains(t, err.Error(), local+"@")
		}
	}
}

// Where roster runs inside Shale everybody of the tenant is of the
// company, and nothing is asked of it: it holds no addresses and no mail
// domains, and Shale's holder there is allowed neither list.
func TestCompanyEmbedded(t *testing.T) {
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Open(c, Config{}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	go s.Run(c)

	admin, _, err := s.Seed(c, "acme", "admin")
	require.NoError(t, err)
	carol, err := s.AddPerson(c, "acme", "carol", "Carol")
	require.NoError(t, err)

	co := NewCompany(s, CompanyConfig{}, nil)
	for _, p := range []Person{admin, carol} {
		is, err := co.Of(c, p.Tenant, p.Id)
		require.NoError(t, err)
		require.True(t, is)
	}
	for _, m := range CompanyMethods {
		require.NotContains(t, AgentMethods, m, "nothing is added to what the embedded roster's agent may call")
	}
}
