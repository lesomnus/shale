// Package dnsupdate keeps names in a zone through RFC 2136 dynamic updates
// signed with TSIG (RFC 8945). It is how the Control Plane maintains the
// names of the `dns` address resolver (§34.10).
//
// It touches the names it owns and nothing else. Every name it holds
// carries an ownership TXT record (the marker); a name it creates must not
// exist yet, and a change or a removal is conditional on the TXT records it
// read, the marker among them. Those conditions are RFC 2136 prerequisites,
// so the server checks them in the same transaction as the update, and a
// name somebody else wrote in the meantime is refused rather than
// overwritten. Within a name it owns, it writes A, AAAA and its own TXT
// record, and leaves every other record there alone.
//
// Everything goes over TCP: a zone transfer needs it anyway, and nothing
// then depends on a reply fitting in a datagram.
package dnsupdate

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/miekg/dns"
)

// Config is where updates go and how they are signed.
type Config struct {
	// Server is `host` or `host:port`; the port is 53 when absent.
	Server string
	// Zone is the zone the names are in.
	Zone string
	// KeyName and Secret are the TSIG key; Algorithm is one of
	// Algorithms, hmac-sha256 when empty.
	KeyName   string
	Algorithm string
	Secret    []byte
	// Timeout bounds one exchange with the server; 10 s when zero.
	Timeout time.Duration
}

// algorithms are the TSIG algorithms by the names the policy takes.
var algorithms = map[string]string{
	"hmac-sha1":   dns.HmacSHA1,
	"hmac-sha224": dns.HmacSHA224,
	"hmac-sha256": dns.HmacSHA256,
	"hmac-sha384": dns.HmacSHA384,
	"hmac-sha512": dns.HmacSHA512,
}

// Algorithm is the TSIG algorithm a name means, hmac-sha256 for none, and
// whether it is one this package signs with.
func Algorithm(name string) (string, bool) {
	name = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(name)), ".")
	if name == "" {
		name = "hmac-sha256"
	}
	v, ok := algorithms[name]

	return v, ok
}

// Errors an update can end with. ErrRefused is the server declining the
// key or the request; ErrPrerequisite is a name that is not as it was read,
// or not ours, so the update was not applied.
var (
	ErrRefused      = errors.New("refused")
	ErrPrerequisite = errors.New("the name is not as it was read")
)

// ServerAddr is a server address with its port: 53 when it names none.
func ServerAddr(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", errors.New("no server")
	}
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		// No port: a name, an IPv4 address, or a bare IPv6 address.
		host, port = strings.TrimSuffix(strings.TrimPrefix(s, "["), "]"), "53"
	}
	if _, err := netip.ParseAddr(host); err != nil && !ValidName(host) {
		return "", fmt.Errorf("%q is neither an IP address nor a name", host)
	}
	if p, err := strconv.ParseUint(port, 10, 16); err != nil || p == 0 {
		return "", fmt.Errorf("%q is not a port", port)
	}

	return net.JoinHostPort(host, port), nil
}

// Fqdn is a name as this package compares names: lower case, with the
// final dot.
func Fqdn(name string) string { return strings.ToLower(dns.Fqdn(strings.TrimSpace(name))) }

// InZone says whether name is a name strictly below zone.
func InZone(name, zone string) bool {
	name, zone = Fqdn(name), Fqdn(zone)

	return name != zone && dns.IsSubDomain(zone, name)
}

// label is one label of a name this package writes or is configured with:
// letters, digits, hyphens and underscores.
var label = regexp.MustCompile(`^[A-Za-z0-9_]([A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?$`)

// ValidName says whether name is a domain name of host-name labels, which
// is what this package writes and is configured with.
func ValidName(name string) bool {
	name = strings.TrimSuffix(strings.TrimSpace(name), ".")
	if name == "" || len(name) > 253 {
		return false
	}
	for l := range strings.SplitSeq(name, ".") {
		if !label.MatchString(l) {
			return false
		}
	}

	return true
}

// Check says what is wrong with the configuration, without asking the
// server anything.
func (c Config) Check() error {
	if _, err := ServerAddr(c.Server); err != nil {
		return fmt.Errorf("server: %w", err)
	}
	if !ValidName(c.Zone) {
		return fmt.Errorf("zone: %q is not a domain name", c.Zone)
	}
	if !ValidName(c.KeyName) {
		return fmt.Errorf("key name: %q is not a domain name", c.KeyName)
	}
	if _, ok := Algorithm(c.Algorithm); !ok {
		return fmt.Errorf("algorithm: %q is none of hmac-sha1, hmac-sha224, hmac-sha256, hmac-sha384, hmac-sha512", c.Algorithm)
	}
	if len(c.Secret) == 0 {
		return errors.New("no TSIG secret")
	}

	return nil
}

// Client talks to one server about one zone.
type Client struct {
	server  string
	zone    string
	key     string
	alg     string
	secret  map[string]string
	timeout time.Duration
}

// New checks the configuration and makes a client. It dials nothing.
func New(c Config) (*Client, error) {
	if err := c.Check(); err != nil {
		return nil, err
	}
	server, _ := ServerAddr(c.Server)
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	key := Fqdn(c.KeyName)
	alg, _ := Algorithm(c.Algorithm)

	return &Client{
		server:  server,
		zone:    Fqdn(c.Zone),
		key:     key,
		alg:     alg,
		secret:  map[string]string{key: base64.StdEncoding.EncodeToString(c.Secret)},
		timeout: timeout,
	}, nil
}

// Zone is the zone the client updates.
func (c *Client) Zone() string { return c.zone }

// Records are what one name holds, as far as its owner is concerned.
type Records struct {
	// Addrs are its A and AAAA records, sorted.
	Addrs []netip.Addr
	// TTL is theirs; zero when they disagree.
	TTL uint32
	// TXT are its TXT records, each one's strings.
	TXT [][]string
	// Other says it holds records of any other type.
	Other bool
}

// Empty says the name holds nothing.
func (r *Records) Empty() bool {
	return r == nil || (len(r.Addrs) == 0 && len(r.TXT) == 0 && !r.Other)
}

// OwnedBy says the name carries the marker.
func (r *Records) OwnedBy(marker string) bool {
	if r == nil {
		return false
	}
	for _, t := range r.TXT {
		if strings.Join(t, "") == marker {
			return true
		}
	}

	return false
}

// Holds says the name's addresses are exactly w's, with its TTL.
func (r *Records) Holds(w Want) bool {
	if r == nil {
		return len(w.Addrs) == 0
	}

	return r.TTL == w.TTL && slices.Equal(r.Addrs, sorted(w.Addrs))
}

func (r *Records) add(rr dns.RR) {
	switch v := rr.(type) {
	case *dns.A:
		r.addr(v.A, v.Hdr.Ttl)
	case *dns.AAAA:
		r.addr(v.AAAA, v.Hdr.Ttl)
	case *dns.TXT:
		r.TXT = append(r.TXT, v.Txt)
	case *dns.RRSIG, *dns.NSEC, *dns.NSEC3:
		// DNSSEC's own, which a signed zone puts beside every name.
	default:
		r.Other = true
	}
}

func (r *Records) addr(ip net.IP, ttl uint32) {
	a, ok := netip.AddrFromSlice(ip)
	if !ok {
		return
	}
	a = a.Unmap()
	if len(r.Addrs) == 0 {
		r.TTL = ttl
	} else if r.TTL != ttl {
		r.TTL = 0
	}
	if !slices.Contains(r.Addrs, a) {
		r.Addrs = append(r.Addrs, a)
		slices.SortFunc(r.Addrs, netip.Addr.Compare)
	}
}

// Want is what one name should hold.
type Want struct {
	Addrs []netip.Addr
	TTL   uint32
}

func sorted(v []netip.Addr) []netip.Addr {
	out := slices.Clone(v)
	slices.SortFunc(out, netip.Addr.Compare)

	return slices.Compact(out)
}

// Probe asks the server for the zone's SOA, signed: it answers whether the
// server is there, serves the zone, and takes the key.
func (c *Client) Probe(ctx context.Context) error {
	m := new(dns.Msg)
	m.SetQuestion(c.zone, dns.TypeSOA)
	m.RecursionDesired = false
	r, err := c.exchange(ctx, m)
	if err != nil {
		return err
	}
	for _, rr := range r.Answer {
		if soa, ok := rr.(*dns.SOA); ok && Fqdn(soa.Hdr.Name) == c.zone {
			return nil
		}
	}

	return fmt.Errorf("%s does not answer with the SOA of %s: is it authoritative for it?", c.server, c.zone)
}

// Transfer reads the whole zone (AXFR), by name.
func (c *Client) Transfer(ctx context.Context) (map[string]*Records, error) {
	ctx, cancel := context.WithTimeout(ctx, 6*c.timeout)
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.server)
	if err != nil {
		return nil, err
	}
	// Transfer has no context of its own: closing the connection is what
	// ends a read the context gave up on.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	m := new(dns.Msg)
	m.SetAxfr(c.zone)
	m.SetTsig(c.key, c.alg, 300, time.Now().Unix())
	t := &dns.Transfer{Conn: &dns.Conn{Conn: conn}, TsigSecret: c.secret, ReadTimeout: c.timeout, WriteTimeout: c.timeout}
	ch, err := t.In(m, c.server)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("zone transfer: %w", err)
	}
	out := map[string]*Records{}
	for env := range ch {
		if env.Error != nil {
			err = env.Error
			continue
		}
		for _, rr := range env.RR {
			name := Fqdn(rr.Header().Name)
			if name == c.zone {
				continue
			}
			if out[name] == nil {
				out[name] = &Records{}
			}
			out[name].add(rr)
		}
	}
	if errors.Is(err, dns.ErrNoSig) {
		// Every message of a transfer is verified, and the one a server
		// answers a key it does not take with is not signed.
		err = fmt.Errorf("%w: the answer is not signed, as a server answers a key it does not take", ErrRefused)
	}
	if err != nil {
		return nil, fmt.Errorf("zone transfer: %w", err)
	}

	return out, nil
}

// Lookup asks the server what one name holds: its A, AAAA and TXT
// records, and whether it is an alias.
func (c *Client) Lookup(ctx context.Context, name string) (*Records, error) {
	name = Fqdn(name)
	out := &Records{}
	for _, t := range []uint16{dns.TypeA, dns.TypeAAAA, dns.TypeTXT} {
		m := new(dns.Msg)
		m.SetQuestion(name, t)
		m.RecursionDesired = false
		r, err := c.exchange(ctx, m)
		if err != nil {
			return nil, err
		}
		for _, rr := range r.Answer {
			if Fqdn(rr.Header().Name) != name {
				continue
			}
			if rr.Header().Rrtype == t || rr.Header().Rrtype == dns.TypeCNAME || rr.Header().Rrtype == dns.TypeDNAME {
				out.add(rr)
			}
		}
	}

	return out, nil
}

// Create writes a name that holds nothing: the marker and the addresses,
// on the condition that the name is not in use.
func (c *Client) Create(ctx context.Context, name string, w Want, marker string) error {
	name = Fqdn(name)
	m := c.update()
	m.NameNotUsed([]dns.RR{&dns.ANY{Hdr: dns.RR_Header{Name: name}}})
	m.Insert(append(addrRRs(name, w), markerRR(name, w.TTL, marker)))

	return c.apply(ctx, m)
}

// Replace gives a name it owns other addresses, on the condition that its
// TXT records, the marker among them, are still the ones `have` read.
func (c *Client) Replace(ctx context.Context, name string, have *Records, w Want, marker string) error {
	name = Fqdn(name)
	if !have.OwnedBy(marker) {
		return ErrPrerequisite
	}
	m := c.update()
	m.Used(txtRRs(name, have.TXT))
	m.RemoveRRset([]dns.RR{
		&dns.ANY{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA}},
		&dns.ANY{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA}},
	})
	m.Insert(addrRRs(name, w))

	return c.apply(ctx, m)
}

// Delete takes a name it owns out of the zone: its addresses and the
// marker, on the same condition as Replace. Other records there stay.
func (c *Client) Delete(ctx context.Context, name string, have *Records, marker string) error {
	name = Fqdn(name)
	if !have.OwnedBy(marker) {
		return ErrPrerequisite
	}
	m := c.update()
	m.Used(txtRRs(name, have.TXT))
	m.RemoveRRset([]dns.RR{
		&dns.ANY{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA}},
		&dns.ANY{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA}},
	})
	m.Remove([]dns.RR{markerRR(name, 0, marker)})

	return c.apply(ctx, m)
}

func (c *Client) update() *dns.Msg {
	m := new(dns.Msg)
	m.SetUpdate(c.zone)

	return m
}

func (c *Client) apply(ctx context.Context, m *dns.Msg) error {
	r, err := c.exchange(ctx, m)
	if err != nil {
		return err
	}
	if r.Rcode != dns.RcodeSuccess {
		return rcodeError(r)
	}

	return nil
}

// exchange sends one signed message and answers the reply, or an error
// for a reply that is not NOERROR or does not verify.
func (c *Client) exchange(ctx context.Context, m *dns.Msg) (*dns.Msg, error) {
	m.SetTsig(c.key, c.alg, 300, time.Now().Unix())
	cl := &dns.Client{Net: "tcp", TsigSecret: c.secret, Timeout: c.timeout}
	r, _, err := cl.ExchangeContext(ctx, m, c.server)
	if r != nil && r.Rcode != dns.RcodeSuccess && r.Rcode != dns.RcodeNameError {
		return nil, rcodeError(r)
	}
	if err != nil {
		return nil, err
	}

	return r, nil
}

func rcodeError(r *dns.Msg) error {
	what := dns.RcodeToString[r.Rcode]
	if what == "" {
		what = fmt.Sprintf("rcode %d", r.Rcode)
	}
	if t := r.IsTsig(); t != nil && t.Error != dns.RcodeSuccess {
		if e := dns.RcodeToString[int(t.Error)]; e != "" {
			what += " (TSIG " + e + ")"
		}
	}
	switch r.Rcode {
	case dns.RcodeNotAuth, dns.RcodeRefused, dns.RcodeNotZone:
		return fmt.Errorf("%w: %s", ErrRefused, what)
	case dns.RcodeYXDomain, dns.RcodeYXRrset, dns.RcodeNXRrset, dns.RcodeNameError:
		return fmt.Errorf("%w: %s", ErrPrerequisite, what)
	}

	return errors.New(what)
}

func addrRRs(name string, w Want) []dns.RR {
	var out []dns.RR
	for _, a := range sorted(w.Addrs) {
		if a.Is4() {
			out = append(out, &dns.A{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: w.TTL}, A: a.AsSlice()})
		} else {
			out = append(out, &dns.AAAA{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: w.TTL}, AAAA: a.AsSlice()})
		}
	}

	return out
}

func markerRR(name string, ttl uint32, marker string) dns.RR {
	return &dns.TXT{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: ttl}, Txt: []string{marker}}
}

func txtRRs(name string, txt [][]string) []dns.RR {
	out := make([]dns.RR, 0, len(txt))
	for _, t := range txt {
		out = append(out, &dns.TXT{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeTXT, Class: dns.ClassINET}, Txt: slices.Clone(t)})
	}

	return out
}
