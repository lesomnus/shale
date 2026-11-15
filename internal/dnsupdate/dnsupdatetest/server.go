// Package dnsupdatetest is an authoritative server for one zone, in the
// process, for the tests of the `dns` address resolver (§34.10): it answers
// queries and zone transfers, applies RFC 2136 updates with their
// prerequisites, and takes nothing that is not signed with its TSIG key.
package dnsupdatetest

import (
	"encoding/base64"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
)

// Server is one zone on 127.0.0.1, over TCP.
type Server struct {
	Zone   string
	Key    string
	Secret []byte

	srv *dns.Server
	ln  net.Listener

	mu      sync.Mutex
	rrs     map[string][]dns.RR
	soa     *dns.SOA
	updates int
	refused int
}

// Counts answers how many updates were applied, and how many messages
// were refused for their signature.
func (s *Server) Counts() (updates, refused int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.updates, s.refused
}

// Start serves zone, signed with key and secret, until Close.
func Start(zone, key string, secret []byte) (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	zone, key = fqdn(zone), fqdn(key)
	s := &Server{
		Zone: zone, Key: key, Secret: secret, ln: ln,
		rrs: map[string][]dns.RR{},
		soa: &dns.SOA{
			Hdr: dns.RR_Header{Name: zone, Rrtype: dns.TypeSOA, Class: dns.ClassINET, Ttl: 300},
			Ns:  "ns." + zone, Mbox: "hostmaster." + zone, Serial: 1, Refresh: 3600, Retry: 600, Expire: 86400, Minttl: 60,
		},
	}
	s.srv = &dns.Server{
		Listener:      ln,
		Net:           "tcp",
		TsigSecret:    map[string]string{key: base64.StdEncoding.EncodeToString(secret)},
		Handler:       s,
		MsgAcceptFunc: accept,
	}
	started := make(chan struct{})
	s.srv.NotifyStartedFunc = func() { close(started) }
	go s.srv.ActivateAndServe()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
	}

	return s, nil
}

// Addr is where the server listens.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops it.
func (s *Server) Close() error { return s.srv.Shutdown() }

// Add puts records in the zone directly, as somebody other than Shale
// would: each in zone file syntax.
func (s *Server) Add(rrs ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range rrs {
		rr, err := dns.NewRR(v)
		if err != nil {
			return err
		}
		s.insert(rr)
	}

	return nil
}

// Remove takes a name's records of one type out directly, as an operator
// would behind Shale's back; dns.TypeANY takes them all.
func (s *Server) Remove(name string, t uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name = fqdn(name)
	if t == dns.TypeANY {
		delete(s.rrs, name)
		return
	}
	s.rrs[name] = slices.DeleteFunc(s.rrs[name], func(rr dns.RR) bool { return rr.Header().Rrtype == t })
	if len(s.rrs[name]) == 0 {
		delete(s.rrs, name)
	}
}

// Get answers a name's records of one type, or all of them for
// dns.TypeANY, in zone file syntax without the TTL.
func (s *Server) Get(name string, t uint16) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, rr := range s.rrs[fqdn(name)] {
		if t == dns.TypeANY || rr.Header().Rrtype == t {
			out = append(out, strings.TrimPrefix(dns.Copy(rr).String(), rr.Header().String()))
		}
	}
	slices.Sort(out)

	return out
}

// TTL answers the TTL of a name's records of one type.
func (s *Server) TTL(name string, t uint16) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rr := range s.rrs[fqdn(name)] {
		if rr.Header().Rrtype == t {
			return rr.Header().Ttl
		}
	}

	return 0
}

// Names answers every name that holds something.
func (s *Server) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for n := range s.rrs {
		out = append(out, n)
	}
	slices.Sort(out)

	return out
}

func accept(dns.Header) dns.MsgAcceptAction { return dns.MsgAccept }

// ServeDNS answers one message.
func (s *Server) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	r := new(dns.Msg)
	r.SetReply(req)
	t := req.IsTsig()
	if t == nil || w.TsigStatus() != nil {
		s.mu.Lock()
		s.refused++
		s.mu.Unlock()
		r.SetRcode(req, dns.RcodeNotAuth)
		w.WriteMsg(r)
		return
	}
	defer func() {
		r.SetTsig(t.Hdr.Name, t.Algorithm, 300, time.Now().Unix())
		w.WriteMsg(r)
	}()
	if len(req.Question) != 1 {
		r.SetRcode(req, dns.RcodeFormatError)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	q := req.Question[0]
	switch req.Opcode {
	case dns.OpcodeQuery:
		s.query(r, q)
	case dns.OpcodeUpdate:
		r.Rcode = s.update(req)
	default:
		r.Rcode = dns.RcodeNotImplemented
	}
}

func (s *Server) query(r *dns.Msg, q dns.Question) {
	name := fqdn(q.Name)
	r.Authoritative = true
	switch {
	case q.Qtype == dns.TypeAXFR && name == s.Zone:
		r.Answer = append(r.Answer, dns.Copy(s.soa))
		for _, n := range s.sortedNames() {
			for _, rr := range s.rrs[n] {
				r.Answer = append(r.Answer, dns.Copy(rr))
			}
		}
		r.Answer = append(r.Answer, dns.Copy(s.soa))
	case q.Qtype == dns.TypeSOA && name == s.Zone:
		r.Answer = append(r.Answer, dns.Copy(s.soa))
	case !dns.IsSubDomain(s.Zone, name):
		r.Rcode = dns.RcodeRefused
	default:
		rrs, ok := s.rrs[name]
		if !ok {
			r.Rcode = dns.RcodeNameError
			return
		}
		for _, rr := range rrs {
			if rr.Header().Rrtype == q.Qtype || rr.Header().Rrtype == dns.TypeCNAME {
				r.Answer = append(r.Answer, dns.Copy(rr))
			}
		}
	}
}

func (s *Server) sortedNames() []string {
	var out []string
	for n := range s.rrs {
		out = append(out, n)
	}
	slices.Sort(out)

	return out
}

// update applies an RFC 2136 update, prerequisites first, all of it or
// none of it.
func (s *Server) update(req *dns.Msg) int {
	if fqdn(req.Question[0].Name) != s.Zone {
		return dns.RcodeNotAuth
	}
	for _, rr := range append(slices.Clone(req.Answer), req.Ns...) {
		if !dns.IsSubDomain(s.Zone, fqdn(rr.Header().Name)) {
			return dns.RcodeNotZone
		}
	}

	// Prerequisites (RFC 2136 §3.2).
	valueDependent := map[[2]any][]dns.RR{}
	for _, rr := range req.Answer {
		h := rr.Header()
		name := fqdn(h.Name)
		switch h.Class {
		case dns.ClassANY:
			if h.Rrtype == dns.TypeANY {
				if len(s.rrs[name]) == 0 {
					return dns.RcodeNameError
				}
			} else if !s.has(name, h.Rrtype) {
				return dns.RcodeNXRrset
			}
		case dns.ClassNONE:
			if h.Rrtype == dns.TypeANY {
				if len(s.rrs[name]) > 0 {
					return dns.RcodeYXDomain
				}
			} else if s.has(name, h.Rrtype) {
				return dns.RcodeYXRrset
			}
		case dns.ClassINET:
			k := [2]any{name, h.Rrtype}
			valueDependent[k] = append(valueDependent[k], rr)
		default:
			return dns.RcodeFormatError
		}
	}
	for k, want := range valueDependent {
		var have []dns.RR
		for _, rr := range s.rrs[k[0].(string)] {
			if rr.Header().Rrtype == k[1].(uint16) {
				have = append(have, rr)
			}
		}
		if !sameSet(have, want) {
			return dns.RcodeNXRrset
		}
	}

	// The update (RFC 2136 §3.4), on a copy that replaces the zone only
	// when all of it applied.
	next := map[string][]dns.RR{}
	for n, rrs := range s.rrs {
		next[n] = slices.Clone(rrs)
	}
	for _, rr := range req.Ns {
		h := rr.Header()
		name := fqdn(h.Name)
		switch h.Class {
		case dns.ClassINET:
			rr = dns.Copy(rr)
			rr.Header().Name = name
			next[name] = slices.DeleteFunc(next[name], func(o dns.RR) bool { return dns.IsDuplicate(o, rr) })
			next[name] = append(next[name], rr)
		case dns.ClassANY:
			if h.Rrtype == dns.TypeANY {
				delete(next, name)
			} else {
				next[name] = slices.DeleteFunc(next[name], func(o dns.RR) bool { return o.Header().Rrtype == h.Rrtype })
			}
		case dns.ClassNONE:
			c := dns.Copy(rr)
			c.Header().Class = dns.ClassINET
			next[name] = slices.DeleteFunc(next[name], func(o dns.RR) bool { return dns.IsDuplicate(o, c) })
		default:
			return dns.RcodeFormatError
		}
		if len(next[name]) == 0 {
			delete(next, name)
		}
	}
	s.rrs = next
	s.soa.Serial++
	s.updates++

	return dns.RcodeSuccess
}

func (s *Server) has(name string, t uint16) bool {
	for _, rr := range s.rrs[name] {
		if rr.Header().Rrtype == t {
			return true
		}
	}

	return false
}

func (s *Server) insert(rr dns.RR) {
	name := fqdn(rr.Header().Name)
	rr.Header().Name = name
	s.rrs[name] = slices.DeleteFunc(s.rrs[name], func(o dns.RR) bool { return dns.IsDuplicate(o, rr) })
	s.rrs[name] = append(s.rrs[name], rr)
}

// sameSet says two RRsets hold the same records, TTLs aside.
func sameSet(a, b []dns.RR) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		found := false
		for _, y := range b {
			c := dns.Copy(y)
			c.Header().Class = x.Header().Class
			c.Header().Name = x.Header().Name
			if dns.IsDuplicate(x, c) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

func fqdn(v string) string { return strings.ToLower(dns.Fqdn(v)) }
