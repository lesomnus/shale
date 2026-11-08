// Package credential supplies private credential storage and diagnostic redaction.
// Enrollment configuration contains references; only a provider's private store
// contains credential material.
package credential

import (
	"net/url"
	"regexp"
	"sort"
	"strings"
)

var uriPattern = regexp.MustCompile(`(?i)[a-z][a-z0-9+.-]*://[^\s<>"']+`)

// SensitiveQueryKey matches the credential-bearing query keys rejected by a
// public source catalog. It also covers vendor-prefixed authentication keys.
func SensitiveQueryKey(key string) bool {
	k := strings.ToLower(key)
	for _, word := range []string{"password", "passwd", "secret", "token", "credential", "auth"} {
		if strings.Contains(k, word) {
			return true
		}
	}
	return k == "user" || k == "username" || k == "pwd" || k == "pass" || k == "apikey" || k == "api_key" || k == "sig" || k == "signature"
}

// Redact removes userinfo and sensitive query values from URLs in diagnostics,
// including legacy manually configured RTSP inputs.
func Redact(text string) string {
	return uriPattern.ReplaceAllStringFunc(text, func(raw string) string {
		u, err := url.Parse(raw)
		if err != nil {
			// Keeping only a malformed URL's tail could retain its query
			// credentials even after the user information was removed.
			return "[invalid URL]"
		}
		if u.User != nil {
			u.User = url.User("redacted")
		}
		q, queryErr := url.ParseQuery(u.RawQuery)
		changed := false
		for k := range q {
			if SensitiveQueryKey(k) {
				q.Set(k, "[redacted]")
				changed = true
			}
		}
		if queryErr != nil {
			// Invalid escaping must not preserve a query which could contain
			// a partial credential that ParseQuery could not decode.
			u.RawQuery = "[redacted]"
		} else if changed {
			u.RawQuery = q.Encode()
		}
		if u.Fragment != "" {
			u.Fragment = "[redacted]"
		}
		return u.String()
	})
}

// Sanitizer also removes material that a subprocess may quote outside its URL.
// Do not persist a Sanitizer: it holds short-lived material until the operation
// ends. Empty values are ignored.
type Sanitizer struct {
	pattern   *regexp.Regexp
	redactAll bool
}

func NewSanitizer(values ...string) Sanitizer {
	unique := map[string]bool{}
	for _, v := range values {
		if v != "" {
			for _, variant := range []string{v, url.PathEscape(v), url.QueryEscape(v), url.UserPassword("", v).String()[1:]} {
				unique[variant] = true
			}
		}
	}
	if len(unique) == 0 {
		return Sanitizer{}
	}
	variants := make([]string, 0, len(unique))
	for v := range unique {
		variants = append(variants, v)
	}
	// Prefer a complete token over a username which happens to be its prefix.
	sort.Slice(variants, func(i, j int) bool {
		if len(variants[i]) != len(variants[j]) {
			return len(variants[i]) > len(variants[j])
		}
		return variants[i] < variants[j]
	})
	patterns := make([]string, len(variants))
	size := 0
	for i, v := range variants {
		patterns[i] = percentPattern(v)
		size += len(patterns[i]) + 1
		if size > 256<<10 {
			return Sanitizer{redactAll: true}
		}
	}
	pattern, err := regexp.Compile(strings.Join(patterns, "|"))
	if err != nil {
		// Percent-decoded credentials may contain arbitrary octets. A regex
		// which cannot represent them must hide the entire diagnostic rather
		// than panic or leave a partially redacted secret behind.
		return Sanitizer{redactAll: true}
	}
	return Sanitizer{pattern: pattern}
}

// Percent escapes are case insensitive; the rest of a credential is not.
func percentPattern(v string) string {
	var b strings.Builder
	start := 0
	for i := 0; i+2 < len(v); i++ {
		if v[i] != '%' || !hexDigit(v[i+1]) || !hexDigit(v[i+2]) {
			continue
		}
		b.WriteString(regexp.QuoteMeta(v[start:i]))
		b.WriteByte('%')
		for _, c := range []byte{v[i+1], v[i+2]} {
			if c >= 'a' && c <= 'f' {
				b.WriteString("[" + string(c) + string(c-'a'+'A') + "]")
			} else if c >= 'A' && c <= 'F' {
				b.WriteString("[" + string(c) + string(c-'A'+'a') + "]")
			} else {
				b.WriteByte(c)
			}
		}
		i += 2
		start = i + 1
	}
	b.WriteString(regexp.QuoteMeta(v[start:]))
	return b.String()
}

func hexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

func (s Sanitizer) Redact(text string) string {
	if s.redactAll {
		return "[redacted]"
	}
	text = Redact(text)
	if s.pattern != nil {
		text = s.pattern.ReplaceAllString(text, "[redacted]")
	}
	return text
}

// InputSanitizer includes legacy URL material which external tools sometimes
// repeat without its enclosing URL.
func InputSanitizer(input string) Sanitizer {
	u, err := url.Parse(input)
	if err != nil {
		return NewSanitizer()
	}
	var values []string
	if u.User != nil {
		values = append(values, u.User.Username())
		if p, ok := u.User.Password(); ok {
			values = append(values, p)
		}
		// Retain exact raw spellings, including over-encoded credentials.
		if scheme := strings.Index(input, "://"); scheme >= 0 {
			authority := input[scheme+3:]
			if end := strings.IndexAny(authority, "/?#"); end >= 0 {
				authority = authority[:end]
			}
			if at := strings.LastIndex(authority, "@"); at >= 0 {
				rawUser := authority[:at]
				user, password, _ := strings.Cut(rawUser, ":")
				values = append(values, rawUser, user, password)
			}
		}
	}
	for _, field := range strings.Split(u.RawQuery, "&") {
		key, raw, _ := strings.Cut(field, "=")
		key, err = url.QueryUnescape(key)
		if err != nil || !SensitiveQueryKey(key) {
			continue
		}
		values = append(values, raw)
		if decoded, err := url.QueryUnescape(raw); err == nil {
			values = append(values, decoded)
		}
	}
	values = append(values, u.Fragment, u.RawFragment)
	return NewSanitizer(values...)
}
