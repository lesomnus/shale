// Package credential supplies private credential storage and diagnostic redaction.
// Enrollment configuration contains references; only a provider's private store
// contains credential material.
package credential

import (
	"net/url"
	"regexp"
	"strings"
)

var uriPattern = regexp.MustCompile(`(?i)[a-z][a-z0-9+.-]*://[^\s<>"']+`)
var sensitiveQuery = map[string]bool{"password": true, "passwd": true, "pass": true, "token": true, "access_token": true, "secret": true, "authorization": true}

// Redact removes userinfo and sensitive query values from URLs in diagnostics,
// including legacy manually configured RTSP inputs.
func Redact(text string) string {
	return uriPattern.ReplaceAllStringFunc(text, func(raw string) string {
		u, err := url.Parse(raw)
		if err != nil {
			if i := strings.LastIndex(raw, "@"); i >= 0 {
				if start := strings.Index(raw, "://"); start >= 0 && i > start {
					return raw[:start+3] + "[redacted]@" + raw[i+1:]
				}
			}
			return "[invalid URL]"
		}
		if u.User != nil {
			u.User = url.User("redacted")
		}
		q := u.Query()
		changed := false
		for k := range q {
			if sensitiveQuery[strings.ToLower(k)] {
				q.Set(k, "[redacted]")
				changed = true
			}
		}
		if changed {
			u.RawQuery = q.Encode()
		}
		return u.String()
	})
}

// Sanitizer also removes material that a subprocess may quote outside its URL.
// Do not persist a Sanitizer: it holds short-lived material until the operation
// ends. Empty values are ignored.
type Sanitizer struct{ values []string }

func NewSanitizer(values ...string) Sanitizer {
	var out []string
	for _, v := range values {
		if v != "" {
			out = append(out, v, url.PathEscape(v), url.QueryEscape(v), url.UserPassword("", v).String()[1:])
		}
	}
	return Sanitizer{values: out}
}
func (s Sanitizer) Redact(text string) string {
	text = Redact(text)
	for _, v := range s.values {
		text = strings.ReplaceAll(text, v, "[redacted]")
	}
	return text
}

// InputSanitizer includes legacy URL material which external tools sometimes
// repeat without its enclosing URL.
func InputSanitizer(input string) Sanitizer {
	u, err := url.Parse(input)
	if err != nil || u.User == nil {
		return NewSanitizer()
	}
	values := []string{u.User.Username()}
	if p, ok := u.User.Password(); ok {
		values = append(values, p)
	}
	return NewSanitizer(values...)
}
