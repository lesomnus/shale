package token

import "testing"

// A token is parsed and verified without panicking, whatever it is (§57).
func FuzzParse(f *testing.F) {
	k, _ := Generate("k-fuzz")
	v := NewVerifier()
	v.Add("k-fuzz", k.Public())
	f.Add("")
	f.Add("a.b")
	f.Add("AAAA.BBBB")
	f.Fuzz(func(t *testing.T, s string) {
		Parse(s)
		v.Verify(s)
	})
}
