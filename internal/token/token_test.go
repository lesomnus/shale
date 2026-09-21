package token

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
)

func TestRoundTrip(t *testing.T) {
	x := require.New(t)

	k, err := Generate("k1")
	x.NoError(err)

	exp := time.Now().Add(time.Hour)
	s, err := k.Sign(api.TokenClaims_builder{
		Exp:       timestamppb.New(exp),
		Aud:       []byte("node-a"),
		Op:        api.TokenOp_TOKEN_OP_PUT,
		LaminaKey: "laminae/2026/09/20/00/x",
		MaxLength: 123,
	}.Build())
	x.NoError(err)

	v := NewVerifier()
	v.Add("k1", k.Public())

	c, err := v.Verify(s)
	x.NoError(err)
	x.Equal("k1", c.GetKid())
	x.Equal(int64(123), c.GetMaxLength())
	x.Equal("laminae/2026/09/20/00/x", c.GetLaminaKey())

	x.NoError(Check(c, []byte("node-a"), api.TokenOp_TOKEN_OP_PUT))
	x.ErrorIs(Check(c, []byte("node-b"), api.TokenOp_TOKEN_OP_PUT), ErrAudience)
	x.ErrorIs(Check(c, []byte("node-a"), api.TokenOp_TOKEN_OP_GET), ErrOp)
	x.NoError(Check(c, []byte("node-a"), api.TokenOp_TOKEN_OP_GET, api.TokenOp_TOKEN_OP_PUT))
}

func TestRefusals(t *testing.T) {
	x := require.New(t)

	k, err := Generate("k1")
	x.NoError(err)
	other, err := Generate("k1")
	x.NoError(err)

	s, err := k.Sign(api.TokenClaims_builder{
		Exp: timestamppb.New(time.Now().Add(time.Hour)),
		Aud: []byte("n"),
	}.Build())
	x.NoError(err)

	v := NewVerifier()
	_, err = v.Verify(s)
	x.ErrorIs(err, ErrUnknownKey)

	// The same kid, another key: the signature does not check out.
	v.Add("k1", other.Public())
	_, err = v.Verify(s)
	x.ErrorIs(err, ErrSignature)

	v.Add("k1", k.Public())
	_, err = v.Verify(s)
	x.NoError(err)

	// Tampering with the claims breaks the signature.
	_, err = v.Verify("A" + s[1:])
	x.Error(err)

	_, err = v.Verify("nodot")
	x.ErrorIs(err, ErrMalformed)

	// Expiry, with skew.
	old, err := k.Sign(api.TokenClaims_builder{
		Exp: timestamppb.New(time.Now().Add(-30 * time.Second)),
		Aud: []byte("n"),
	}.Build())
	x.NoError(err)
	_, err = v.Verify(old)
	x.NoError(err, "within the skew")

	v.Skew = 0
	_, err = v.Verify(old)
	x.ErrorIs(err, ErrExpired)
}

func TestFromHeader(t *testing.T) {
	x := require.New(t)
	x.Equal("abc", FromHeader("Shale abc"))
	x.Equal("abc", FromHeader("shale   abc "))
	x.Equal("", FromHeader("Bearer abc"))
	x.Equal("", FromHeader(""))
}
