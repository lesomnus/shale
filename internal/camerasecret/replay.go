package camerasecret

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"io"
)

// ReplayTag is a keyed comparison value, never an unkeyed password digest.
// Persist it explicitly and compare with Equal. It is not a credential or a
// grant. The zero tag is invalid, including when compared with another zero.
type ReplayTag struct {
	sum   [sha256.Size]byte
	valid bool
}

func (t ReplayTag) Equal(other ReplayTag) bool {
	return t.valid && other.valid && hmac.Equal(t.sum[:], other.sum[:])
}

func (t ReplayTag) MarshalBinary() ([]byte, error) {
	if !t.valid {
		return nil, ErrReplayTag
	}
	return append([]byte{wireVersion}, t.sum[:]...), nil
}

func ParseReplayTag(b []byte) (ReplayTag, error) {
	if len(b) != sha256.Size+1 || b[0] != wireVersion {
		return ReplayTag{}, ErrReplayTag
	}
	t := ReplayTag{valid: true}
	copy(t.sum[:], b[1:])
	return t, nil
}

func (ReplayTag) Format(s fmt.State, _ rune)           { _, _ = io.WriteString(s, redactedReplay) }
func (ReplayTag) String() string                       { return redactedReplay }
func (ReplayTag) GoString() string                     { return redactedReplay }
func (ReplayTag) MarshalJSON() ([]byte, error)         { return []byte(`"` + redactedReplay + `"`), nil }
func (ReplayTag) MarshalYAML() (any, error)            { return redactedReplay, nil }
func (*ReplayTag) UnmarshalJSON([]byte) error          { return ErrReplayTag }
func (*ReplayTag) UnmarshalYAML(func(any) error) error { return ErrReplayTag }
