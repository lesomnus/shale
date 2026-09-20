// Package storage is the Storage Node (§22-§25): sinks, the self-describing
// object files in them, the HTTP data plane producers and readers use, the
// in-memory index, the events it pushes to the Control Plane, and the
// control API the Control Plane calls.
package storage

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/shale/api"
)

// XattrName is where an object file carries its record (§23.1).
const XattrName = "user.shale"

// RecordMax is the hard limit on the encoded record: XFS keeps an attribute
// inline only while its value fits in one byte of length.
const RecordMax = 255

// FormatVersion is the record format this node writes.
const FormatVersion = 1

var ErrNoRecord = errors.New("storage: no record")

// EncodeRecord marshals a record and refuses one over the limit.
func EncodeRecord(r *api.ObjectRecord) ([]byte, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(b) > RecordMax {
		return nil, fmt.Errorf("storage: record is %d bytes, over the %d-byte limit", len(b), RecordMax)
	}

	return b, nil
}

// WriteRecord sets the record on an open file.
func WriteRecord(f *os.File, r *api.ObjectRecord) error {
	b, err := EncodeRecord(r)
	if err != nil {
		return err
	}

	return unix.Fsetxattr(int(f.Fd()), XattrName, b, 0)
}

// ReadRecord reads the record of an open file.
func ReadRecord(f *os.File) (*api.ObjectRecord, error) {
	buf := make([]byte, RecordMax+1)
	n, err := unix.Fgetxattr(int(f.Fd()), XattrName, buf)
	if err != nil {
		if errors.Is(err, unix.ENODATA) {
			return nil, ErrNoRecord
		}

		return nil, err
	}

	return decode(buf[:n])
}

// ReadRecordPath reads the record by path.
func ReadRecordPath(path string) (*api.ObjectRecord, error) {
	buf := make([]byte, RecordMax+1)
	n, err := unix.Getxattr(path, XattrName, buf)
	if err != nil {
		if errors.Is(err, unix.ENODATA) {
			return nil, ErrNoRecord
		}

		return nil, err
	}

	return decode(buf[:n])
}

// WriteRecordPath sets the record by path.
func WriteRecordPath(path string, r *api.ObjectRecord) error {
	b, err := EncodeRecord(r)
	if err != nil {
		return err
	}

	return unix.Setxattr(path, XattrName, b, 0)
}

func decode(b []byte) (*api.ObjectRecord, error) {
	var r api.ObjectRecord
	// A newer format than this node knows is read for the fields it knows;
	// unknown fields are kept by proto (§23.1).
	if err := proto.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("storage: record: %w", err)
	}

	return &r, nil
}

// RecordFromToken is the record a put token carries, filled with what the
// node knows at open (§23.1).
func RecordFromToken(c *api.TokenClaims, sizeHint int64) *api.ObjectRecord {
	r := proto.Clone(c.GetRecord()).(*api.ObjectRecord)
	if r == nil {
		r = &api.ObjectRecord{}
	}
	r.SetFormatVersion(FormatVersion)
	r.SetState(api.RecordState_RECORD_STATE_OPEN)
	r.SetSizeHint(sizeHint)
	if c.GetMode() != api.UploadMode_UPLOAD_MODE_UNSPECIFIED {
		r.SetMode(c.GetMode())
	}
	if c.GetAbandonTimeoutSeconds() > 0 {
		r.SetAbandonTimeoutSeconds(c.GetAbandonTimeoutSeconds())
	}
	if len(r.GetAttemptId()) == 0 {
		r.SetAttemptId(c.GetAttemptId())
	}

	return r
}
