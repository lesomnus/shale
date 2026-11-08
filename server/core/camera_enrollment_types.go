package core

import (
	"context"

	"github.com/lesomnus/shale/api"
	"github.com/protobuf-orm/ent/dialect"
)

// CameraEnrollmentPreparation is issued only after the caller's current network
// assignment and the immutable policy have been authorized. Server is the
// recorded private stack bound to Driver's existing transaction. An adapter
// must not capture a pool connection, perform camera I/O or let a reference
// escape before the enclosing transaction commits.
type CameraEnrollmentPreparation struct {
	Server          api.Server
	Driver          dialect.Driver
	Assignment      *api.CameraAssignment
	Device          *api.CameraDevice
	Binding         *api.CameraBinding
	Policy          *api.CameraPolicy
	IntentKey       string
	InitialUsername string
	AllowActivation bool
}

// CameraEnrollmentCredentials contains durable references only. Their purpose
// is a delivery boundary; it does not assert separate physical camera accounts.
type CameraEnrollmentCredentials struct {
	Initial    *api.CameraCredentialReference
	Management *api.CameraCredentialReference
	Streaming  *api.CameraCredentialReference
}

func (s Core) cameraEnrollmentDriver(ctx context.Context) dialect.Driver {
	if bound, ok := ctx.Value(txKey{}).(txBound); ok {
		return bound.drv
	}
	return s.driver()
}
