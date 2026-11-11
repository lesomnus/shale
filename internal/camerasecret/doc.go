// Package camerasecret protects opaque camera credential payloads at rest.
//
// A Context identifies an immutable credential version. It deliberately omits
// the Worker, Producer, assignment, network, site and lease generation: those
// determine who may receive a credential now, not which secret was encrypted.
// The caller must authorize every operation before calling a Backend. These
// primitives confer no delivery grant and make no camera or database changes.
//
// The AES backend derives independent encryption and import replay keys from
// the CP's 32-byte KEK. It does not use or change the signing-key wrap format.
// Persist envelopes and replay tags only through their explicit binary methods;
// their ordinary formatting, JSON and YAML representations are redacted.
package camerasecret
