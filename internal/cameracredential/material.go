package cameracredential

import "github.com/lesomnus/shale/camera/credential/material"

// Material is shared with CP consumers without pulling CP database code into
// the camera client. Binary serialization is an explicit plaintext boundary.
type Material = material.Material

func NewMaterial(username, password string) (Material, error) {
	return material.New(username, password)
}
func ParseMaterial(b []byte) (Material, error) { return material.Parse(b) }

// V1 binary marker, also pinned by the public codec and compatibility tests.
const materialMagic = "SHCAMMAT\x01"
