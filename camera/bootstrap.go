package camera

import "regexp"

// BootstrapSettings explicitly selects an installed compatibility profile and
// its web transport. The device address comes from separately authorized fresh
// discovery. These settings confer no activation or credential authority.
type BootstrapSettings struct {
	CompatibilityProfile string `json:"compatibility_profile" yaml:"compatibility_profile"`
	WebScheme            string `json:"web_scheme" yaml:"web_scheme"`
	WebPort              int    `json:"web_port" yaml:"web_port"`
}

var bootstrapProfileToken = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,127}$`)

// Validate checks portable syntax only. Driver availability is determined by
// the executing Worker; transport fields are never inferred or defaulted.
func (s BootstrapSettings) Validate() error {
	if !bootstrapProfileToken.MatchString(s.CompatibilityProfile) || (s.WebScheme != "http" && s.WebScheme != "https") || s.WebPort < 1 || s.WebPort > 65535 {
		return ErrInvalid
	}
	return nil
}
