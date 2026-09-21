package cmd_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/lesomnus/shale/cmd"
)

// A source's controls are read as text whatever the YAML scalar was, so
// `exposure_dynamic_framerate: 0` and `power_line_frequency: "1"` both
// land as the values v4l2-ctl would take (§38.3).
func TestSourceControlsDecode(t *testing.T) {
	var c cmd.Config
	require.NoError(t, yaml.Unmarshal([]byte(`
producer:
  sources:
    - alias: a
      input: v4l2:/dev/video0
      controls: {exposure_dynamic_framerate: 0, power_line_frequency: "1", focus_automatic_continuous: false}
`), &c))
	require.Equal(t, map[string]string{"exposure_dynamic_framerate": "0", "power_line_frequency": "1", "focus_automatic_continuous": "false"}, c.Producer.Sources[0].Controls)
}
