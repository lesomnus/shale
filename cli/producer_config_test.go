package cli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cmd"
)

// producer.fragment_duration reaches every source, the demo's included.
func TestProducerConfigFragment(t *testing.T) {
	var c cmd.Config
	c.Producer.FragmentDuration = 200 * time.Millisecond
	c.Producer.Demo = 1
	c.Producer.Sources = []cmd.SourceConfig{{Alias: "door", Input: "v4l2:/dev/video0", MaxBitrate: "2Mbps"}}
	cfg, err := ProducerConfig(&c)
	require.NoError(t, err)
	require.Len(t, cfg.Sources, 2)
	for _, s := range cfg.Sources {
		require.Equal(t, 200*time.Millisecond, s.Fragment, s.Alias)
	}
}
