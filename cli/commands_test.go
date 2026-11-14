package cli

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cmd"
)

// A command is found by its name, so a second of the same name could never
// run: each group's verbs are in the tree of the one surface that serves
// them (§32).
func TestEveryCommandNameIsReachable(t *testing.T) {
	var c cmd.Config
	seen := map[string]int{}
	for _, v := range Cmd(&c).Commands {
		seen[v.Name]++
	}
	for name, n := range seen {
		require.Equal(t, 1, n, "`shale %s` is listed %d times", name, n)
	}
}
