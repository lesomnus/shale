//go:build linux

package discovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSystemNeighborsExecutesBoundedReadOnlyExactInterfaceQuery(t *testing.T) {
	command := filepath.Join(t.TempDir(), "ip")
	script := "#!/bin/sh\n[ \"$*\" = '-j neighbor show dev camera0' ] || exit 9\nprintf '%s' '[{\"dst\":\"192.0.2.10\",\"lladdr\":\"02:00:00:00:00:01\",\"state\":[\"REACHABLE\"],\"unused\":true}]'\n"
	require.NoError(t, os.WriteFile(command, []byte(script), 0700))
	out, err := (SystemNeighbors{IPCommand: command}).Neighbors(t.Context(), "camera0")
	require.NoError(t, err)
	require.Equal(t, []Neighbor{{Address: "192.0.2.10", MAC: "02:00:00:00:00:01", States: []string{"REACHABLE"}}}, out)
	_, err = (SystemNeighbors{IPCommand: command}).Neighbors(t.Context(), "camera0;echo invalid")
	require.Error(t, err)
	var b neighborBuffer
	_, err = b.Write(make([]byte, (128<<10)+1))
	require.Error(t, err)
	require.Zero(t, b.Len())
	require.NoError(t, os.WriteFile(command, []byte("#!/bin/sh\necho private-error >&2\nexit 3\n"), 0700))
	_, err = (SystemNeighbors{IPCommand: command}).Neighbors(t.Context(), "camera0")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private-error")
}
