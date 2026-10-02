package cli

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/k8s"
	"github.com/lesomnus/shale/internal/k8s/k8stest"
)

// initIn runs `shale init` with args against a fake API server, as the
// init Job does (§34.5): the state directory is scratch, and roster is on
// a database of its own outside it.
func initIn(t *testing.T, f *k8stest.Server, args ...string) (*cmd.Config, string, error) {
	t.Helper()
	if f != nil {
		prev := inCluster
		inCluster = func() (*k8s.Client, bool) { return f.Client(), true }
		t.Cleanup(func() { inCluster = prev })
	}
	c := &cmd.Config{}
	ApplyDev(c, t.TempDir())
	c.Auth.Roster.Db.Driver = "sqlite3"
	c.Auth.Roster.Db.Dsn = "file:" + filepath.Join(t.TempDir(), "roster.db") + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"

	var out bytes.Buffer
	x := NewCmdInit(c)
	x.Writer = &out
	x.ErrWriter = &out
	err := x.Run(context.Background(), args)

	return c, out.String(), err
}

var printedPassword = regexp.MustCompile(`(?m)^(operator|admin) .*password: (\S+)$`)

// TestInitK8sPasswords is the first passwords in a Secret of their own
// instead of the Job's log (§34.5): a pod's output is read by whatever
// collects logs, for as long as it keeps them, and is gone with the pod.
func TestInitK8sPasswords(t *testing.T) {
	f := k8stest.New(t)
	_, out, err := initIn(t, f, "--k8s-secret", "shale-control")
	require.NoError(t, err, out)

	state, ok := f.Get("shale-control")
	require.True(t, ok)
	require.Contains(t, state.Data, "kek")
	for k := range state.Data {
		require.NotContains(t, k, ".ops")
	}

	pw, ok := f.Get("shale-control-passwords")
	require.True(t, ok)
	require.Len(t, pw.Data, 2)
	ops, adm := string(pw.Data["cluster.ops"]), string(pw.Data["acme.admin"])
	require.NotEmpty(t, ops)
	require.NotEmpty(t, adm)
	require.NotEqual(t, ops, adm)
	require.Equal(t, "true", pw.Labels[PasswordSecretLabel])
	require.Equal(t, "cluster.ops=@cluster/ops acme.admin=@acme/admin", pw.Annotations[PasswordSecretHolders])
	require.Contains(t, pw.Annotations[PasswordSecretNote], "delete this Secret")
	require.Contains(t, pw.Annotations[PasswordSecretNote], "shale holder issue-password")

	// The output says where, and not what.
	require.NotContains(t, out, ops)
	require.NotContains(t, out, adm)
	require.NotContains(t, out, "WARNING")
	require.Regexp(t, `(?m)^ca          \S+$`, out)
	require.Regexp(t, `(?m)^signing key \S+$`, out)
	require.Contains(t, out, "password: in secret shale/shale-control-passwords, key cluster.ops\n")
	require.Contains(t, out, "password: in secret shale/shale-control-passwords, key acme.admin\n")
	require.Contains(t, out, `kubectl -n shale get secret shale-control-passwords -o jsonpath='{.data.acme\.admin}' | base64 -d`)

	// Run again it does nothing: the state's Secret is what "initialized"
	// means, and neither is touched.
	_, out, err = initIn(t, f, "--if-needed", "--k8s-secret", "shale-control")
	require.NoError(t, err)
	require.Contains(t, out, "already initialized")
	require.Equal(t, 1, f.Posts("shale-control-passwords"))
	again, _ := f.Get("shale-control-passwords")
	require.Equal(t, pw.Data, again.Data)
}

// TestInitK8sPasswordSecretName is the Secret named on the command line.
func TestInitK8sPasswordSecretName(t *testing.T) {
	f := k8stest.New(t)
	_, out, err := initIn(t, f, "--k8s-secret", "state", "--k8s-password-secret", "first", "--tenant", "hday", "--operator", "root")
	require.NoError(t, err, out)
	pw, ok := f.Get("first")
	require.True(t, ok)
	require.Contains(t, pw.Data, "cluster.root")
	require.Contains(t, pw.Data, "hday.admin")
	_, ok = f.Get("state-passwords")
	require.False(t, ok)
	require.Contains(t, out, "kubectl -n shale get secret first ")
}

// TestInitK8sPasswordSecretThere is one left from before: never
// overwritten, and init stops before it makes anything, so nothing is lost.
func TestInitK8sPasswordSecretThere(t *testing.T) {
	f := k8stest.New(t)
	old := k8stest.Secret{Name: "shale-control-passwords", Data: map[string][]byte{"cluster.ops": []byte("old")}}
	f.Put(old)

	c, out, err := initIn(t, f, "--if-needed", "--k8s-secret", "shale-control")
	require.ErrorIs(t, err, k8s.ErrExists)
	require.Contains(t, err.Error(), "never overwritten")
	require.NotContains(t, out, "password")

	got, _ := f.Get("shale-control-passwords")
	require.Equal(t, old.Data, got.Data)
	require.Zero(t, f.Posts("shale-control-passwords"))
	_, ok := f.Get("shale-control")
	require.False(t, ok)
	_, err = os.Stat(filepath.Join(c.StateDir("control"), "kek"))
	require.ErrorIs(t, err, os.ErrNotExist, "nothing made")
}

// TestInitK8sPasswordSecretForbidden is a role that does not allow it:
// asked before anything is made, so init fails and the Job retries.
func TestInitK8sPasswordSecretForbidden(t *testing.T) {
	f := k8stest.New(t)
	f.Refuse("shale-control-passwords", http.StatusForbidden, true)

	c, out, err := initIn(t, f, "--k8s-secret", "shale-control")
	require.Error(t, err)
	require.Contains(t, err.Error(), "403")
	require.NotContains(t, out, "password")
	_, ok := f.Get("shale-control")
	require.False(t, ok)
	_, err = os.Stat(filepath.Join(c.StateDir("control"), "kek"))
	require.ErrorIs(t, err, os.ErrNotExist, "nothing made")
}

// TestInitK8sPasswordSecretFails is the Secret refused after the people
// were made. A rerun cannot make them again, so failing would leave a
// deployment nobody can sign in to: the passwords are printed after all,
// under a warning, and the state's Secret is still made.
func TestInitK8sPasswordSecretFails(t *testing.T) {
	f := k8stest.New(t)
	f.Refuse("shale-control-passwords", http.StatusInternalServerError, false)

	_, out, err := initIn(t, f, "--k8s-secret", "shale-control")
	require.NoError(t, err, out)
	require.Regexp(t, `(?m)^WARNING: the passwords could not be kept where they belong: .*500`, out)
	require.Contains(t, out, "shale holder issue-password")
	m := printedPassword.FindAllStringSubmatch(out, -1)
	require.Len(t, m, 2, out)
	for _, v := range m {
		require.NotEqual(t, "in", v[2])
		require.Len(t, v[2], len(m[0][2]))
	}
	require.Contains(t, out, "the passwords are printed once")
	_, ok := f.Get("shale-control-passwords")
	require.False(t, ok)
	_, ok = f.Get("shale-control")
	require.True(t, ok)
}

// TestInitPrintsPasswords is init outside a cluster, unchanged: the
// passwords printed once.
func TestInitPrintsPasswords(t *testing.T) {
	_, out, err := initIn(t, nil)
	require.NoError(t, err, out)
	m := printedPassword.FindAllStringSubmatch(out, -1)
	require.Len(t, m, 2, out)
	require.NotEqual(t, m[0][2], m[1][2])
	require.NotContains(t, out, "secret")
	require.NotContains(t, out, "WARNING")
	require.Contains(t, out, "\nthe passwords are printed once; sign in with `shale login @acme/admin`")
}
