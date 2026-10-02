package k8s_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/k8s"
	"github.com/lesomnus/shale/internal/k8s/k8stest"
)

// TestCreate is a Secret made with what it says about itself, never made
// over one that is there, and not made at all by a dry run (§34.5).
func TestCreate(t *testing.T) {
	ctx := context.Background()
	f := k8stest.New(t)
	k := f.Client()
	require.Equal(t, "shale", k.Namespace())

	ok, err := k.SecretExists(ctx, "a")
	require.NoError(t, err)
	require.False(t, ok)

	s := k8s.Secret{
		Name:        "a",
		Labels:      map[string]string{"shale.io/x": "true"},
		Annotations: map[string]string{"kubernetes.io/description": "read me"},
		Data:        map[string][]byte{"cluster.ops": []byte("pw")},
	}
	require.NoError(t, k.Create(ctx, s, true))
	_, ok = f.Get("a")
	require.False(t, ok, "a dry run writes nothing")

	require.NoError(t, k.Create(ctx, s, false))
	got, ok := f.Get("a")
	require.True(t, ok)
	require.Equal(t, s.Labels, got.Labels)
	require.Equal(t, s.Annotations, got.Annotations)
	require.Equal(t, []byte("pw"), got.Data["cluster.ops"])
	ok, err = k.SecretExists(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)

	// Never over one that is there, dry or not.
	s.Data = map[string][]byte{"cluster.ops": []byte("other")}
	require.ErrorIs(t, k.Create(ctx, s, true), k8s.ErrExists)
	require.ErrorIs(t, k.Create(ctx, s, false), k8s.ErrExists)
	require.ErrorIs(t, k.CreateSecret(ctx, "a", s.Data), k8s.ErrExists)
	got, _ = f.Get("a")
	require.Equal(t, []byte("pw"), got.Data["cluster.ops"])
	require.Equal(t, 1, f.Posts("a"))

	// What the API server refuses is an error, and not ErrExists.
	f.Refuse("b", http.StatusForbidden, true)
	err = k.Create(ctx, k8s.Secret{Name: "b"}, true)
	require.Error(t, err)
	require.NotErrorIs(t, err, k8s.ErrExists)
	require.Contains(t, err.Error(), "403")
}
