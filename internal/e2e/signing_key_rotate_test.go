package e2e_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
)

// `signing-key rotate` (§33.3) answers the key it made without its
// private half: `private_key` is a secret of the row, the key as stored
// under the KEK, and the answer is no read of it -- as a CA's Rotate
// answers none of its own. Plain and `immediate` alike, over the wire and
// as the command prints it; the stored row has it all the same.
func TestSigningKeyRotateAnswersNoPrivateKey(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	keys := api.NewSigningKeyServiceClient(c.dialCluster(c.operator()))

	for _, immediate := range []bool{false, true} {
		v, err := keys.Rotate(ctx, api.SigningKeyRotateRequest_builder{Immediate: immediate}.Build())
		require.NoError(t, err)
		require.NotEmpty(t, v.GetId())
		require.NotEmpty(t, v.GetPublicKey())
		require.Empty(t, v.GetPrivateKey(), "immediate=%v", immediate)

		stored, err := c.running.CP.Ungated.SigningKey().Get(ctx, api.SigningKeyGetRequest_builder{
			Ref:    api.SigningKeyRef_builder{Id: v.GetId()}.Build(),
			Select: api.SigningKeySelect_builder{All: z.Ptr(true)}.Build(),
		}.Build())
		require.NoError(t, err)
		require.NotEmpty(t, stored.GetPrivateKey(), "what is stored is the key, under the KEK")
	}

	for _, args := range [][]string{{"signing-key", "rotate"}, {"signing-key", "rotate", "-o", "json"}, {"signing-key", "rotate", "-o", "protojson", `{"immediate": true}`}} {
		var out bytes.Buffer
		x := cli.Cmd(&cmd.Config{})
		x.Writer = &out
		x.ErrWriter = io.Discard
		err := x.Run(ctx, append([]string{"--cluster-addr", "http://" + c.running.ClusterAddr, "--as", c.operator()}, args...))
		require.NoError(t, err, "%v", args)
		require.NotEmpty(t, out.String())
		for _, w := range []string{"private_key", "privateKey"} {
			require.False(t, strings.Contains(out.String(), w), "%v prints %s:\n%s", args, w, out.String())
		}
	}
}
