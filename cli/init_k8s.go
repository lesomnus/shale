package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lesomnus/shale/internal/k8s"
)

// PasswordSecretSuffix names the Secret of the first passwords after the
// one of the state: `shale-control` and `shale-control-passwords` (§34.5).
const PasswordSecretSuffix = "-passwords"

// The Secret of the first passwords says what it is to whoever finds it.
const (
	// PasswordSecretLabel marks it, for `kubectl get secret -l`.
	PasswordSecretLabel = "shale.io/first-passwords"
	// PasswordSecretHolders names who each key is.
	PasswordSecretHolders = "shale.io/holders"
	// PasswordSecretNote is what to do with it.
	PasswordSecretNote = "kubernetes.io/description"
)

// PasswordKey is the key of a person's password in that Secret:
// `<tenant>.<alias>`, since a key is `[-._a-zA-Z0-9]+` and has no room for
// `@tenant/alias`. The annotation PasswordSecretHolders says it again in
// full, and an alias no key can carry is refused by the dry run, before
// init makes anything.
func PasswordKey(p Password) string { return p.Tenant + "." + p.Alias }

// secretStash keeps the first passwords in a Secret of their own, beside
// the state's (§34.5): a pod's output is its log, which is read by
// whatever collects logs and kept for as long as that keeps them.
type secretStash struct {
	k    *k8s.Client
	name string
}

func (s *secretStash) secret(ps []Password) k8s.Secret {
	data := map[string][]byte{}
	holders := make([]string, 0, len(ps))
	for _, p := range ps {
		data[PasswordKey(p)] = []byte(p.Secret)
		holders = append(holders, PasswordKey(p)+"=@"+p.Tenant+"/"+p.Alias)
	}

	return k8s.Secret{
		Name:   s.name,
		Labels: map[string]string{PasswordSecretLabel: "true"},
		Annotations: map[string]string{
			PasswordSecretHolders: strings.Join(holders, " "),
			PasswordSecretNote: "The first passwords `shale init` made, one key per person (<tenant>.<alias>), and nowhere else. " +
				"Read them, keep them where passwords are kept, then delete this Secret: nothing reads it. " +
				"`shale holder issue-password` gives somebody a new password and does not change this Secret.",
		},
		Data: data,
	}
}

// check asks the API server whether the Secret could be made, with nothing
// written: a Secret by that name, or a role that does not allow it, stops
// init before there is anything to lose.
func (s *secretStash) check(ctx context.Context, ps []Password) error {
	err := s.k.Create(ctx, s.secret(ps), true)
	if errors.Is(err, k8s.ErrExists) {
		return fmt.Errorf("%w: it holds the first passwords of an init that did not finish, or of an earlier deployment, and is never overwritten; keep what you need from it, delete it, and run init again", err)
	}
	if err != nil {
		return fmt.Errorf("the secret for the first passwords: %w", err)
	}

	return nil
}

func (s *secretStash) Put(ctx context.Context, ps []Password) error {
	return s.k.Create(ctx, s.secret(ps), false)
}

func (s *secretStash) Where(p Password) string {
	return fmt.Sprintf("in secret %s/%s, key %s", s.k.Namespace(), s.name, PasswordKey(p))
}

func (s *secretStash) Read(p Password) string {
	return fmt.Sprintf("the passwords are not printed: secret %s/%s holds them, one key each; read one with\n\n"+
		"  kubectl -n %s get secret %s -o jsonpath='{.data.%s}' | base64 -d\n\n"+
		"keep them where passwords are kept, then delete the secret: nothing reads it",
		s.k.Namespace(), s.name, s.k.Namespace(), s.name, strings.ReplaceAll(PasswordKey(p), ".", `\.`))
}
