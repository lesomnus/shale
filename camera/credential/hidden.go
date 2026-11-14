package credential

import (
	"context"
	"net/url"

	"github.com/lesomnus/shale/internal/rtspproxy"
)

// Hidden runs use with a URL that carries no credential (#249). A process's
// arguments are readable by every local user, so an rtsp or rtsps input with
// a username, a password or a query (where a legacy camera's token is) is
// served on loopback by a proxy that authenticates to the camera and keeps
// the query, and use is handed the proxy's URL, which names a random token
// instead. The proxy lives as long as use runs. Any other input is handed
// over as it is.
func Hidden(ctx context.Context, input string, use func(string) error) error {
	u, err := url.Parse(input)
	if err != nil || (u.User == nil && u.RawQuery == "") || (u.Scheme != "rtsp" && u.Scheme != "rtsps") {
		return use(input)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	user := u.User.Username()
	pass, _ := u.User.Password()
	target := *u
	target.User = nil
	p, err := rtspproxy.Start(&target, user, pass)
	if err != nil {
		return err
	}
	defer p.Close()

	return use(p.URL())
}
