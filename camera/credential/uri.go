package credential

import (
	"errors"
	"net/url"
)

func parseInput(input string) (*url.URL, error) {
	u, e := url.Parse(input)
	if e != nil || u.Host == "" || (u.Scheme != "rtsp" && u.Scheme != "rtsps") {
		return nil, errors.New("referenced input must be an RTSP endpoint")
	}
	if u.User != nil {
		return nil, errors.New("referenced input must not also contain credentials")
	}
	return u, nil
}
func userPassword(u, p string) *url.Userinfo { return url.UserPassword(u, p) }
