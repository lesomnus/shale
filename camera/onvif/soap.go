// Package onvif implements the read-only ONVIF Device/Media inspector and a
// narrowly scoped video configurator. Authentication never sets the device clock.
package onvif

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
)

const (
	deviceNS    = "http://www.onvif.org/ver10/device/wsdl"
	media1NS    = "http://www.onvif.org/ver10/media/wsdl"
	media2NS    = "http://www.onvif.org/ver20/media/wsdl"
	schemaNS    = "http://www.onvif.org/ver10/schema"
	maxResponse = 4 << 20
)

var (
	ErrTransport    = errors.New("ONVIF transport failed")
	ErrResponse     = errors.New("invalid ONVIF response")
	ErrVerification = errors.New("ONVIF configuration read-back differs from plan")
)

// Client has no retained credentials or device state. Its zero value is usable.
// HTTPClient's transport can configure certificate trust for a managed camera;
// certificate validation is never disabled by this package. Redirects are refused.
type Client struct{ HTTPClient *http.Client }

func NewClient(h *http.Client) *Client { return &Client{HTTPClient: h} }

type node struct {
	Name     xml.Name
	Attrs    []xml.Attr
	Text     string
	Children []*node
}

func parseXML(data []byte) (*node, error) {
	d := xml.NewDecoder(bytes.NewReader(data))
	root := &node{}
	stack := []*node{root}
	count := 0
	for {
		t, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, ErrResponse
		}
		switch x := t.(type) {
		case xml.StartElement:
			count++
			if count > 50000 || len(stack) > 64 {
				return nil, ErrResponse
			}
			n := &node{Name: x.Name, Attrs: x.Attr}
			p := stack[len(stack)-1]
			p.Children = append(p.Children, n)
			stack = append(stack, n)
		case xml.CharData:
			stack[len(stack)-1].Text += string(x)
		case xml.EndElement:
			if len(stack) < 2 {
				return nil, ErrResponse
			}
			stack = stack[:len(stack)-1]
		}
	}
	if len(stack) != 1 || len(root.Children) != 1 {
		return nil, ErrResponse
	}
	return root.Children[0], nil
}

func (n *node) child(name string) *node {
	if n != nil {
		for _, c := range n.Children {
			if c.Name.Local == name {
				return c
			}
		}
	}
	return nil
}
func (n *node) all(name string) []*node {
	var out []*node
	if n != nil {
		for _, c := range n.Children {
			if c.Name.Local == name {
				out = append(out, c)
			}
		}
	}
	return out
}
func (n *node) text(name string) string {
	if c := n.child(name); c != nil {
		return strings.TrimSpace(c.Text)
	}
	return ""
}
func (n *node) attr(name string) string {
	if n != nil {
		for _, a := range n.Attrs {
			if a.Name.Local == name {
				return a.Value
			}
		}
	}
	return ""
}
func (n *node) find(name string) *node {
	if n == nil {
		return nil
	}
	if n.Name.Local == name {
		return n
	}
	for _, c := range n.Children {
		if found := c.find(name); found != nil {
			return found
		}
	}
	return nil
}
func (n *node) set(name, value string) {
	c := n.child(name)
	if c == nil {
		c = &node{Name: xml.Name{Space: schemaNS, Local: name}}
		n.Children = append(n.Children, c)
	}
	c.Text = value
}
func (n *node) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	start.Name = n.Name
	for _, a := range n.Attrs {
		if a.Name.Space != "xmlns" && a.Name.Local != "xmlns" {
			start.Attr = append(start.Attr, a)
		}
	}
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	if n.Text != "" {
		if err := e.EncodeToken(xml.CharData(n.Text)); err != nil {
			return err
		}
	}
	for _, c := range n.Children {
		if err := e.Encode(c); err != nil {
			return err
		}
	}
	return e.EncodeToken(start.End())
}

func escape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func validatedEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return nil, camera.ErrInvalid
	}
	if camera.SanitizeURI(raw) != u.String() {
		return nil, camera.ErrInvalid
	}
	return u, nil
}

func sameCamera(base *url.URL, raw string, stream bool) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u == nil {
		return "", ErrResponse
	}
	u = base.ResolveReference(u)
	if !strings.EqualFold(u.Hostname(), base.Hostname()) {
		return "", ErrResponse
	}
	if stream {
		if u.Scheme != "rtsp" && u.Scheme != "rtsps" {
			return "", ErrResponse
		}
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return "", ErrResponse
	}
	return camera.SanitizeURI(u.String()), nil
}

func security(material camera.CredentialMaterial) (string, error) {
	if material.Username == "" {
		return "", nil
	}
	nonce := make([]byte, 20)
	if _, err := rand.Read(nonce); err != nil {
		return "", ErrTransport
	}
	created := time.Now().UTC().Format(time.RFC3339Nano)
	digest := sha1.New()
	_, _ = digest.Write(nonce)
	_, _ = digest.Write([]byte(created))
	_, _ = digest.Write([]byte(material.Password))
	return `<wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"><wsse:UsernameToken><wsse:Username>` + escape(material.Username) + `</wsse:Username><wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">` + base64.StdEncoding.EncodeToString(digest.Sum(nil)) + `</wsse:Password><wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">` + base64.StdEncoding.EncodeToString(nonce) + `</wsse:Nonce><wsu:Created>` + created + `</wsu:Created></wsse:UsernameToken></wsse:Security>`, nil
}

func (c *Client) request(ctx context.Context, endpoint, ns, operation, body string, material camera.CredentialMaterial) (*node, error) {
	if _, err := validatedEndpoint(endpoint); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	header, err := security(material)
	if err != nil {
		return nil, err
	}
	payload := `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:t="` + ns + `" xmlns:tt="` + schemaNS + `"><s:Header>` + header + `</s:Header><s:Body><t:` + operation + `>` + body + `</t:` + operation + `></s:Body></s:Envelope>`
	h := http.Client{}
	if c.HTTPClient != nil {
		h = *c.HTTPClient
	}
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	auth := ""
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(payload))
		if err != nil {
			return nil, camera.ErrInvalid
		}
		req.Header.Set("Content-Type", `application/soap+xml; charset=utf-8; action="`+ns+`/`+operation+`"`)
		req.Header.Set("SOAPAction", `"`+ns+`/`+operation+`"`)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		res, err := h.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrTransport
		}
		data, readErr := io.ReadAll(io.LimitReader(res.Body, maxResponse+1))
		_ = res.Body.Close()
		if readErr != nil || len(data) > maxResponse {
			return nil, ErrResponse
		}
		if res.StatusCode == 401 {
			if attempt == 0 && material.Username != "" {
				auth, err = digestAuthorization(res.Header.Values("WWW-Authenticate"), req, material)
				if err == nil {
					continue
				}
			}
			return nil, camera.ErrAuthRequired
		}
		if res.StatusCode == 403 {
			return nil, camera.ErrForbidden
		}
		if res.StatusCode == 404 || res.StatusCode == 405 || res.StatusCode == 501 {
			return nil, camera.ErrUnsupported
		}
		root, parseErr := parseXML(data)
		if parseErr != nil {
			return nil, ErrResponse
		}
		if f := root.find("Fault"); f != nil {
			// Do not return fault text. Some devices echo request credentials.
			b, _ := xml.Marshal(f)
			s := string(b)
			switch {
			case strings.Contains(s, "NotAuthorized"):
				return nil, camera.ErrAuthRequired
			case strings.Contains(s, "OperationProhibited"), strings.Contains(s, "AccessDenied"):
				return nil, camera.ErrForbidden
			case strings.Contains(s, "ActionNotSupported"), strings.Contains(s, "NoSuchService"), strings.Contains(s, "OptionalActionNotImplemented"):
				return nil, camera.ErrUnsupported
			}
			return nil, ErrResponse
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, ErrTransport
		}
		response := root.find(operation + "Response")
		if response == nil {
			return nil, ErrResponse
		}
		return response, nil
	}
	return nil, camera.ErrAuthRequired
}

var challengeParam = regexp.MustCompile(`(?i)([a-z][a-z0-9_-]*)\s*=\s*(?:"((?:\\.|[^"\\])*)"|([^,\s]+))`)

func digestAuthorization(headers []string, req *http.Request, m camera.CredentialMaterial) (string, error) {
	for _, header := range headers {
		i := strings.Index(strings.ToLower(header), "digest ")
		if i < 0 {
			continue
		}
		params := map[string]string{}
		for _, match := range challengeParam.FindAllStringSubmatch(header[i+7:], -1) {
			v := match[2]
			if v == "" {
				v = match[3]
			}
			params[strings.ToLower(match[1])] = strings.ReplaceAll(v, `\"`, `"`)
		}
		if params["nonce"] == "" || params["realm"] == "" {
			continue
		}
		alg := strings.ToUpper(params["algorithm"])
		if alg == "" {
			alg = "MD5"
		}
		var constructor func() hash.Hash
		switch strings.TrimSuffix(alg, "-SESS") {
		case "MD5":
			constructor = md5.New
		case "SHA-256":
			constructor = sha256.New
		default:
			continue
		}
		hashText := func(s string) string {
			h := constructor()
			_, _ = h.Write([]byte(s))
			return hex.EncodeToString(h.Sum(nil))
		}
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", ErrTransport
		}
		cnonce := hex.EncodeToString(random)
		qop := ""
		if offered := params["qop"]; offered != "" {
			for _, v := range strings.Split(offered, ",") {
				if strings.TrimSpace(v) == "auth" {
					qop = "auth"
				}
			}
			if qop == "" {
				continue
			}
		}
		uri := req.URL.RequestURI()
		ha1 := hashText(m.Username + ":" + params["realm"] + ":" + m.Password)
		if strings.HasSuffix(alg, "-SESS") {
			ha1 = hashText(ha1 + ":" + params["nonce"] + ":" + cnonce)
		}
		ha2 := hashText(req.Method + ":" + uri)
		response := hashText(ha1 + ":" + params["nonce"] + ":" + ha2)
		if qop != "" {
			response = hashText(ha1 + ":" + params["nonce"] + ":00000001:" + cnonce + ":" + qop + ":" + ha2)
		}
		quote := func(s string) string { return strconv.Quote(s) }
		value := `Digest username=` + quote(m.Username) + `, realm=` + quote(params["realm"]) + `, nonce=` + quote(params["nonce"]) + `, uri=` + quote(uri) + `, response=` + quote(response) + `, algorithm=` + alg
		if params["opaque"] != "" {
			value += `, opaque=` + quote(params["opaque"])
		}
		if qop != "" {
			value += `, qop=auth, nc=00000001, cnonce=` + quote(cnonce)
		} else if strings.HasSuffix(alg, "-SESS") {
			value += `, cnonce=` + quote(cnonce)
		}
		return value, nil
	}
	return "", fmt.Errorf("ONVIF HTTP authentication: %w", camera.ErrUnsupported)
}
