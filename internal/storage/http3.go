package storage

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"

	"github.com/lesomnus/shale/internal/h3"
)

// serveData serves the data plane on dl over TCP, and with TLS over HTTP/3
// as well, on UDP at the same port, with the same handler and the same
// certificates, unless `storage.http3` is off (§35.6). A UDP port the node
// cannot have leaves it serving TCP alone, with a warning: HTTP/3 is what
// some clients add, and TCP is what every client speaks. It answers
// whether HTTP/3 is served.
func (n *Node) serveData(ctx context.Context, g *errgroup.Group, dl net.Listener, tlsCfg *tls.Config) bool {
	var quicLn *quic.Listener
	var closeQuic func()
	if tlsCfg != nil && n.cfg.HTTP3 {
		quicLn, closeQuic = n.listenQuic(dl.Addr(), tlsCfg)
	}
	handler := n.dataHandler(quicLn != nil)

	httpSrv := &http.Server{Handler: handler, ReadHeaderTimeout: 30 * time.Second, ErrorLog: log.New(httpNoise{n.log}, "", 0)}
	g.Go(func() error {
		var err error
		if tlsCfg != nil {
			httpSrv.TLSConfig = tlsCfg
			err = httpSrv.ServeTLS(dl, "", "")
		} else {
			err = httpSrv.Serve(dl)
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	})
	g.Go(func() error {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		return httpSrv.Shutdown(shut)
	})
	if quicLn == nil {
		return false
	}

	// Errors here end HTTP/3 and nothing else: TCP goes on serving.
	h3Srv := &http3.Server{Handler: handler, IdleTimeout: h3.ConnIdle, Logger: n.log}
	g.Go(func() error {
		if err := h3Srv.ServeListener(quicLn); err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
			n.log.Warn("http3: stopped serving; TCP goes on", "err", err.Error())
		}

		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		// New connections are refused from now, as TCP's are once its
		// listener closes: a handshake the listener completed while the
		// server waited out its requests would leave the client on a
		// connection nobody answers until its idle timeout, where a
		// refusal sends it on at once, to TCP or the node's next run.
		quicLn.Close()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h3Srv.Shutdown(shut); err != nil {
			h3Srv.Close()
		}
		closeQuic()

		return nil
	})

	return true
}

// listenQuic opens the QUIC listener on the UDP port of the TCP listener,
// and what closes it and the socket; or nil, and says why.
func (n *Node) listenQuic(addr net.Addr, tlsCfg *tls.Config) (*quic.Listener, func()) {
	ta, ok := addr.(*net.TCPAddr)
	if !ok {
		return nil, nil
	}
	ua := &net.UDPAddr{IP: ta.IP, Port: ta.Port, Zone: ta.Zone}
	if ta.IP.IsUnspecified() {
		// Every interface, both families, as TCP's ":port" is.
		ua.IP = nil
	}
	udp, err := net.ListenUDP("udp", ua)
	if err != nil {
		n.log.Warn("http3: not served, TCP only: the UDP port is not to be had", "addr", ua.String(), "err", err.Error())
		return nil, nil
	}
	tr := &quic.Transport{Conn: udp}
	closeAll := func() {
		// The transport leaves a socket it was given open.
		tr.Close()
		udp.Close()
	}
	ln, err := tr.Listen(http3.ConfigureTLSConfig(tlsCfg), h3.ServerConfig())
	if err != nil {
		closeAll()
		n.log.Warn("http3: not served, TCP only", "addr", ua.String(), "err", err.Error())
		return nil, nil
	}

	return ln, func() {
		ln.Close()
		closeAll()
	}
}

// dataHandler is the data plane as its listeners serve it: counted by HTTP
// version, and saying on every answer, over TCP and HTTP/3 alike, that
// HTTP/3 is on this port when it is (§35.6). The answers are otherwise the
// same on every version.
func (n *Node) dataHandler(h3on bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.m.requests.Add(r.Context(), 1, metric.WithAttributes(attribute.String("proto", protoOf(r))))
		if h3on {
			w.Header().Set("Alt-Svc", h3.AltSvc(r.Host))
		}
		n.dp.ServeHTTP(w, r)
	})
}

// protoOf names a request's HTTP version as an Endpoint does: h1, h2, h3.
func protoOf(r *http.Request) string {
	switch r.ProtoMajor {
	case 3:
		return "h3"
	case 2:
		return "h2"
	}

	return "h1"
}
