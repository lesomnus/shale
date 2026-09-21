// Package console serves the console (§40) from the control plane's own
// HTTP listener when it was built into the binary: `npm run build` in ts/
// writes it to dist/ here, which the Dockerfile and the package build do
// before `go build`. A binary built without it answers with a line that
// says so, rather than a 404 that looks like a wrong address.
package console

import (
	"bytes"
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"
)

//go:embed all:dist
var dist embed.FS

// Built says whether the console was built into this binary.
func Built() bool {
	_, err := fs.Stat(dist, "dist/index.html")

	return err == nil
}

// Handler serves the console at the root of the listener it is mounted on.
func Handler() http.Handler {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		return http.HandlerFunc(notBuilt)
	}

	return handler(sub)
}

// handler serves one build of the page: its files as they are, with a
// build's hashed assets marked immutable, and the page itself for any other
// path, since the page routes by its hash and `/cameras` is a bookmark of it
// rather than a file.
func handler(fsys fs.FS) http.Handler {
	index, err := fs.ReadFile(fsys, "index.html")
	if err != nil {
		return http.HandlerFunc(notBuilt)
	}
	built := time.Now()
	files := http.FileServer(http.FS(fsys))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if p != "" && p != "index.html" {
			if st, err := fs.Stat(fsys, p); err == nil && !st.IsDir() {
				if strings.HasPrefix(p, "assets/") {
					// Vite names them by content hash.
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				r2 := r.Clone(r.Context())
				r2.URL.Path = "/" + p
				files.ServeHTTP(w, r2)

				return
			}
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeContent(w, r, "index.html", built, bytes.NewReader(index))
	})
}

func notBuilt(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte("the console was not built into this binary: `npm run build` in ts/ before `go build` (§40.4)\n"))
}
