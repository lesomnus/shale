package console

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestHandler(t *testing.T) {
	h := handler(fstest.MapFS{
		"index.html":         {Data: []byte("<!doctype html><title>shale</title>")},
		"assets/app-abc.js":  {Data: []byte("console.log(1)")},
		"assets/app-abc.css": {Data: []byte("body{}")},
	})
	get := func(p string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, p, nil))

		return w
	}

	// The page, at the root and at any route of its own.
	for _, p := range []string{"/", "/index.html", "/cameras", "/live/abc", "/assets/", "/assets/missing.js"} {
		w := get(p)
		require.Equal(t, http.StatusOK, w.Code, p)
		require.Contains(t, w.Body.String(), "<title>shale</title>", p)
		require.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"), p)
		require.Equal(t, "no-cache", w.Header().Get("Cache-Control"), p)
	}

	// A file of the build, as it is; the hashed ones for good.
	w := get("/assets/app-abc.js")
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "console.log(1)", w.Body.String())
	require.Contains(t, w.Header().Get("Content-Type"), "javascript")
	require.Equal(t, "public, max-age=31536000, immutable", w.Header().Get("Cache-Control"))
	w = get("/assets/../assets/app-abc.css")
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "body{}", w.Body.String())
}

func TestNotBuilt(t *testing.T) {
	h := handler(fstest.MapFS{})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(t, w.Body.String(), "npm run build")

	// This checkout: whatever `npm run build` left, or nothing.
	if !Built() {
		w = httptest.NewRecorder()
		Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
		require.Equal(t, http.StatusNotFound, w.Code)
	}
}
