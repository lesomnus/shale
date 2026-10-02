// Package k8stest is a fake of the little of the Kubernetes API that
// package k8s speaks: Secrets in one namespace, kept in memory.
package k8stest

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/lesomnus/shale/internal/k8s"
)

// Secret is a Secret as the fake holds it, its data decoded.
type Secret struct {
	Name        string
	Labels      map[string]string
	Annotations map[string]string
	Data        map[string][]byte
}

// Server is the fake API server.
type Server struct {
	Namespace string
	Token     string

	srv *httptest.Server

	mu      sync.Mutex
	secrets map[string]Secret
	refuse  map[string]refusal
	posts   map[string]int
}

type refusal struct {
	code   int
	dryRun bool
}

// keyRe is what a Secret's key may be.
var keyRe = regexp.MustCompile(`^[-._a-zA-Z0-9]+$`)

// New starts a fake in the namespace `shale`, stopped with the test.
func New(t testing.TB) *Server {
	s := &Server{Namespace: "shale", Token: "token", secrets: map[string]Secret{}, refuse: map[string]refusal{}, posts: map[string]int{}}
	s.srv = httptest.NewTLSServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.srv.Close)

	return s
}

// Client is a client of this fake.
func (s *Server) Client() *k8s.Client {
	return k8s.New(s.srv.URL, s.Namespace, s.Token, s.srv.Client())
}

// Put places a Secret, as if somebody had made it.
func (s *Server) Put(v Secret) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.secrets[v.Name] = v
}

// Get answers the named Secret.
func (s *Server) Get(name string) (Secret, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.secrets[name]

	return v, ok
}

// Refuse makes every create of the named Secret answer code, as an API
// server that will not have it would: a role that does not allow it, say.
// Dry runs are refused too when dryRun is set; when it is not, the dry run
// passes and the create after it fails, as one that fails in between does.
func (s *Server) Refuse(name string, code int, dryRun bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refuse[name] = refusal{code, dryRun}
}

// Posts is how many creates of the named Secret were not dry runs.
func (s *Server) Posts(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.posts[name]
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+s.Token {
		http.Error(w, `{"reason":"Unauthorized"}`, http.StatusUnauthorized)
		return
	}
	prefix := "/api/v1/namespaces/" + s.Namespace + "/secrets"
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, prefix+"/"):
		name := strings.TrimPrefix(r.URL.Path, prefix+"/")
		if _, ok := s.secrets[name]; !ok {
			http.Error(w, `{"reason":"NotFound"}`, http.StatusNotFound)
			return
		}
		w.Write([]byte(`{"kind":"Secret"}`))

	case r.Method == http.MethodPost && r.URL.Path == prefix:
		var body struct {
			Metadata struct {
				Name        string            `json:"name"`
				Labels      map[string]string `json:"labels"`
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
			Data map[string]string `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		name := body.Metadata.Name
		dryRun := r.URL.Query().Get("dryRun") == "All"
		if v, ok := s.refuse[name]; ok && (!dryRun || v.dryRun) {
			http.Error(w, `{"reason":"refused"}`, v.code)
			return
		}
		if _, ok := s.secrets[name]; ok {
			http.Error(w, `{"reason":"AlreadyExists"}`, http.StatusConflict)
			return
		}
		v := Secret{Name: name, Labels: body.Metadata.Labels, Annotations: body.Metadata.Annotations, Data: map[string][]byte{}}
		for k, d := range body.Data {
			if !keyRe.MatchString(k) {
				http.Error(w, `{"reason":"Invalid"}`, http.StatusUnprocessableEntity)
				return
			}
			b, err := base64.StdEncoding.DecodeString(d)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			v.Data[k] = b
		}
		if !dryRun {
			s.secrets[name] = v
			s.posts[name]++
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"kind":"Secret"}`))

	default:
		http.Error(w, `{"reason":"NotFound"}`, http.StatusNotFound)
	}
}
