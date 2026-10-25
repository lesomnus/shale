package sandbox_test

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// tags is what the sandbox is built with: the words of the `sandbox:build`
// script in `ts/package.json` and of CI's build of it. A link under other tags
// is a link nobody ships.
const tags = "grpcnotrace"

// TestTheSandboxLetsTheLinkerPruneMethods says that nothing the sandbox links
// may call a method by index.
//
// A function that calls `reflect.Type.Method`, or `MethodByName` with a name
// the compiler cannot see, is marked for the linker, and a program that can
// reach one keeps every exported method of every type converted to an
// interface -- in every package, used or not -- since any of them might now be
// called by name. Nothing fails: the page works, and is larger. And there is
// no package to keep out, since the call was in one the sandbox has to link.
// The generated client's `internal/ent/runtime.go` makes the ent schema's
// fields at init, for their defaults and value scanners, and each field it
// made listed its Go type's methods that way, from the day payday's `Audit`
// gave a column a default (payday#87) until protobuf-orm/ent listed the
// methods in its loader, which only codegen runs (protobuf-orm/ent#20, which
// payday 53eabb7 brings): the sandbox went from 183.9 MB to 114.1 MB.
//
// The linker says which marked functions it reached, and how, in its dump
// (`-dumpdep`). So the sandbox is linked as it ships -- `GOOS=js`, the same
// tags -- and the dump is read, and a failure names the chain from the
// program's start to the first such call, which is the import to look at.
// payday holds its own sandbox to this by the same name (lesomnus/payday#170),
// and roster its own.
func TestTheSandboxLetsTheLinkerPruneMethods(t *testing.T) {
	x := require.New(t)

	if testing.Short() {
		t.Skip("links the sandbox")
	}

	// `../..` is the module root from this package's directory, as
	// `npm run sandbox:build` builds from it.
	c := exec.CommandContext(t.Context(), "go", "build", "-tags", tags,
		"-ldflags=-dumpdep",
		"-o", filepath.Join(t.TempDir(), "app.wasm"),
		"./wasm",
	)
	c.Dir = filepath.Join("..", "..")
	c.Env = append(os.Environ(), "GOOS=js", "GOARCH=wasm")

	// The go command hands on what the linker says, the dump with it, on
	// stderr.
	var dump bytes.Buffer
	c.Stdout = &dump
	c.Stderr = &dump
	if err := c.Run(); err != nil {
		lines := strings.Split(strings.TrimSpace(dump.String()), "\n")
		x.NoError(err, "%s", strings.Join(lines[max(0, len(lines)-40):], "\n"))
	}

	// A line is `from -> to`, written when `to` is first reached, each name
	// followed by what the linker marked it with.
	var (
		from   = map[string]string{}
		marked []string
		iface  int
	)
	s := bufio.NewScanner(&dump)
	s.Buffer(nil, 1<<20)
	for s.Scan() {
		a, b, ok := strings.Cut(s.Text(), " -> ")
		if !ok {
			continue
		}
		if strings.HasSuffix(b, "<UsedInIface>") {
			iface++
		}
		a, _ = linked(a)
		b, m := linked(b)
		if _, ok := from[b]; ok {
			continue
		}
		from[b] = a
		if m {
			marked = append(marked, b)
		}
	}
	x.NoError(s.Err())

	// A dump that marks nothing would pass anything. The linker marks what is
	// converted to an interface the same way, and the sandbox converts tens of
	// thousands.
	x.Greater(iface, 1000)

	if len(marked) > 0 {
		chain := []string{marked[0]}
		for n := marked[0]; from[n] != "" && from[n] != "_"; n = from[n] {
			chain = append(chain, from[n])
		}
		slices.Reverse(chain)
		x.Fail("the sandbox calls a method by index, so the linker keeps every method",
			"%d marked functions are reached; the first through\n\t%s", len(marked), strings.Join(chain, "\n\t"))
	}
}

// linked is a name in the linker's dump without what it marked the name with,
// and whether that was a method called by index.
func linked(s string) (string, bool) {
	s, m := strings.CutSuffix(s, "<ReflectMethod>")
	s = strings.TrimSuffix(s, "<UsedInIface>")
	return strings.TrimSuffix(s, " "), m
}
