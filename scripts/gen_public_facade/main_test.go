package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderFacadeAliasesTypesAndForwardsFunctions(t *testing.T) {
	directory := t.TempDir()
	source := `package harness

import (
    "context"
    "io"
)

const PublicConstant = 1
var PublicVariable = []string{"one"}
type PublicType[T any] struct{ Value T }
func PublicUnnamed(context.Context) {}
func PublicFunction[T any](ctx context.Context, value T, writers ...io.Writer) (T, error) { return value, nil }
func Version() string { return "ignored" }
`
	if err := os.WriteFile(filepath.Join(directory, "sample.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	declarations, imports, err := scan(directory)
	if err != nil {
		t.Fatal(err)
	}
	data, err := render(declarations, imports)
	if err != nil {
		t.Fatal(err)
	}
	generated := string(data)
	for _, want := range []string{
		"const PublicConstant = core.PublicConstant",
		"var PublicVariable = core.PublicVariable",
		"type PublicType[T any] = core.PublicType[T]",
		"func PublicUnnamed(arg0 context.Context)",
		"core.PublicUnnamed(arg0)",
		"func PublicFunction[T any](ctx context.Context, value T, writers ...io.Writer) (T, error)",
		"return core.PublicFunction[T](ctx, value, writers...)",
	} {
		if !strings.Contains(generated, want) {
			t.Fatalf("generated facade missing %q:\n%s", want, generated)
		}
	}
	if strings.Contains(generated, "func Version") {
		t.Fatalf("generated facade included root-owned Version:\n%s", generated)
	}
}
