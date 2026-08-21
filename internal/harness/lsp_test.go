package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLSPStdioProviderAndToolUseRealProtocol(t *testing.T) {
	workspace := t.TempDir()
	source := filepath.Join(workspace, "x.ts")
	if err := os.WriteFile(source, []byte("const answer = 42\nanswer\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(workspace, "lsp.log")
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(workspace, ".dsh")
	cfg.Workspace = workspace
	cfg.Persist = false
	cfg.SessionTitleLLM.Enabled = false
	cfg.LSPServers = map[string]LSPStdioConfig{
		"fixture": {
			Command: os.Args[0], Args: []string{"-test.run=^TestLSPFakeServer$"},
			Env:                 map[string]string{"GO_WANT_LSP_HELPER": "1", "LSP_TEST_LOG": marker},
			ExtensionToLanguage: map[string]string{".ts": "typescript"},
			Configuration:       map[string]any{"strict": true}, KillGrace: 50 * time.Millisecond,
		},
	}
	cfg.LSPTool = LSPToolConfig{Enabled: true}
	engine, err := New(WithConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	definition, err := engine.QueryLSP(t.Context(), LSPQuery{
		Operation: LSPGoToDefinition, FilePath: "x.ts", Position: LSPPosition{Line: 1}, WorkspaceRoot: workspace,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(definition.Locations) != 1 || definition.Locations[0].Range.Start != (LSPPosition{Line: 1, Character: 2}) {
		t.Fatalf("definition = %#v", definition)
	}
	hover, err := engine.QueryLSP(t.Context(), LSPQuery{
		Operation: LSPHover, FilePath: source, Position: LSPPosition{}, WorkspaceRoot: workspace,
	})
	if err != nil || hover.Hover == nil || hover.Hover.Contents != "**number**" {
		t.Fatalf("hover = %#v, %v", hover, err)
	}
	sessionID, err := engine.CreateSession(t.Context(), workspace, "", "")
	if err != nil {
		t.Fatal(err)
	}
	toolResult := executeRegisteredTool(t, engine, "lsp", sessionID, map[string]any{
		"operation": LSPGoToDefinition, "file_path": "x.ts", "line": 2, "character": 1,
	})
	if toolResult.IsError || len(toolResult.Content) != 1 || toolResult.Content[0].Text != "x.ts:2:3" {
		t.Fatalf("lsp tool = %#v", toolResult)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	text := string(log)
	if strings.Count(text, "initialize\n") != 1 || strings.Count(text, "textDocument/didOpen\n") != 3 || !strings.Contains(text, "shutdown\nexit\n") {
		t.Fatalf("protocol log = %q", text)
	}
}

func TestLSPCancellationKillsAndReplacesServer(t *testing.T) {
	workspace := t.TempDir()
	for _, name := range []string{"hang.ts", "ok.ts"} {
		if err := os.WriteFile(filepath.Join(workspace, name), []byte("const x = 1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	provider, err := NewStdioLSPProvider("fixture", LSPStdioConfig{
		Command: os.Args[0], Args: []string{"-test.run=^TestLSPFakeServer$"},
		Env: map[string]string{"GO_WANT_LSP_HELPER": "1"}, ExtensionToLanguage: map[string]string{"ts": "typescript"},
		KillGrace: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(WithPersistence(false), WithSessionTitleLLM(false))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if _, err := engine.RegisterLSPProvider(provider); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	_, err = engine.QueryLSP(ctx, LSPQuery{Operation: LSPHover, FilePath: "hang.ts", WorkspaceRoot: workspace})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hanging query error = %v", err)
	}
	result, err := engine.QueryLSP(t.Context(), LSPQuery{Operation: LSPHover, FilePath: "ok.ts", WorkspaceRoot: workspace})
	if err != nil || result.Hover == nil {
		t.Fatalf("replacement query = %#v, %v", result, err)
	}
}

func TestLSPRegistryRejectsConflictsAndOutsideSources(t *testing.T) {
	workspace := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.ts")
	if err := os.WriteFile(outside, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	provider, err := NewStdioLSPProvider("fixture", LSPStdioConfig{
		Command: os.Args[0], Args: []string{"-test.run=^TestLSPFakeServer$"},
		Env: map[string]string{"GO_WANT_LSP_HELPER": "1"}, ExtensionToLanguage: map[string]string{".TS": "typescript"},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(WithPersistence(false), WithSessionTitleLLM(false))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if _, err := engine.RegisterLSPProvider(provider); err != nil {
		t.Fatal(err)
	}
	second, _ := NewStdioLSPProvider("second", LSPStdioConfig{Command: os.Args[0], ExtensionToLanguage: map[string]string{"ts": "typescript"}})
	if _, err := engine.RegisterLSPProvider(second); err == nil || !strings.Contains(err.Error(), "LSP_CONFLICT") {
		t.Fatalf("conflict error = %v", err)
	}
	_, err = engine.QueryLSP(t.Context(), LSPQuery{Operation: LSPHover, FilePath: outside, WorkspaceRoot: workspace})
	if err == nil || !strings.Contains(err.Error(), "LSP_SOURCE_OUTSIDE_WORKSPACE") {
		t.Fatalf("outside source error = %v", err)
	}
}

func TestLSPFakeServer(t *testing.T) {
	if os.Getenv("GO_WANT_LSP_HELPER") != "1" {
		return
	}
	if err := runLSPFakeServer(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func runLSPFakeServer(input io.Reader, output io.Writer) error {
	reader := bufio.NewReader(input)
	var openURI string
	for {
		message, err := readLSPTestMessage(reader)
		if err != nil {
			return err
		}
		method, _ := message["method"].(string)
		if marker := os.Getenv("LSP_TEST_LOG"); marker != "" {
			file, err := os.OpenFile(marker, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				return err
			}
			_, _ = file.WriteString(method + "\n")
			_ = file.Close()
		}
		id := message["id"]
		switch method {
		case "initialize":
			if err := writeLSPTestMessage(output, map[string]any{"jsonrpc": "2.0", "id": "config", "method": "workspace/configuration", "params": map[string]any{"items": []any{map[string]any{}}}}); err != nil {
				return err
			}
			configuration, err := readLSPTestMessage(reader)
			if err != nil {
				return err
			}
			if configuration["id"] != "config" {
				return fmt.Errorf("configuration response = %#v", configuration)
			}
			if err := writeLSPTestMessage(output, map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"capabilities": map[string]any{
				"positionEncoding": "utf-16", "textDocumentSync": map[string]any{"openClose": true, "change": 1},
				"definitionProvider": true, "referencesProvider": true, "implementationProvider": true, "hoverProvider": true,
			}}}); err != nil {
				return err
			}
		case "textDocument/didOpen":
			params, _ := message["params"].(map[string]any)
			document, _ := params["textDocument"].(map[string]any)
			openURI, _ = document["uri"].(string)
			if text, _ := document["text"].(string); text == "" {
				return errors.New("didOpen omitted source text")
			}
		case "textDocument/didClose":
			openURI = ""
		case "textDocument/definition":
			if openURI == "" {
				return errors.New("definition without didOpen")
			}
			if err := writeLSPTestMessage(output, map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{
				"targetUri": openURI, "targetSelectionRange": lspTestRange(1, 2, 1, 8),
			}}); err != nil {
				return err
			}
		case "textDocument/references":
			params, _ := message["params"].(map[string]any)
			contextValue, _ := params["context"].(map[string]any)
			if contextValue["includeDeclaration"] != true {
				return errors.New("references omitted includeDeclaration")
			}
			if err := writeLSPTestMessage(output, map[string]any{"jsonrpc": "2.0", "id": id, "result": []any{map[string]any{"uri": openURI, "range": lspTestRange(0, 0, 0, 1)}}}); err != nil {
				return err
			}
		case "textDocument/implementation":
			if err := writeLSPTestMessage(output, map[string]any{"jsonrpc": "2.0", "id": id, "result": nil}); err != nil {
				return err
			}
		case "textDocument/hover":
			if strings.Contains(openURI, "hang.ts") {
				continue
			}
			if err := writeLSPTestMessage(output, map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"contents": map[string]any{"kind": "markdown", "value": "**number**"}}}); err != nil {
				return err
			}
		case "$/cancelRequest":
			continue
		case "shutdown":
			if err := writeLSPTestMessage(output, map[string]any{"jsonrpc": "2.0", "id": id, "result": nil}); err != nil {
				return err
			}
		case "exit":
			return nil
		}
	}
}

func lspTestRange(startLine, startCharacter, endLine, endCharacter int) map[string]any {
	return map[string]any{
		"start": map[string]any{"line": startLine, "character": startCharacter},
		"end":   map[string]any{"line": endLine, "character": endCharacter},
	}
}

func readLSPTestMessage(reader *bufio.Reader) (map[string]any, error) {
	length := -1
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == "\r\n" {
			break
		}
		name, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok && strings.EqualFold(name, "Content-Length") {
			length, err = strconv.Atoi(strings.TrimSpace(value))
			if err != nil {
				return nil, err
			}
		}
	}
	if length < 0 {
		return nil, errors.New("missing Content-Length")
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var message map[string]any
	return message, decoder.Decode(&message)
}

func writeLSPTestMessage(output io.Writer, message any) error {
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(output, "Content-Length: %d\r\n\r\n%s", len(body), body)
	return err
}
