package harness

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSyncInspectManifestValidatesAndCopies(t *testing.T) {
	e := newIntegrationEngine(t)
	providers := []CordisInspectProviderManifest{{
		ID: "Client", Description: "client facts", Methods: []CordisInspectMethodManifest{{
			Name: "list", Description: "list facts",
			InputSchema: map[string]any{"type": "object"}, OutputSchema: map[string]any{"type": "object"},
		}},
	}}
	if err := e.SyncInspectManifest(providers); err != nil {
		t.Fatal(err)
	}
	providers[0].Methods[0].Description = "mutated"
	got := e.ListInspectProviders()
	if len(got) != 5 || got[4].Platform != "client" || got[4].Methods[0].Description != "list facts" {
		t.Fatalf("manifest copy = %#v", got)
	}
	if err := e.SyncInspectManifest([]CordisInspectProviderManifest{{ID: "Client", Description: "dup", Methods: []CordisInspectMethodManifest{}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.SyncInspectManifest([]CordisInspectProviderManifest{
		{ID: "Client", Description: "one"}, {ID: "Client", Description: "two"},
	}); err == nil {
		t.Fatal("duplicate provider was accepted")
	}
	if len(e.ListInspectProviders()) != 5 || e.ListInspectProviders()[4].Description != "dup" {
		t.Fatal("invalid manifest replaced the prior directory")
	}
}

func TestDynamicCordisRPCUsesRemoteArgsAndRejectsUnknownQuery(t *testing.T) {
	e := newIntegrationEngine(t)
	server := httptest.NewServer(e.Handler())
	t.Cleanup(server.Close)

	status, envelope := postRPC(t, server.Client(), server.URL, "", "dynamicCordisRunner/syncInspectManifest", map[string]any{
		"args": map[string]any{"providers": []any{map[string]any{
			"id": "Client", "description": "client facts", "methods": []any{map[string]any{
				"name": "list", "description": "list facts",
				"inputSchema": map[string]any{"type": "object"}, "outputSchema": map[string]any{"type": "object"},
			}},
		}}},
	})
	if status != http.StatusOK || rpcResult(t, envelope)["ok"] != true {
		t.Fatalf("sync response = %d %#v", status, envelope)
	}
	if len(e.ListInspectProviders()) != 5 {
		t.Fatal("remote args did not update manifest")
	}

	status, envelope = postRPC(t, server.Client(), server.URL, "", "dynamicCordisRunner/resolveInspectQuery", map[string]any{
		"args": map[string]any{
			"agentId": "session-1", "requestId": "inspect-1",
			"resolution": map[string]any{"ok": true, "data": map[string]any{}},
		},
	})
	result := rpcResult(t, envelope)
	value, ok := result["value"].(map[string]any)
	if status != http.StatusOK || !ok || value["accepted"] != false {
		t.Fatalf("unknown query response = %d %#v", status, envelope)
	}
}
