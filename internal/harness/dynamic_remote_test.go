package harness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestDynamicCordisClientLifecycleUsesForwardedRemoteEvents(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(context.Background(), e.Config().Workspace, "cordis-client-lifecycle", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "web"},
		Name:      "browser package",
		Purpose:   "exercise forwarded lifecycle",
		Code: DynamicCordisCode{
			Host:   "return { apply(ctx) {} }",
			Client: "return { apply(ctx) {} }",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	host := e.SubscribeHost(ctx)
	run, err := e.DynamicCordisRun(ctx, sessionID, receipt.PluginID, receipt.PackageID, "run")
	if err != nil {
		t.Fatal(err)
	}
	if !run.OK || run.Status != "awaiting-approval" {
		t.Fatalf("initial run = %#v", run)
	}

	requestID := ""
	for requestID == "" {
		select {
		case frame := <-host:
			if frame["type"] != "host/remote-event" || frame["event"] != "cordis/request-run" {
				continue
			}
			args, _ := frame["args"].([]any)
			request, _ := args[0].(map[string]any)
			requestID, _ = request["requestId"].(string)
		case <-ctx.Done():
			t.Fatal("request-run was not forwarded")
		}
	}

	hostHalf, err := e.DynamicCordisRunHostHalf(ctx, sessionID, receipt.PluginID, receipt.PackageID, "run", requestID, false)
	if err != nil || !hostHalf.OK || !hostHalf.StartedHere {
		t.Fatalf("host half = %#v, err=%v", hostHalf, err)
	}
	if _, err := e.DynamicCordisGetClientCode(sessionID, receipt.PluginID, hostHalf.PluginRunID); err != nil {
		t.Fatal(err)
	}
	ack, err := e.DynamicCordisResolveRequestRun(requestID, DynamicCordisRunResolution{OK: true, PluginRunID: hostHalf.PluginRunID})
	if err != nil || ack["accepted"] != true {
		t.Fatalf("resolve = %#v, err=%v", ack, err)
	}

	seenPackage, seenResolved := false, false
	deadline := time.After(2 * time.Second)
	for !seenPackage || !seenResolved {
		select {
		case frame := <-host:
			if frame["type"] != "host/remote-event" {
				continue
			}
			switch frame["event"] {
			case "cordis/dynamic-package":
				seenPackage = true
			case "cordis/request-run-resolved":
				seenResolved = true
				args, _ := frame["args"].([]any)
				resolved, _ := args[0].(map[string]any)
				if resolved["requestId"] != requestID || resolved["outcome"] != "approved" {
					t.Fatalf("resolved event = %#v", frame)
				}
			}
		case <-deadline:
			t.Fatalf("lifecycle events missing: package=%v resolved=%v", seenPackage, seenResolved)
		}
	}

	rows := e.DynamicCordisInventory(sessionID)
	if len(rows) != 1 || rows[0].CurrentPackageID != receipt.PackageID || rows[0].ActiveRun == nil {
		t.Fatalf("inventory after client lifecycle = %#v", rows)
	}
}

func TestDynamicCordisRenderFailureRemoteIsRetainedForActiveRun(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-render-failure", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "rend"},
		Name:      "rendered package",
		Purpose:   "record a browser render failure",
		Code:      DynamicCordisCode{Host: "return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(t.Context(), sessionID, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	session, _ := e.getSession(sessionID)
	session.mu.Lock()
	session.Running = true
	session.mu.Unlock()
	payload, _ := json.Marshal(map[string]any{"args": map[string]any{
		"agentId": sessionID, "pluginId": receipt.PluginID, "pluginRunId": run.PluginRunID,
		"failure": map[string]any{"slot": "shell.overlay", "message": "render failed", "stack": "render stack", "abdicated": true},
	}})
	if _, handled, rpcErr := e.dispatchRemote(t.Context(), "dynamicCordisRunner/reportRenderFailure", payload); rpcErr != nil || !handled {
		t.Fatalf("report remote = handled %v, error %v", handled, rpcErr)
	}
	rows := e.DynamicCordisInventory(sessionID)
	if len(rows) != 1 || rows[0].ActiveRun == nil || rows[0].ActiveRun.RenderFailure == nil {
		t.Fatalf("inventory = %#v", rows)
	}
	if failure := rows[0].ActiveRun.RenderFailure; failure.Slot != "shell.overlay" || failure.Message != "render failed" || failure.Stack != "render stack" || !failure.Abdicated {
		t.Fatalf("render failure = %#v", failure)
	}
	latest, _ := rows[0].LatestRun.(map[string]any)
	diagnostic, _ := latest["error"].(map[string]any)
	if diagnostic["phase"] != "client-render" || diagnostic["pluginId"] != receipt.PluginID || diagnostic["packageId"] != receipt.PackageID || diagnostic["pluginRunId"] != run.PluginRunID {
		t.Fatalf("render diagnostic = %#v", diagnostic)
	}
	session.mu.Lock()
	if len(session.steering) != 1 || session.steering[0].source["kind"] != "plugin" || session.steering[0].source["plugin"] != "cordis-host-runner" || !strings.Contains(session.steering[0].text, "Cordis Client UI") {
		t.Fatalf("render steering = %#v", session.steering)
	}
	session.mu.Unlock()
	e.DynamicCordisReportRenderFailure(sessionID, receipt.PluginID, run.PluginRunID, DynamicCordisRenderFailure{Slot: "other", Message: "new render failure"})
	session.mu.Lock()
	if len(session.steering) != 1 {
		t.Fatalf("duplicate render steering = %#v", session.steering)
	}
	session.mu.Unlock()
	if _, err := e.DynamicCordisStop(sessionID, receipt.PluginID); err != nil {
		t.Fatal(err)
	}
	rows = e.DynamicCordisInventory(sessionID)
	if len(rows) != 1 || rows[0].ActiveRun != nil {
		t.Fatalf("stopped inventory retained active failure: %#v", rows)
	}
}

func TestDynamicCordisClientGuardFailureSteersOncePerMessage(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-client-guard", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID,
		Plugin:    DynamicCordisPluginSelector{Kind: "new", IDPrefix: "guard"},
		Name:      "guarded package",
		Purpose:   "report a client guard failure",
		Code:      DynamicCordisCode{Host: "return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(t.Context(), sessionID, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	session, _ := e.getSession(sessionID)
	session.mu.Lock()
	session.Running = true
	session.mu.Unlock()
	report := func(message string) {
		t.Helper()
		payload, _ := json.Marshal(map[string]any{"args": map[string]any{
			"agentId": sessionID, "pluginId": receipt.PluginID, "pluginRunId": run.PluginRunID,
			"failure": map[string]any{"message": message, "stack": "client stack"},
		}})
		if _, handled, rpcErr := e.dispatchRemote(t.Context(), "dynamicCordisRunner/reportClientGuardFailure", payload); rpcErr != nil || !handled {
			t.Fatalf("report remote = handled %v, error %v", handled, rpcErr)
		}
	}
	report("blocked access")
	report("blocked access")
	session.mu.Lock()
	if len(session.steering) != 1 || !strings.Contains(session.steering[0].text, "Client guard rejected runtime code") || !strings.Contains(session.steering[0].text, "client stack") {
		t.Fatalf("guard steering = %#v", session.steering)
	}
	session.mu.Unlock()
	report("another access")
	session.mu.Lock()
	if len(session.steering) != 2 {
		t.Fatalf("distinct guard steering = %#v", session.steering)
	}
	session.mu.Unlock()
}

func TestDynamicCordisUpdateFailureRetainsCurrentAndPendingNext(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-update-failure", "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "ver"},
		Name: "v1", Purpose: "first", Code: DynamicCordisCode{Host: "return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run, err := e.DynamicCordisRun(t.Context(), sessionID, first.PluginID, first.PackageID, "run"); err != nil || !run.OK {
		t.Fatalf("first run = %#v, %v", run, err)
	}
	second, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "existing", PluginID: first.PluginID},
		Name: "v2", Purpose: "broken", Code: DynamicCordisCode{Host: `throw new Error("broken update")`},
	})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := e.DynamicCordisRun(t.Context(), sessionID, first.PluginID, second.PackageID, "update")
	if err != nil || failed.OK || failed.Reason != "host-half-failed" {
		t.Fatalf("failed update = %#v, %v", failed, err)
	}
	rows := e.DynamicCordisInventory(sessionID)
	if len(rows) != 1 || rows[0].CurrentPackageID != first.PackageID || rows[0].NextPackageID != second.PackageID || rows[0].ActiveRun != nil {
		t.Fatalf("inventory after failed update = %#v", rows)
	}
	latest, _ := rows[0].LatestRun.(map[string]any)
	if latest["status"] != "failed" {
		t.Fatalf("latest failed attempt = %#v", latest)
	}
}

func TestDynamicCordisClientFailureRetractsStartedHost(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-client-failure", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "uix"},
		Name: "ui", Purpose: "fails", Code: DynamicCordisCode{Host: "return { apply(ctx) {} }", Client: "return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	host := e.SubscribeHost(ctx)
	run, err := e.DynamicCordisRun(ctx, sessionID, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	requestID := ""
	for requestID == "" {
		select {
		case frame := <-host:
			if frame["event"] != "cordis/request-run" {
				continue
			}
			args, _ := frame["args"].([]any)
			request, _ := args[0].(map[string]any)
			requestID, _ = request["requestId"].(string)
		case <-ctx.Done():
			t.Fatal("request-run was not emitted")
		}
	}
	half, err := e.DynamicCordisRunHostHalf(ctx, sessionID, receipt.PluginID, receipt.PackageID, "run", requestID, false)
	if err != nil || !half.OK {
		t.Fatalf("host half = %#v, %v", half, err)
	}
	startedHere := true
	if _, err := e.DynamicCordisSettleUserRun(sessionID, receipt.PluginID, DynamicCordisRunResolution{OK: false, Reason: "client-half-failed", PluginRunID: half.PluginRunID, StartedHere: &startedHere, Message: "client failed"}); err != nil {
		t.Fatal(err)
	}
	rows := e.DynamicCordisInventory(sessionID)
	if len(rows) != 1 || rows[0].ActiveRun != nil {
		t.Fatalf("inventory after client failure = %#v", rows)
	}
}

func TestDynamicCordisResolveWaitsForBrowserHostHalf(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-browser-host", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "wait"},
		Name: "browser package", Purpose: "wait for browser", Code: DynamicCordisCode{Host: "return { apply(ctx) {} }", Client: "return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.DynamicCordisRun(t.Context(), sessionID, receipt.PluginID, receipt.PackageID, "run")
	if err != nil || !run.OK {
		t.Fatalf("run = %#v, %v", run, err)
	}
	e.dynamicCordis.RLock()
	requestID := ""
	for id := range e.dynamicCordis.pendingRuns {
		requestID = id
	}
	e.dynamicCordis.RUnlock()
	if requestID == "" {
		t.Fatal("pending request was not recorded")
	}

	ack, err := e.DynamicCordisResolveRequestRun(requestID, DynamicCordisRunResolution{OK: true, PluginRunID: run.PluginRunID})
	if err != nil || ack["accepted"] != false {
		t.Fatalf("early resolve = %#v, %v", ack, err)
	}
	if rows := e.DynamicCordisInventory(sessionID); len(rows) != 1 || rows[0].ActiveRun != nil {
		t.Fatalf("early resolve started host = %#v", rows)
	}

	half, err := e.DynamicCordisRunHostHalf(t.Context(), sessionID, receipt.PluginID, receipt.PackageID, "run", requestID, false)
	if err != nil || !half.OK {
		t.Fatalf("host half = %#v, %v", half, err)
	}
	ack, err = e.DynamicCordisResolveRequestRun(requestID, DynamicCordisRunResolution{OK: true, PluginRunID: half.PluginRunID, WaitingFor: []string{"slots"}})
	if err != nil || ack["accepted"] != true {
		t.Fatalf("settled resolve = %#v, %v", ack, err)
	}
	rows := e.DynamicCordisInventory(sessionID)
	latest, _ := rows[0].LatestRun.(map[string]any)
	client, _ := latest["client"].(map[string]any)
	if rows[0].CurrentPackageID != receipt.PackageID || rows[0].NextPackageID != "" || latest["status"] != "waiting" || client["status"] != "waiting" {
		t.Fatalf("settled inventory = %#v", rows)
	}
}

func TestDynamicCordisDirectSettleReturnsClientWaiting(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-direct-waiting", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "panel"},
		Name: "panel", Purpose: "wait", Code: DynamicCordisCode{Host: "return { apply(ctx) {} }", Client: "return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	half, err := e.DynamicCordisRunHostHalf(t.Context(), sessionID, receipt.PluginID, receipt.PackageID, "run", "", false)
	if err != nil || !half.OK {
		t.Fatalf("host half = %#v, %v", half, err)
	}
	settled, err := e.DynamicCordisSettleUserRun(sessionID, receipt.PluginID, DynamicCordisRunResolution{OK: true, PluginRunID: half.PluginRunID, WaitingFor: []string{"slots"}})
	if err != nil || !settled.OK || settled.Mode != "run" || settled.NextPackageID != "" || len(settled.WaitingFor) != 0 || len(settled.ClientWaitingFor) != 1 || settled.ClientWaitingFor[0] != "slots" {
		t.Fatalf("settled = %#v, %v", settled, err)
	}
	rows := e.DynamicCordisInventory(sessionID)
	latest, _ := rows[0].LatestRun.(map[string]any)
	client, _ := latest["client"].(map[string]any)
	if latest["status"] != "waiting" || client["status"] != "waiting" {
		t.Fatalf("waiting inventory = %#v", rows)
	}
}

func TestDynamicCordisConcurrentDirectHostHalfSharesAttempt(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-concurrent-host", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "race"},
		Name: "slow", Purpose: "share activation", Code: DynamicCordisCode{Host: "const until = Date.now() + 100; while (Date.now() < until) {} return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		half DynamicCordisHostHalfResult
		err  error
	}
	firstResult := make(chan result, 1)
	go func() {
		half, runErr := e.DynamicCordisRunHostHalf(t.Context(), sessionID, receipt.PluginID, receipt.PackageID, "run", "", false)
		firstResult <- result{half: half, err: runErr}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		e.dynamicCordis.RLock()
		starting := e.dynamicCordis.starting[receipt.PluginID] != nil
		e.dynamicCordis.RUnlock()
		if starting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first host half did not enter starting state")
		}
		time.Sleep(time.Millisecond)
	}
	second, err := e.DynamicCordisRunHostHalf(t.Context(), sessionID, receipt.PluginID, receipt.PackageID, "run", "", false)
	first := <-firstResult
	if first.err != nil || err != nil || !first.half.OK || !second.OK || first.half.PluginRunID != second.PluginRunID {
		t.Fatalf("first = %#v, %v; second = %#v, %v", first.half, first.err, second, err)
	}
}

func TestDynamicCordisStopMarksPendingPackageHalves(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-stop-pending", "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "stop"},
		Name: "host", Purpose: "current", Code: DynamicCordisCode{Host: "return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run, runErr := e.DynamicCordisRun(t.Context(), sessionID, first.PluginID, first.PackageID, "run"); runErr != nil || !run.OK {
		t.Fatalf("first run = %#v, %v", run, runErr)
	}
	second, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "existing", PluginID: first.PluginID},
		Name: "client", Purpose: "pending update", Code: DynamicCordisCode{Client: "return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run, runErr := e.DynamicCordisRun(t.Context(), sessionID, first.PluginID, second.PackageID, "update"); runErr != nil || !run.OK {
		t.Fatalf("update = %#v, %v", run, runErr)
	}
	if stopped, stopErr := e.DynamicCordisStop(sessionID, first.PluginID); stopErr != nil || !stopped.OK {
		t.Fatalf("stop = %#v, %v", stopped, stopErr)
	}
	rows := e.DynamicCordisInventory(sessionID)
	latest, _ := rows[0].LatestRun.(map[string]any)
	host, _ := latest["host"].(map[string]any)
	client, _ := latest["client"].(map[string]any)
	if rows[0].CurrentPackageID != first.PackageID || rows[0].NextPackageID != second.PackageID || latest["status"] != "stopped" || host["status"] != "absent" || client["status"] != "stopped" {
		t.Fatalf("stopped inventory = %#v", rows)
	}
}

func TestDynamicCordisDirectRejectReturnsRejected(t *testing.T) {
	e := newIntegrationEngine(t)
	sessionID, err := e.CreateSession(t.Context(), e.Config().Workspace, "cordis-direct-reject", "")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := e.DynamicCordisDefine(DynamicCordisDefineRequest{
		SessionID: sessionID, Plugin: DynamicCordisPluginSelector{Kind: "new", IDPrefix: "deny"},
		Name: "idle", Purpose: "reject", Code: DynamicCordisCode{Client: "return { apply(ctx) {} }"},
	})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := e.DynamicCordisSettleUserRun(sessionID, receipt.PluginID, DynamicCordisRunResolution{Reason: "rejected"})
	if err != nil || settled.OK || settled.Reason != "rejected" || settled.Message != "the run request was declined" {
		t.Fatalf("rejected = %#v, %v", settled, err)
	}
}
