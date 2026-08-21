package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	sandboxReadOnly       = "read-only"
	sandboxWorkspaceWrite = "workspace-write"
	sandboxDangerFull     = "danger-full-access"
)

var sandboxPresetModes = map[string]string{
	"read-only":          sandboxReadOnly,
	"workspace-write":    sandboxWorkspaceWrite,
	"danger-full-access": sandboxDangerFull,
}

// sandboxModeForCall resolves the session override at the execution boundary.
// Permission events are the durable source of truth; the environment is only
// the deployment default for calls made before a session has an override.
func (e *Engine) sandboxModeForCall(call ToolCall) (string, error) {
	mode := strings.TrimSpace(os.Getenv("DSH_PERMISSION_MODE"))
	if _, ok := sandboxPresetModes[mode]; !ok {
		mode = sandboxWorkspaceWrite
	}
	if e == nil || call.SessionID == "" {
		return mode, nil
	}
	s, err := e.getSession(call.SessionID)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	events := append([]Event(nil), s.Events...)
	s.mu.Unlock()
	for _, event := range events {
		data, _ := event.Data.(map[string]any)
		switch event.Type {
		case "permission/preset":
			if preset, ok := data["preset"].(string); ok {
				if next, exists := sandboxPresetModes[preset]; exists {
					mode = next
				}
			}
		case "sandbox/mode":
			if next, ok := data["mode"].(string); ok {
				if _, exists := sandboxPresetModes[next]; exists {
					mode = next
				}
			}
		}
	}
	return mode, nil
}

func sandboxDenied(mode string, operation string) error {
	if mode == sandboxReadOnly {
		return errors.New("FS_SANDBOX_DENIED: [sandbox: file access denied under read-only mode] " + operation + " is unavailable")
	}
	return nil
}

func sandboxPolicyDenied(mode string, operation string) error {
	return errors.New("FS_SANDBOX_DENIED: [sandbox: file access denied under " + mode + " mode] " + operation + " is outside the writable roots")
}

func validateSandboxEscalation(requested, justification *string) error {
	if requested != nil && justification == nil {
		return errors.New("invalid escalation: sandbox_permissions requires a justification")
	}
	if requested == nil && justification != nil {
		return errors.New("invalid escalation: justification is only valid together with sandbox_permissions")
	}
	if justification != nil && strings.TrimSpace(*justification) == "" {
		return errors.New("invalid justification: expected a non-empty sentence")
	}
	return nil
}

func sandboxWiderMode(current, requested string) bool {
	switch current {
	case sandboxReadOnly:
		return requested == sandboxWorkspaceWrite || requested == sandboxDangerFull
	case sandboxWorkspaceWrite:
		return requested == sandboxDangerFull
	default:
		return false
	}
}

// resolveSandboxMode applies an approved one-shot escalation without changing
// the session's standing mode. The approval request is durable in the same
// session log and answerable through the existing mux interaction channel.
func (e *Engine) resolveSandboxMode(ctx context.Context, call ToolCall, requested, justification *string, subject string) (string, error) {
	if err := validateSandboxEscalation(requested, justification); err != nil {
		return "", err
	}
	standing, err := e.sandboxModeForCall(call)
	if err != nil || requested == nil {
		return standing, err
	}
	target := strings.TrimSpace(*requested)
	if target != sandboxWorkspaceWrite && target != sandboxDangerFull {
		return "", fmt.Errorf("sandbox escalation target %q is invalid", target)
	}
	if !sandboxWiderMode(standing, target) {
		return "", fmt.Errorf("sandbox escalation to %q is not strictly wider than this call's current %q mode", target, standing)
	}
	if call.SessionID == "" {
		return "", fmt.Errorf("sandbox escalation to %q requires approval, but the call has no agent to route it through", target)
	}
	session, err := e.getSession(call.SessionID)
	if err != nil {
		return "", err
	}
	session.mu.Lock()
	events := append([]Event(nil), session.Events...)
	session.mu.Unlock()
	policy := effectiveEventString(events, "approval/policy", "policy", "ask")
	approvalID := newID("approval")
	toolName := strings.TrimSpace(call.Name)
	if toolName == "" {
		toolName = subject
	}
	reason := "escalate sandbox to " + target + ": " + strings.TrimSpace(*justification)
	asked := map[string]any{"id": approvalID, "toolName": toolName, "reason": reason}
	if call.ID != "" {
		asked["callId"] = call.ID
	}
	if _, err := e.appendEvent(session, "approval/asked", asked); err != nil {
		return "", err
	}
	outcome := "rejected"
	if policy == "ask" {
		payload := map[string]any{
			"type": "approval/requested", "sessionId": call.SessionID, "approvalId": approvalID,
			"toolName": toolName, "reason": reason,
		}
		if call.ID != "" {
			payload["callId"] = call.ID
		}
		value, interactionErr := e.RequestInteraction(ctx, call.SessionID, "approval/requested", payload)
		if interactionErr != nil {
			if errors.Is(interactionErr, context.Canceled) || errors.Is(interactionErr, context.DeadlineExceeded) {
				outcome = "cancelled"
			} else {
				outcome = "unavailable"
			}
		} else if response, ok := value.(map[string]any); ok {
			responseSession, _ := response["sessionId"].(string)
			responseID, _ := response["approvalId"].(string)
			responseOutcome, _ := response["outcome"].(string)
			if responseSession == call.SessionID && responseID == approvalID && (responseOutcome == "allowed-once" || responseOutcome == "rejected" || responseOutcome == "cancelled") {
				outcome = responseOutcome
			} else {
				outcome = "unavailable"
			}
		} else {
			outcome = "unavailable"
		}
	}
	if _, err := e.appendEvent(session, "approval/decided", map[string]any{"id": approvalID, "outcome": outcome}); err != nil {
		return "", err
	}
	switch outcome {
	case "allowed-once":
		return target, nil
	case "rejected":
		return "", fmt.Errorf("the user rejected escalating this %s to %q", subject, target)
	case "cancelled":
		return "", fmt.Errorf("approval for escalating to %q was cancelled", target)
	default:
		return "", fmt.Errorf("sandbox escalation to %q requires approval, but no approval channel is available", target)
	}
}

func sandboxDenialMarker(mode string) string {
	return "[sandbox: file access denied under " + mode + " mode]"
}

func sandboxEscalationHint(subject string) string {
	return "[sandbox: escalation available - retry this exact " + subject + " once with sandbox_permissions (the narrowest wider mode that suffices) + justification; the approval prompt asks the user]"
}

func sandboxErrorWithEscalationHint(err error, subject string) error {
	if err == nil || !strings.Contains(err.Error(), "FS_SANDBOX_DENIED") || strings.Contains(err.Error(), "sandbox: escalation available") {
		return err
	}
	return errors.New(err.Error() + "\n" + sandboxEscalationHint(subject))
}

func sandboxOutputDenied(text string) bool {
	text = strings.ToLower(text)
	return strings.Contains(text, "read-only file system") || strings.Contains(text, "permission denied") ||
		strings.Contains(text, "operation not permitted") || strings.Contains(text, "access is denied") ||
		strings.Contains(text, "access to the path") && strings.Contains(text, "is denied")
}
