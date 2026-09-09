package harness

import "testing"

func TestInboxRecoveryRejectsDuplicatePendingIDs(t *testing.T) {
	message := func(id string) any {
		return map[string]any{"id": id, "role": "user", "content": []any{}, "source": map[string]any{"kind": "user"}}
	}
	splice := func(target string, start, removed int, inserted ...any) Event {
		if inserted == nil {
			inserted = []any{}
		}
		return Event{Type: "agent/inbox/spliced", Data: map[string]any{"target": target, "start": start, "removedCount": removed, "inserted": inserted}}
	}
	for _, test := range []struct {
		name   string
		events []Event
		fail   bool
	}{
		{"same target", []Event{splice("next-turn", 0, 0, message("m"), message("m"))}, true},
		{"cross target", []Event{splice("next-turn", 0, 0, message("m")), splice("next-step", 0, 0, message("m"))}, true},
		{"replace in place", []Event{splice("next-turn", 0, 0, message("m")), splice("next-turn", 0, 1, message("m"))}, false},
		{"move after removal", []Event{splice("next-turn", 0, 0, message("m")), splice("next-turn", 0, 1), splice("next-step", 0, 0, message("m"))}, false},
		{"distinct IDs", []Event{splice("next-turn", 0, 0, message("m")), splice("next-step", 0, 0, message("n"))}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := restorePromptQueues(test.events)
			if (err != nil) != test.fail {
				t.Fatalf("restore error = %v, want failure %v", err, test.fail)
			}
		})
	}
}
