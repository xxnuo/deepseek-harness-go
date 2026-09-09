package main

import "testing"

func TestWorkspaceFilesProfileLimits(t *testing.T) {
	for _, test := range []struct {
		name, config string
		fail         bool
	}{
		{"defaults", "{}", false},
		{"custom", "{maxBytes: 9, maxLines: 2, maxEntries: 3}", false},
		{"zero", "{maxBytes: 0}", true},
		{"negative", "{maxLines: -1}", true},
		{"fraction", "{maxEntries: 2.5}", true},
		{"unknown ignored", "{maxUnknown: 1}", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			composed := testComposition(t, "- id: workspace-files\n  name: '@deepseek-ai/dsh-api-workspace-files'\n  config: "+test.config+"\n")
			err := composed.resolveWorkspaceFilesConfig()
			if (err != nil) != test.fail {
				t.Fatalf("config error = %v, want failure %v", err, test.fail)
			}
			if test.name == "custom" && (composed.workspaceFiles.MaxBytes != 9 || composed.workspaceFiles.MaxLines != 2 || composed.workspaceFiles.MaxEntries != 3) {
				t.Fatalf("custom config = %#v", composed.workspaceFiles)
			}
		})
	}
}
