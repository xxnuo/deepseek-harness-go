package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

func main() {
	upstream := flag.String("upstream", "deepseek-harness", "pinned upstream checkout")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fatal(errors.New("missing go command"))
	}

	root, err := os.Getwd()
	if err != nil {
		fatal(err)
	}
	temporary, err := os.MkdirTemp("", "deepseek-harness-go-build-*")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(temporary)

	bundle := filepath.Join(temporary, "embedded-assets.tar.zst")
	generate := exec.Command("go", "run", "./scripts/sync_runtime_assets.go", "-bundle-only", "-output", bundle, "-upstream", *upstream)
	generate.Dir = root
	generate.Env = hostGoEnvironment(os.Environ())
	generate.Stdout = os.Stdout
	generate.Stderr = os.Stderr
	if err := generate.Run(); err != nil {
		fatal(fmt.Errorf("generate runtime assets: %w", err))
	}
	embedSource := filepath.Join(temporary, "embedded_assets_bundle.go")
	if err := os.WriteFile(embedSource, []byte("package harness\n\nimport _ \"embed\"\n\n//go:embed embedded-assets.tar.zst\nvar embeddedAssetBundleData []byte\n\nfunc embeddedAssetBundleBytes() []byte { return embeddedAssetBundleData }\n"), 0o600); err != nil {
		fatal(err)
	}

	overlayPath := filepath.Join(temporary, "overlay.json")
	assetTarget := filepath.Join(root, "internal", "harness", "embedded-assets.tar.zst")
	sourceTarget := filepath.Join(root, "internal", "harness", "embedded_assets_bundle.go")
	overlay, err := json.Marshal(struct {
		Replace map[string]string `json:"Replace"`
	}{Replace: map[string]string{assetTarget: bundle, sourceTarget: embedSource}})
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(overlayPath, overlay, 0o600); err != nil {
		fatal(err)
	}

	goArgs := append([]string{args[0], "-overlay", overlayPath}, args[1:]...)
	command := exec.Command("go", goArgs...)
	command.Dir = root
	command.Env = targetGoEnvironment(os.Environ())
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			os.Exit(exit.ExitCode())
		}
		fatal(err)
	}
}

func targetGoEnvironment(environment []string) []string {
	goos, goarch := os.Getenv("DSH_TARGET_GOOS"), os.Getenv("DSH_TARGET_GOARCH")
	if goos == "" && goarch == "" {
		return environment
	}
	result := make([]string, 0, len(environment)+2)
	for _, entry := range environment {
		if (goos != "" && len(entry) >= 5 && entry[:5] == "GOOS=") || (goarch != "" && len(entry) >= 7 && entry[:7] == "GOARCH=") {
			continue
		}
		result = append(result, entry)
	}
	if goos != "" {
		result = append(result, "GOOS="+goos)
	}
	if goarch != "" {
		result = append(result, "GOARCH="+goarch)
	}
	return result
}

func hostGoEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment)+2)
	for _, entry := range environment {
		if (len(entry) >= 5 && entry[:5] == "GOOS=") || (len(entry) >= 7 && entry[:7] == "GOARCH=") {
			continue
		}
		result = append(result, entry)
	}
	result = append(result, "GOOS="+runtime.GOOS, "GOARCH="+runtime.GOARCH)
	return result
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
