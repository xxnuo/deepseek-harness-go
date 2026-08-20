package harness

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type upstreamInventoryRow struct {
	status  string
	root    string
	files   int
	digest  string
	goFiles []string
}

type upstreamWorkspaceFile struct {
	path string
	blob string
}

func TestUpstreamContractWorkspaceInventory(t *testing.T) {
	repository := repositoryRoot(t)
	upstream := filepath.Join(repository, "deepseek-harness")
	rows := readUpstreamInventory(t, filepath.Join(repository, "upstream_inventory.tsv"))

	wantRoots := make([]string, 0, len(rows))
	byRoot := make(map[string]upstreamInventoryRow, len(rows))
	for _, row := range rows {
		if _, exists := byRoot[row.root]; exists {
			t.Fatalf("duplicate upstream inventory root %q", row.root)
		}
		byRoot[row.root] = row
		wantRoots = append(wantRoots, row.root)
		for _, path := range row.goFiles {
			info, err := os.Stat(filepath.Join(repository, filepath.FromSlash(path)))
			if err != nil || info.IsDir() {
				t.Fatalf("%s Go surface %q is not a file", row.root, path)
			}
		}
	}
	sort.Strings(wantRoots)
	assertStringSet(t, "upstream workspace packages", wantRoots, upstreamWorkspaceRoots(t, upstream))

	owned := upstreamWorkspaceFiles(t, upstream, wantRoots)
	var changed []string
	for _, root := range wantRoots {
		row := byRoot[root]
		if row.status == "frontend" {
			continue
		}
		files := owned[root]
		digest := upstreamFileContentDigest(files)
		if len(files) != row.files || digest != row.digest {
			goSurface := "-"
			if len(row.goFiles) > 0 {
				goSurface = strings.Join(row.goFiles, ",")
			}
			changed = append(changed, strings.Join([]string{row.status, root, strconv.Itoa(len(files)), digest, goSurface}, "\t"))
		}
	}
	if len(changed) > 0 {
		t.Fatalf("upstream tracked-file sets changed; review each package, then replace its row in upstream_inventory.tsv:\n%s", strings.Join(changed, "\n"))
	}
}

func readUpstreamInventory(t *testing.T, path string) []upstreamInventoryRow {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	allowed := map[string]bool{
		"frontend": true,
		"hybrid":   true,
		"missing":  true,
		"partial":  true,
		"ported":   true,
		"replaced": true,
		"support":  true,
	}
	var rows []upstreamInventoryRow
	scanner := bufio.NewScanner(file)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 5 {
			t.Fatalf("%s:%d: want five tab-separated fields", path, lineNumber)
		}
		status, root := fields[0], filepath.ToSlash(filepath.Clean(fields[1]))
		if !allowed[status] {
			t.Fatalf("%s:%d: unknown status %q", path, lineNumber, status)
		}
		if status == "missing" || status == "partial" {
			t.Fatalf("%s:%d: incomplete Go migration status %q is not allowed in the verified baseline", path, lineNumber, status)
		}
		if root != fields[1] || root == "." || filepath.IsAbs(root) || strings.HasPrefix(root, "../") {
			t.Fatalf("%s:%d: invalid workspace root %q", path, lineNumber, fields[1])
		}
		row := upstreamInventoryRow{status: status, root: root, digest: fields[3]}
		if status == "frontend" {
			if fields[2] != "-" || fields[3] != "-" || fields[4] != "-" {
				t.Fatalf("%s:%d: frontend rows must use '-' for file inventory and Go surface", path, lineNumber)
			}
		} else {
			row.files, err = strconv.Atoi(fields[2])
			if err != nil || row.files < 1 {
				t.Fatalf("%s:%d: invalid file count %q", path, lineNumber, fields[2])
			}
			decoded, decodeErr := hex.DecodeString(row.digest)
			if decodeErr != nil || len(decoded) != sha256.Size {
				t.Fatalf("%s:%d: invalid sha256 %q", path, lineNumber, row.digest)
			}
			if fields[4] != "-" {
				row.goFiles = strings.Split(fields[4], ",")
			}
		}
		if (status == "hybrid" || status == "ported" || status == "partial" || status == "replaced") && len(row.goFiles) == 0 {
			t.Fatalf("%s:%d: %s row requires a Go surface", path, lineNumber, status)
		}
		if (status == "missing" || status == "support") && fields[4] != "-" {
			t.Fatalf("%s:%d: %s row must not claim a Go surface", path, lineNumber, status)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("upstream inventory is empty")
	}
	return rows
}

func upstreamWorkspaceRoots(t *testing.T, upstream string) []string {
	t.Helper()
	var workspace struct {
		Packages []string `yaml:"packages"`
	}
	data := readTestFile(t, filepath.Join(upstream, "pnpm-workspace.yaml"))
	if err := yaml.Unmarshal(data, &workspace); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, pattern := range workspace.Packages {
		if strings.Contains(pattern, "**") {
			t.Fatalf("unsupported recursive workspace pattern %q", pattern)
		}
		matches, err := filepath.Glob(filepath.Join(upstream, filepath.FromSlash(pattern)))
		if err != nil {
			t.Fatalf("workspace pattern %q: %v", pattern, err)
		}
		for _, match := range matches {
			if info, err := os.Stat(filepath.Join(match, "package.json")); err != nil || info.IsDir() {
				continue
			}
			relative, err := filepath.Rel(upstream, match)
			if err != nil {
				t.Fatal(err)
			}
			seen[filepath.ToSlash(relative)] = true
		}
	}
	roots := make([]string, 0, len(seen))
	for root := range seen {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	return roots
}

func upstreamWorkspaceFiles(t *testing.T, upstream string, roots []string) map[string][]upstreamWorkspaceFile {
	t.Helper()
	command := exec.Command("git", "-C", upstream, "ls-tree", "-r", "-z", "--full-tree", "HEAD", "--")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git ls-tree: %v", err)
	}
	byDepth := append([]string(nil), roots...)
	sort.Slice(byDepth, func(i, j int) bool {
		left, right := strings.Count(byDepth[i], "/"), strings.Count(byDepth[j], "/")
		if left != right {
			return left > right
		}
		return len(byDepth[i]) > len(byDepth[j])
	})
	owned := make(map[string][]upstreamWorkspaceFile, len(roots))
	for _, record := range strings.Split(string(output), "\x00") {
		if record == "" {
			continue
		}
		metadata, raw, ok := strings.Cut(record, "\t")
		fields := strings.Fields(metadata)
		if !ok || len(fields) != 3 || fields[1] != "blob" {
			t.Fatalf("unexpected git ls-tree record %q", record)
		}
		for _, root := range byDepth {
			if raw == root || strings.HasPrefix(raw, root+"/") {
				owned[root] = append(owned[root], upstreamWorkspaceFile{
					path: strings.TrimPrefix(raw, root+"/"),
					blob: fields[2],
				})
				break
			}
		}
	}
	for root := range owned {
		sort.Slice(owned[root], func(i, j int) bool { return owned[root][i].path < owned[root][j].path })
	}
	return owned
}

func upstreamFileContentDigest(files []upstreamWorkspaceFile) string {
	hash := sha256.New()
	for _, file := range files {
		fmt.Fprintf(hash, "%s%c%s%c", file.path, byte(0), file.blob, byte(0))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func TestUpstreamFileContentDigestTracksPathAndBlob(t *testing.T) {
	baseline := upstreamFileContentDigest([]upstreamWorkspaceFile{{path: "src/index.ts", blob: "aaa"}})
	for _, changed := range [][]upstreamWorkspaceFile{
		{{path: "src/other.ts", blob: "aaa"}},
		{{path: "src/index.ts", blob: "bbb"}},
	} {
		if upstreamFileContentDigest(changed) == baseline {
			t.Fatalf("digest did not change for %#v", changed)
		}
	}
}
