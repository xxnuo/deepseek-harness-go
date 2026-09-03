package harness

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// BootEntry mirrors the upstream client module manifest consumed by the
// original web shell. Keep this wire shape stable; the TS shell rejects a
// missing rev/id/url/rev field before it renders the application.
type BootEntry struct {
	ID          string   `json:"id"`
	URL         string   `json:"url"`
	Rev         string   `json:"rev"`
	Inject      []string `json:"inject,omitempty"`
	Immediately bool     `json:"immediately,omitempty"`
	External    []string `json:"external,omitempty"`
}

type BootBatchPhase string

const (
	BootBatchBootstrap   BootBatchPhase = "bootstrap"
	BootBatchApplication BootBatchPhase = "application"
)

type BootBatch struct {
	Phase   BootBatchPhase `json:"phase"`
	URL     string         `json:"url"`
	Rev     string         `json:"rev"`
	Entries []string       `json:"entries"`
}

type BootGraph struct {
	Rev     string      `json:"rev"`
	Entries []BootEntry `json:"entries"`
	Batches []BootBatch `json:"batches"`
}

type clientSourceMap struct {
	body   []byte
	parsed map[string]any
}

type bootPluginRecord struct {
	entry     BootEntry
	path      string
	bundle    []byte
	sourceMap *clientSourceMap
	baseline  bootArtifactBaseline
}

// bootArtifactBaseline is captured before reading a bundle. The HMR side
// compares this snapshot with the live file when it installs its watcher, so
// a write in the startup-to-watch window cannot be lost.
type bootArtifactBaseline struct {
	path    string
	mtimeNS int64
	size    int64
}

type bootResponse struct {
	body        []byte
	contentType string
}

type bootCombo struct {
	url          string
	rev          string
	entries      []string
	script       []byte
	sourceMap    []byte
	sourceMapURL string
}

type bootSnapshot struct {
	graph                  BootGraph
	paths                  map[string]string
	responses              map[string]bootResponse
	batchResponses         map[string]bootResponse
	previousBatchResponses map[string]bootResponse
	records                map[string]bootPluginRecord
	baselines              map[string]bootArtifactBaseline
	configKey              string
}

const (
	clientModulesID          = "@deepseek-ai/dsh-client-modules"
	maxComboURLBytes         = 3 * 1024
	comboRevisionPlaceholder = "000000000000"
)

var (
	clientSourceMapTrailer = regexp.MustCompile(`(?:\r?\n)?//# sourceMappingURL=[^\r\n]*(?:\r?\n)?$`)
	clientSourceURLTrailer = regexp.MustCompile(`(?:\r?\n)?//# sourceURL=([^\r\n]+)(?:\r?\n)?$`)
	absoluteSourceURL      = regexp.MustCompile(`^[A-Za-z][A-Za-z\d+.-]*:`)
)

type clientPackageManifest struct {
	Name    string                     `json:"name"`
	Exports map[string]json.RawMessage `json:"exports"`
	Dsh     struct {
		Client struct {
			Platform    string   `json:"platform"`
			Inject      []string `json:"inject"`
			Immediately bool     `json:"immediately"`
			External    []string `json:"external"`
		} `json:"client"`
	} `json:"dsh"`
}

// clientExport resolves the package export used by the upstream client build.
// Packages currently use an object with a "default" condition, while custom
// packages may expose the target as a plain string. Restrict the target to the
// package directory so an untrusted package.json cannot escape PluginDir.
func clientExport(pkg clientPackageManifest, packageDir string) (string, bool, error) {
	raw, ok := pkg.Exports["./client"]
	if !ok {
		return "", false, nil
	}
	var direct string
	if err := json.Unmarshal(raw, &direct); err == nil && direct != "" {
		return resolveClientTarget(packageDir, direct)
	}
	var conditional struct {
		Default string `json:"default"`
	}
	if err := json.Unmarshal(raw, &conditional); err != nil || conditional.Default == "" {
		return "", false, errors.New("exports[\"./client\"] must be a string or an object with a default string")
	}
	return resolveClientTarget(packageDir, conditional.Default)
}

func resolveClientTarget(packageDir, target string) (string, bool, error) {
	if filepath.IsAbs(target) {
		return "", false, errors.New("client export must be relative")
	}
	clean := filepath.Clean(target)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", false, errors.New("client export escapes package directory")
	}
	return filepath.Join(packageDir, clean), true, nil
}

// frontendPluginRoots resolves the checked-out TS package trees from the web
// dist directory. Explicit roots let CLI profiles combine embedded packages
// with profile-local client plugins.
func (e *Engine) frontendPluginRoots() []string {
	var roots []string
	add := func(root string) {
		if root == "" {
			return
		}
		for _, existing := range roots {
			if existing == root {
				return
			}
		}
		roots = append(roots, root)
	}
	for _, root := range e.cfg.PluginDirs {
		add(root)
	}
	add(e.cfg.PluginDir)
	if len(roots) > 0 {
		return roots
	}
	root := e.frontendRoot()
	if root != "" {
		if abs, err := filepath.Abs(root); err == nil {
			// apps/web/dist -> repository root/packages
			candidate := filepath.Join(abs, "..", "..", "..", "packages")
			if st, err := os.Stat(candidate); err == nil && st.IsDir() {
				return []string{candidate}
			}
		}
	}
	for _, candidate := range []string{"deepseek-harness/packages", "packages"} {
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return []string{candidate}
		}
	}
	return nil
}

func (e *Engine) clientPluginActive(name string) bool {
	if e.cfg.ClientPlugins == nil {
		return true
	}
	for _, active := range e.cfg.ClientPlugins {
		if active == name {
			return true
		}
	}
	return false
}

func (e *Engine) clientPluginComposed(name string) bool {
	return e.cfg.ClientPlugins != nil && e.clientPluginActive(name)
}

// buildBootGraph derives the alpha.4 client boot wire and the package paths
// used by the HMR watcher.
func (e *Engine) buildBootGraph() (BootGraph, map[string]string, error) {
	snapshot, err := e.buildBootSnapshot()
	if err != nil {
		return BootGraph{}, nil, err
	}
	return snapshot.graph, snapshot.paths, nil
}

// buildBootSnapshot returns the Engine's immutable boot generation. The
// generation is deliberately shared by index rendering, HTTP bundle serving,
// and the HMR endpoint; rebuilding it independently would invalidate URLs
// while a browser is still consuming the prior graph.
func (e *Engine) buildBootSnapshot() (bootSnapshot, error) {
	key := e.bootConfigKey()
	e.bootMu.Lock()
	defer e.bootMu.Unlock()
	if e.bootSnapshot != nil && e.bootSnapshot.configKey == key {
		return *e.bootSnapshot, nil
	}
	snapshot, err := e.buildBootSnapshotFreshLocked(key)
	if err != nil {
		return bootSnapshot{}, err
	}
	if e.bootSnapshot != nil {
		snapshot.previousBatchResponses = e.bootSnapshot.batchResponses
	}
	e.bootSnapshot = &snapshot
	return snapshot, nil
}

// rebuildBootArtifact snapshots one changed bundle and publishes a new boot
// generation. It returns changed=false when the bytes still produce the
// current artifact revision (for example, a metadata-only write).
func (e *Engine) rebuildBootArtifact(id string) (rev string, changed bool, err error) {
	e.bootMu.Lock()
	defer e.bootMu.Unlock()
	key := e.bootConfigKey()
	if e.bootSnapshot == nil || e.bootSnapshot.configKey != key {
		snapshot, buildErr := e.buildBootSnapshotFreshLocked(key)
		if buildErr != nil {
			return "", false, buildErr
		}
		if e.bootSnapshot != nil {
			snapshot.previousBatchResponses = e.bootSnapshot.batchResponses
		}
		e.bootSnapshot = &snapshot
	}
	current := e.bootSnapshot
	record, ok := current.records[id]
	if !ok {
		return "", false, os.ErrNotExist
	}
	stat, err := os.Stat(record.path)
	if err != nil {
		return "", false, err
	}
	content, err := os.ReadFile(record.path)
	if err != nil {
		return "", false, err
	}
	sourceMap, err := readClientSourceMap(record.path)
	if err != nil {
		return "", false, err
	}
	rev = clientArtifactRevision(content, sourceMap)
	if rev == record.entry.Rev {
		record.baseline = bootArtifactBaseline{path: record.path, mtimeNS: stat.ModTime().UnixNano(), size: stat.Size()}
		next := *current
		next.records = cloneBootRecords(current.records)
		next.baselines = cloneBootBaselines(current.baselines)
		next.records[id] = record
		next.baselines[id] = record.baseline
		e.bootSnapshot = &next
		return rev, false, nil
	}
	record.entry.Rev = rev
	record.entry.URL = comboURL([]string{id}, rev, false)
	record.bundle = content
	record.sourceMap = sourceMap
	record.baseline = bootArtifactBaseline{path: record.path, mtimeNS: stat.ModTime().UnixNano(), size: stat.Size()}
	records := make([]bootPluginRecord, 0, len(current.records))
	paths := make(map[string]string, len(current.paths))
	for packageID, item := range current.records {
		if packageID == id {
			item = record
		}
		records = append(records, item)
		paths[packageID] = item.path
	}
	next, err := e.composeBootSnapshotLocked(records, paths, current.configKey)
	if err != nil {
		return "", false, err
	}
	next.previousBatchResponses = current.batchResponses
	e.bootSnapshot = &next
	return rev, true, nil
}

func cloneBootRecords(records map[string]bootPluginRecord) map[string]bootPluginRecord {
	cloned := make(map[string]bootPluginRecord, len(records))
	for id, record := range records {
		record.entry.Inject = append([]string(nil), record.entry.Inject...)
		record.entry.External = append([]string(nil), record.entry.External...)
		record.bundle = append([]byte(nil), record.bundle...)
		if record.sourceMap != nil {
			mapped := &clientSourceMap{body: append([]byte(nil), record.sourceMap.body...), parsed: make(map[string]any, len(record.sourceMap.parsed))}
			for key, value := range record.sourceMap.parsed {
				mapped.parsed[key] = value
			}
			record.sourceMap = mapped
		}
		cloned[id] = record
	}
	return cloned
}

func cloneBootBaselines(baselines map[string]bootArtifactBaseline) map[string]bootArtifactBaseline {
	cloned := make(map[string]bootArtifactBaseline, len(baselines))
	for id, baseline := range baselines {
		cloned[id] = baseline
	}
	return cloned
}

func (e *Engine) bootSnapshotResponses() (current, previous map[string]bootResponse, err error) {
	if _, err := e.buildBootSnapshot(); err != nil {
		return nil, nil, err
	}
	e.bootMu.Lock()
	defer e.bootMu.Unlock()
	return e.bootSnapshot.responses, e.bootSnapshot.previousBatchResponses, nil
}

func (e *Engine) bootConfigKey() string {
	// This key intentionally describes composition inputs, not bundle bytes.
	// Bundle changes are HMR events and must preserve the current generation
	// until rebuilt() publishes a replacement.
	var plugins []string
	if e.cfg.ClientPlugins != nil {
		plugins = append([]string{}, e.cfg.ClientPlugins...)
	}
	value := struct {
		Roots   []string `json:"roots"`
		Plugins []string `json:"plugins"`
	}{Roots: e.frontendPluginRoots(), Plugins: plugins}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func (e *Engine) allocateInitialBootRevision() (string, error) {
	if e.bootInitialRevisionNonce == "" {
		var value [8]byte
		if _, err := rand.Read(value[:]); err != nil {
			return "", fmt.Errorf("client-modules: allocate initial revision nonce: %w", err)
		}
		e.bootInitialRevisionNonce = hex.EncodeToString(value[:])
	}
	revision := fmt.Sprintf("%s-%d", e.bootInitialRevisionNonce, e.bootNextInitialRevision)
	e.bootNextInitialRevision++
	return revision, nil
}

// buildBootSnapshotFreshLocked scans package declarations and captures the
// initial artifact baselines. The caller holds bootMu.
func (e *Engine) buildBootSnapshotFreshLocked(configKey string) (bootSnapshot, error) {
	roots := e.frontendPluginRoots()
	if len(roots) == 0 {
		graph := emptyBootGraph()
		return bootSnapshot{graph: graph, paths: map[string]string{}, responses: map[string]bootResponse{}, batchResponses: map[string]bootResponse{}, previousBatchResponses: map[string]bootResponse{}, records: map[string]bootPluginRecord{}, baselines: map[string]bootArtifactBaseline{}, configKey: configKey}, nil
	}
	var allowed map[string]struct{}
	if e.cfg.ClientPlugins != nil {
		allowed = make(map[string]struct{}, len(e.cfg.ClientPlugins))
		for _, name := range e.cfg.ClientPlugins {
			allowed[name] = struct{}{}
		}
	}
	records := make([]bootPluginRecord, 0)
	paths := make(map[string]string)
	for _, root := range roots {
		if stat, err := os.Stat(root); err != nil || !stat.IsDir() {
			continue
		}
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				if path != root && (d.Name() == "node_modules" || d.Name() == ".git" || d.Name() == "dist") {
					return fs.SkipDir
				}
				return nil
			}
			if d.Name() != "package.json" {
				return nil
			}
			var pkg clientPackageManifest
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(data, &pkg); err != nil || pkg.Name == "" || pkg.Dsh.Client.Platform != "web" {
				return nil
			}
			if allowed != nil {
				if _, ok := allowed[pkg.Name]; !ok {
					return nil
				}
			} else if strings.HasSuffix(pkg.Name, "ui-directory-picker-browse") {
				return nil
			}
			if _, exists := paths[pkg.Name]; exists {
				return nil
			}
			bundle, ok, err := clientExport(pkg, filepath.Dir(path))
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			stat, err := os.Stat(bundle)
			if err != nil {
				return nil
			}
			content, err := os.ReadFile(bundle)
			if err != nil {
				return err
			}
			sourceMap, err := readClientSourceMap(bundle)
			if err != nil {
				return err
			}
			rev, err := e.allocateInitialBootRevision()
			if err != nil {
				return err
			}
			inject := append([]string(nil), pkg.Dsh.Client.Inject...)
			external := append([]string(nil), pkg.Dsh.Client.External...)
			entry := BootEntry{ID: pkg.Name, URL: comboURL([]string{pkg.Name}, rev, false), Rev: rev, Inject: inject, Immediately: pkg.Dsh.Client.Immediately, External: external}
			records = append(records, bootPluginRecord{entry: entry, path: bundle, bundle: content, sourceMap: sourceMap, baseline: bootArtifactBaseline{path: bundle, mtimeNS: stat.ModTime().UnixNano(), size: stat.Size()}})
			paths[pkg.Name] = bundle
			return nil
		})
		if err != nil {
			return bootSnapshot{}, err
		}
	}
	entries := make([]BootEntry, len(records))
	for index := range records {
		entries[index] = records[index].entry
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	entries, err := orderBootEntries(entries)
	if err != nil {
		return bootSnapshot{}, err
	}
	if len(entries) == 0 {
		graph := emptyBootGraph()
		return bootSnapshot{graph: graph, paths: paths, responses: map[string]bootResponse{}, batchResponses: map[string]bootResponse{}, previousBatchResponses: map[string]bootResponse{}, records: map[string]bootPluginRecord{}, baselines: map[string]bootArtifactBaseline{}, configKey: configKey}, nil
	}
	return e.composeBootSnapshotLocked(records, paths, configKey)
}

// composeBootSnapshotLocked creates response bytes from an already captured
// record set. Rebuilds update one record and come through this same path.
func (e *Engine) composeBootSnapshotLocked(records []bootPluginRecord, paths map[string]string, configKey string) (bootSnapshot, error) {
	entries := make([]BootEntry, len(records))
	for index := range records {
		entries[index] = records[index].entry
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	entries, err := orderBootEntries(entries)
	if err != nil {
		return bootSnapshot{}, err
	}
	if len(entries) == 0 {
		graph := emptyBootGraph()
		return bootSnapshot{graph: graph, paths: paths, responses: map[string]bootResponse{}, batchResponses: map[string]bootResponse{}, previousBatchResponses: map[string]bootResponse{}, records: map[string]bootPluginRecord{}, baselines: map[string]bootArtifactBaseline{}, configKey: configKey}, nil
	}
	recordByID := make(map[string]bootPluginRecord, len(records))
	for _, record := range records {
		recordByID[record.entry.ID] = record
	}
	ordered := make([]bootPluginRecord, 0, len(entries))
	for _, entry := range entries {
		record := recordByID[entry.ID]
		record.entry = entry
		ordered = append(ordered, record)
	}

	bootstrap := make([]bootPluginRecord, 0, 1)
	application := make([]bootPluginRecord, 0, len(ordered))
	for _, record := range ordered {
		if record.entry.ID == clientModulesID {
			bootstrap = append(bootstrap, record)
		} else {
			application = append(application, record)
		}
	}
	responses := make(map[string]bootResponse, len(records)*2+4)
	batchResponses := make(map[string]bootResponse, len(records)*2+4)
	batches := make([]BootBatch, 0, 2)
	for _, phase := range []struct {
		name    BootBatchPhase
		records []bootPluginRecord
	}{{BootBatchBootstrap, bootstrap}, {BootBatchApplication, application}} {
		chunks, err := partitionBootRecords(phase.records)
		if err != nil {
			return bootSnapshot{}, err
		}
		for _, chunk := range chunks {
			combo, err := buildBootCombo(chunk, "")
			if err != nil {
				return bootSnapshot{}, err
			}
			batches = append(batches, BootBatch{Phase: phase.name, URL: combo.url, Rev: combo.rev, Entries: combo.entries})
			addBootComboResponses(batchResponses, combo)
			addBootComboResponses(responses, combo)
		}
	}
	for _, record := range ordered {
		combo, err := buildBootCombo([]bootPluginRecord{record}, record.entry.Rev)
		if err != nil {
			return bootSnapshot{}, err
		}
		addBootComboResponses(responses, combo)
	}
	graphPayload := struct {
		Entries []BootEntry `json:"entries"`
		Batches []BootBatch `json:"batches"`
	}{Entries: entries, Batches: batches}
	encoded, err := json.Marshal(graphPayload)
	if err != nil {
		return bootSnapshot{}, err
	}
	graph := BootGraph{Rev: shortRevisionBytes(encoded), Entries: entries, Batches: batches}
	recordMap := make(map[string]bootPluginRecord, len(ordered))
	baselines := make(map[string]bootArtifactBaseline, len(ordered))
	for _, record := range ordered {
		recordMap[record.entry.ID] = record
		baselines[record.entry.ID] = record.baseline
	}
	return bootSnapshot{graph: graph, paths: paths, responses: responses, batchResponses: batchResponses, records: recordMap, baselines: baselines, configKey: configKey}, nil
}

func emptyBootGraph() BootGraph {
	entries := []BootEntry{}
	batches := []BootBatch{}
	payload, _ := json.Marshal(struct {
		Entries []BootEntry `json:"entries"`
		Batches []BootBatch `json:"batches"`
	}{Entries: entries, Batches: batches})
	return BootGraph{Rev: shortRevisionBytes(payload), Entries: entries, Batches: batches}
}

func readClientSourceMap(bundlePath string) (*clientSourceMap, error) {
	body, err := os.ReadFile(bundlePath + ".map")
	if err != nil {
		return nil, nil
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, nil
	}
	version, versionOK := parsed["version"].(float64)
	sources, sourcesOK := stringArray(parsed["sources"])
	names, namesOK := stringArray(parsed["names"])
	_, mappingsOK := parsed["mappings"].(string)
	if !versionOK || version != 3 || !sourcesOK || !namesOK || !mappingsOK {
		return nil, nil
	}
	parsed["sources"] = sources
	parsed["names"] = names
	return &clientSourceMap{body: body, parsed: parsed}, nil
}

func stringArray(value any) ([]string, bool) {
	raw, ok := value.([]any)
	if !ok {
		if values, typed := value.([]string); typed {
			return append([]string(nil), values...), true
		}
		return nil, false
	}
	values := make([]string, len(raw))
	for index, item := range raw {
		value, ok := item.(string)
		if !ok {
			return nil, false
		}
		values[index] = value
	}
	return values, true
}

func clientArtifactRevision(bundle []byte, sourceMap *clientSourceMap) string {
	parts := [][]byte{bundle}
	if sourceMap != nil {
		parts = append(parts, sourceMap.body)
	}
	return framedRevision("plugin-artifact", parts...)
}

func comboURL(ids []string, revision string, sourceMap bool) string {
	resources := make([]string, len(ids))
	for index, id := range ids {
		resources[index] = id + "/client.js"
		if sourceMap {
			resources[index] += ".map"
		}
	}
	return "/plugins/??" + strings.Join(resources, ",") + "&rev=" + revision
}

func partitionBootRecords(records []bootPluginRecord) ([][]bootPluginRecord, error) {
	chunks := make([][]bootPluginRecord, 0, 1)
	current := make([]bootPluginRecord, 0, len(records))
	for _, record := range records {
		candidate := append(append([]bootPluginRecord(nil), current...), record)
		if projectedComboURLBytes(candidate) <= maxComboURLBytes {
			current = candidate
			continue
		}
		if len(current) == 0 {
			return nil, fmt.Errorf("client-modules: %s exceeds the %d-byte combo URL limit", record.entry.ID, maxComboURLBytes)
		}
		chunks = append(chunks, current)
		current = []bootPluginRecord{record}
		if projectedComboURLBytes(current) > maxComboURLBytes {
			return nil, fmt.Errorf("client-modules: %s exceeds the %d-byte combo URL limit", record.entry.ID, maxComboURLBytes)
		}
	}
	if len(current) > 0 {
		chunks = append(chunks, current)
	}
	return chunks, nil
}

func projectedComboURLBytes(records []bootPluginRecord) int {
	ids := make([]string, len(records))
	for index := range records {
		ids[index] = records[index].entry.ID
	}
	return len([]byte(comboURL(ids, comboRevisionPlaceholder, true)))
}

func buildBootCombo(records []bootPluginRecord, revision string) (bootCombo, error) {
	var source strings.Builder
	sections := make([]map[string]any, 0, len(records))
	line := 0
	ids := make([]string, 0, len(records))
	for _, record := range records {
		prepared, fallback := prepareComboSource(record)
		var sectionMap map[string]any
		if record.sourceMap == nil {
			sectionMap = identitySourceMap(prepared, fallback)
		} else {
			sectionMap = relocateSourceMap(record)
		}
		sections = append(sections, map[string]any{
			"offset": map[string]any{"line": line, "column": 0},
			"map":    sectionMap,
		})
		bundle := prepared + ";\n"
		source.WriteString(bundle)
		line += strings.Count(bundle, "\n")
		ids = append(ids, record.entry.ID)
	}
	indexedMap, err := json.Marshal(map[string]any{"version": 3, "file": "client.js", "sections": sections})
	if err != nil {
		return bootCombo{}, err
	}
	indexedMap = append(indexedMap, '\n')
	sourceBytes := []byte(source.String())
	if revision == "" {
		revision = framedRevision("combo", sourceBytes, indexedMap)
	}
	sourceMapURL := comboURL(ids, revision, true)
	script := append(append([]byte(nil), sourceBytes...), []byte("//# sourceMappingURL="+sourceMapURL+"\n")...)
	return bootCombo{
		url:          comboURL(ids, revision, false),
		rev:          revision,
		entries:      append([]string(nil), ids...),
		script:       script,
		sourceMap:    indexedMap,
		sourceMapURL: sourceMapURL,
	}, nil
}

func prepareComboSource(record bootPluginRecord) (string, string) {
	source := string(record.bundle)
	fallback := "/plugins/" + record.entry.ID + "/client.js"
	if match := clientSourceURLTrailer.FindStringSubmatch(source); match != nil {
		candidate := match[1]
		if strings.HasPrefix(candidate, "/") || absoluteSourceURL.MatchString(candidate) {
			fallback = candidate
		} else {
			fallback = "/" + candidate
		}
	}
	source = clientSourceURLTrailer.ReplaceAllString(source, "")
	source = clientSourceMapTrailer.ReplaceAllString(source, "")
	if !strings.HasSuffix(source, "\n") {
		source += "\n"
	}
	return source, fallback
}

func identitySourceMap(source, sourceURL string) map[string]any {
	lines := strings.Count(source, "\n")
	mappings := make([]string, lines)
	for index := range mappings {
		if index == 0 {
			mappings[index] = "AAAA"
		} else {
			mappings[index] = "AACA"
		}
	}
	return map[string]any{
		"version":        3,
		"names":          []string{},
		"sources":        []string{sourceURL},
		"sourcesContent": []string{source},
		"mappings":       strings.Join(mappings, ";"),
	}
}

func relocateSourceMap(record bootPluginRecord) map[string]any {
	result := make(map[string]any, len(record.sourceMap.parsed))
	for key, value := range record.sourceMap.parsed {
		result[key] = value
	}
	sources, _ := stringArray(record.sourceMap.parsed["sources"])
	sourceRoot, _ := record.sourceMap.parsed["sourceRoot"].(string)
	base := &url.URL{Scheme: "http", Host: "dsh.invalid", Path: "/plugins/" + record.entry.ID + "/client.js.map"}
	relocated := make([]string, len(sources))
	for index, source := range sources {
		separator := ""
		if sourceRoot != "" && !strings.HasSuffix(sourceRoot, "/") && !strings.HasPrefix(source, "/") {
			separator = "/"
		}
		reference, err := url.Parse(sourceRoot + separator + source)
		if err != nil {
			relocated[index] = source
			continue
		}
		resolved := base.ResolveReference(reference)
		if resolved.Scheme == base.Scheme && resolved.Host == base.Host {
			relocated[index] = resolved.EscapedPath()
			if resolved.RawQuery != "" {
				relocated[index] += "?" + resolved.RawQuery
			}
			if resolved.Fragment != "" {
				relocated[index] += "#" + resolved.Fragment
			}
		} else {
			relocated[index] = resolved.String()
		}
	}
	result["sources"] = relocated
	delete(result, "sourceRoot")
	return result
}

func addBootComboResponses(responses map[string]bootResponse, combo bootCombo) {
	responses[combo.url] = bootResponse{body: combo.script, contentType: "text/javascript; charset=utf-8"}
	responses[combo.sourceMapURL] = bootResponse{body: combo.sourceMap, contentType: "application/json; charset=utf-8"}
}

func findPluginPath(e *Engine, id string) (string, bool) {
	_, paths, err := e.buildBootGraph()
	if err != nil {
		return "", false
	}
	path, ok := paths[id]
	return path, ok
}

func framedRevision(domain string, parts ...[]byte) string {
	hash := sha1.New()
	_, _ = hash.Write([]byte(domain))
	_, _ = hash.Write([]byte{0})
	for _, part := range parts {
		_, _ = hash.Write([]byte(strconv.Itoa(len(part))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write(part)
	}
	return hex.EncodeToString(hash.Sum(nil))[:12]
}

func orderBootEntries(entries []BootEntry) ([]BootEntry, error) {
	byID := make(map[string]int, len(entries))
	for index := range entries {
		byID[entries[index].ID] = index
	}
	ordered := make([]BootEntry, 0, len(entries))
	state := make([]uint8, len(entries))
	open := make([]string, 0, len(entries))
	var visit func(int) error
	visit = func(index int) error {
		switch state[index] {
		case 2:
			return nil
		case 1:
			start := 0
			for start < len(open) && open[start] != entries[index].ID {
				start++
			}
			cycle := append(append([]string(nil), open[start:]...), entries[index].ID)
			return errors.New("client-modules: module graph cycle " + strings.Join(cycle, " -> ") + " — a requested package row must precede its consumers, and factory-form CJS cannot deliver partial exports")
		}
		state[index] = 1
		open = append(open, entries[index].ID)
		for _, request := range entries[index].External {
			dependencyID := strings.TrimSuffix(request, "/client")
			dependency, ok := byID[request]
			if !ok {
				dependency, ok = byID[dependencyID]
			}
			if !ok {
				continue
			}
			if dependency == index {
				return errors.New("client-modules: \"" + entries[index].ID + "\" requests module \"" + request + "\" that it answers itself — a row must not declare its own package in dsh.client.external")
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		open = open[:len(open)-1]
		state[index] = 2
		ordered = append(ordered, entries[index])
		return nil
	}
	for index := range entries {
		if err := visit(index); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

func shortRevision(value string) string { return shortRevisionBytes([]byte(value)) }

func shortRevisionBytes(value []byte) string {
	sum := sha1.Sum(value)
	return hex.EncodeToString(sum[:])[:12]
}
