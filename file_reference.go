package harness

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

const (
	DefaultFileReferenceMaxResults = 20
	DefaultFileReferenceMaxEntries = 10_000
)

var DefaultFileReferenceExcludedDirectories = []string{".git", "node_modules"}

const FileReferencePrompt = "Paths prefixed with @ are files explicitly referenced by the user. Use the read tool when their contents are needed; do not claim to have inspected a file before reading it."

// FileReferenceConfig controls bounded path-only discovery below a session's
// workspace. File contents remain behind the read tool.
type FileReferenceConfig struct {
	MaxResults          int
	MaxEntries          int
	ExcludedDirectories []string
}

func defaultFileReferenceConfig() FileReferenceConfig {
	return FileReferenceConfig{
		MaxResults:          DefaultFileReferenceMaxResults,
		MaxEntries:          DefaultFileReferenceMaxEntries,
		ExcludedDirectories: append([]string(nil), DefaultFileReferenceExcludedDirectories...),
	}
}

func normalizeFileReferenceConfig(config FileReferenceConfig) FileReferenceConfig {
	defaults := defaultFileReferenceConfig()
	if config.MaxResults == 0 {
		config.MaxResults = defaults.MaxResults
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = defaults.MaxEntries
	}
	if config.ExcludedDirectories == nil {
		config.ExcludedDirectories = defaults.ExcludedDirectories
	} else {
		config.ExcludedDirectories = append([]string(nil), config.ExcludedDirectories...)
	}
	return config
}

func (config FileReferenceConfig) normalized() (FileReferenceConfig, error) {
	config = normalizeFileReferenceConfig(config)
	if config.MaxResults <= 0 {
		return FileReferenceConfig{}, errors.New("file-reference: maxResults must be a positive integer")
	}
	if config.MaxEntries <= 0 {
		return FileReferenceConfig{}, errors.New("file-reference: maxEntries must be a positive integer")
	}
	for _, name := range config.ExcludedDirectories {
		if name == "" || strings.ContainsAny(name, `/\`) {
			return FileReferenceConfig{}, errors.New("file-reference: excludedDirectories entries must be non-empty directory basenames")
		}
	}
	return config, nil
}

// FileReferenceCandidate is a path-only completion result relative to the
// target session workspace.
type FileReferenceCandidate struct {
	Path string `json:"path"`
	Kind string `json:"kind"`
}

type ActiveFileReferenceToken struct {
	Prefix string
	Query  string
	Quoted bool
}

// ParseActiveFileReferenceToken extracts an @path token ending at a UTF-8 byte
// cursor. An @ embedded inside another token, such as an email address, is not
// a completion trigger.
func ParseActiveFileReferenceToken(line string, cursorByte int) (ActiveFileReferenceToken, bool) {
	if cursorByte < 0 || cursorByte > len(line) || !utf8.ValidString(line[:cursorByte]) {
		return ActiveFileReferenceToken{}, false
	}
	before := line[:cursorByte]
	for opening := strings.LastIndex(before, `@"`); opening >= 0; opening = strings.LastIndex(before[:opening], `@"`) {
		if fileReferenceTokenBoundary(before, opening) && !strings.Contains(before[opening+2:], `"`) {
			return ActiveFileReferenceToken{Prefix: before[opening:], Query: before[opening+2:], Quoted: true}, true
		}
	}
	start := 0
	for index, character := range before {
		if unicode.IsSpace(character) {
			start = index + utf8.RuneLen(character)
		}
	}
	token := before[start:]
	if !strings.HasPrefix(token, "@") {
		return ActiveFileReferenceToken{}, false
	}
	return ActiveFileReferenceToken{Prefix: token, Query: token[1:]}, true
}

func fileReferenceTokenBoundary(text string, index int) bool {
	if index == 0 {
		return true
	}
	previous, _ := utf8.DecodeLastRuneInString(text[:index])
	return unicode.IsSpace(previous)
}

// FormatFileReferenceMention renders a selected path for prompt insertion.
func FormatFileReferenceMention(candidate FileReferenceCandidate, preserveQuote bool) (string, bool) {
	if candidate.Kind != "file" && candidate.Kind != "directory" {
		return "", false
	}
	path := candidate.Path
	if candidate.Kind == "directory" {
		path += "/"
	}
	quoted := preserveQuote
	for _, character := range path {
		if character <= 0x1f || character >= 0x7f && character <= 0x9f || character == '"' {
			return "", false
		}
		quoted = quoted || unicode.IsSpace(character)
	}
	if !quoted {
		return "@" + path, true
	}
	if candidate.Kind == "directory" {
		return `@"` + path, true
	}
	return `@"` + path + `"`, true
}

type fileReferenceIndexGeneration struct {
	cancel     context.CancelFunc
	done       chan struct{}
	candidates []FileReferenceCandidate
	err        error
}

// WorkspaceFileSearch owns the reusable bounded index for one workspace.
// Directory-scoped queries always read live state; bare fuzzy queries reuse the
// current index until Invalidate is called.
type WorkspaceFileSearch struct {
	root       string
	config     FileReferenceConfig
	mu         sync.Mutex
	generation *fileReferenceIndexGeneration
	disposed   bool
}

func NewWorkspaceFileSearch(root string, config FileReferenceConfig) (*WorkspaceFileSearch, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	return &WorkspaceFileSearch{root: root, config: config}, nil
}

func (search *WorkspaceFileSearch) List(ctx context.Context, rawQuery string) ([]FileReferenceCandidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	query := strings.ReplaceAll(rawQuery, `\`, "/")
	slash := strings.LastIndex(query, "/")
	if query == "" || slash >= 0 {
		directory, fragment := "", ""
		if slash >= 0 {
			directory, fragment = query[:slash+1], query[slash+1:]
		}
		search.mu.Lock()
		disposed := search.disposed
		search.mu.Unlock()
		if disposed {
			return []FileReferenceCandidate{}, nil
		}
		return listFileReferenceDirectory(ctx, search.root, directory, fragment, search.config)
	}
	candidates, err := search.index(ctx)
	if err != nil {
		return nil, err
	}
	visible := candidates[:0]
	for _, candidate := range candidates {
		if strings.HasPrefix(query, ".") || strings.Contains(query, "/.") || !pathHasHiddenSegment(candidate.Path) {
			visible = append(visible, candidate)
		}
	}
	return rankFileReferenceCandidates(visible, query, search.config.MaxResults), nil
}

func (search *WorkspaceFileSearch) index(ctx context.Context) ([]FileReferenceCandidate, error) {
	search.mu.Lock()
	if search.disposed {
		search.mu.Unlock()
		return []FileReferenceCandidate{}, nil
	}
	generation := search.generation
	if generation == nil {
		scanCtx, cancel := context.WithCancel(context.Background())
		generation = &fileReferenceIndexGeneration{cancel: cancel, done: make(chan struct{})}
		search.generation = generation
		go func() {
			generation.candidates, generation.err = scanWorkspaceFileReferences(scanCtx, search.root, search.config)
			close(generation.done)
			if generation.err != nil {
				search.mu.Lock()
				if search.generation == generation {
					search.generation = nil
				}
				search.mu.Unlock()
			}
		}()
	}
	search.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-generation.done:
		if generation.err != nil {
			return nil, generation.err
		}
		return append([]FileReferenceCandidate(nil), generation.candidates...), nil
	}
}

func (search *WorkspaceFileSearch) Invalidate() {
	search.mu.Lock()
	generation := search.generation
	search.generation = nil
	search.mu.Unlock()
	if generation != nil {
		generation.cancel()
	}
}

func (search *WorkspaceFileSearch) Close() {
	search.mu.Lock()
	if search.disposed {
		search.mu.Unlock()
		return
	}
	search.disposed = true
	generation := search.generation
	search.generation = nil
	search.mu.Unlock()
	if generation != nil {
		generation.cancel()
	}
}

// ListFileReferenceCandidates discovers paths below the target session cwd.
func (e *Engine) ListFileReferenceCandidates(ctx context.Context, targetID, query string) ([]FileReferenceCandidate, error) {
	session, err := e.getSession(targetID)
	if err != nil {
		return nil, err
	}
	session.mu.Lock()
	root := session.Header.CWD
	session.mu.Unlock()
	if root == "" {
		root = e.cfg.Workspace
	}
	search, err := e.fileReferenceSearch(targetID, root)
	if err != nil {
		return nil, err
	}
	return search.List(ctx, query)
}

func (e *Engine) fileReferenceSearch(targetID, root string) (*WorkspaceFileSearch, error) {
	e.fileReferenceMu.Lock()
	defer e.fileReferenceMu.Unlock()
	if search := e.fileReferenceSearches[targetID]; search != nil {
		return search, nil
	}
	search, err := NewWorkspaceFileSearch(root, e.cfg.FileReference)
	if err != nil {
		return nil, err
	}
	e.fileReferenceSearches[targetID] = search
	return search, nil
}

func (e *Engine) invalidateFileReferenceSearch(targetID string) {
	e.fileReferenceMu.Lock()
	search := e.fileReferenceSearches[targetID]
	e.fileReferenceMu.Unlock()
	if search != nil {
		search.Invalidate()
	}
}

func (e *Engine) releaseFileReferenceSearch(targetID string) {
	e.fileReferenceMu.Lock()
	search := e.fileReferenceSearches[targetID]
	delete(e.fileReferenceSearches, targetID)
	e.fileReferenceMu.Unlock()
	if search != nil {
		search.Close()
	}
}

func (e *Engine) closeFileReferenceSearches() {
	e.fileReferenceMu.Lock()
	searches := make([]*WorkspaceFileSearch, 0, len(e.fileReferenceSearches))
	for _, search := range e.fileReferenceSearches {
		searches = append(searches, search)
	}
	e.fileReferenceSearches = map[string]*WorkspaceFileSearch{}
	e.fileReferenceMu.Unlock()
	for _, search := range searches {
		search.Close()
	}
}

// SearchWorkspaceFileReferences performs the reusable bounded filesystem
// search without requiring an Engine.
func SearchWorkspaceFileReferences(ctx context.Context, root, rawQuery string, config FileReferenceConfig) ([]FileReferenceCandidate, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	query := strings.ReplaceAll(rawQuery, `\`, "/")
	slash := strings.LastIndex(query, "/")
	if query == "" || slash >= 0 {
		directory, fragment := "", ""
		if slash >= 0 {
			directory, fragment = query[:slash+1], query[slash+1:]
		}
		return listFileReferenceDirectory(ctx, root, directory, fragment, config)
	}
	candidates, err := scanWorkspaceFileReferences(ctx, root, config)
	if err != nil {
		return nil, err
	}
	visible := candidates[:0]
	for _, candidate := range candidates {
		if strings.HasPrefix(query, ".") || strings.Contains(query, "/.") || !pathHasHiddenSegment(candidate.Path) {
			visible = append(visible, candidate)
		}
	}
	return rankFileReferenceCandidates(visible, query, config.MaxResults), nil
}

func scanWorkspaceFileReferences(ctx context.Context, root string, config FileReferenceConfig) ([]FileReferenceCandidate, error) {
	type directory struct {
		absolute string
		relative string
	}
	excluded := stringSet(config.ExcludedDirectories)
	directories := []directory{{absolute: root}}
	indexed := make([]FileReferenceCandidate, 0, min(config.MaxEntries, 256))
	for cursor := 0; cursor < len(directories) && len(indexed) < config.MaxEntries; cursor++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entries, err := readFileReferenceDirectory(ctx, directories[cursor].absolute)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			kind, ok := fileReferenceEntryKind(entry)
			if !ok {
				continue
			}
			path := entry.Name()
			if directories[cursor].relative != "" {
				path = directories[cursor].relative + "/" + path
			}
			if kind == "directory" {
				if excluded[entry.Name()] {
					continue
				}
				directories = append(directories, directory{
					absolute: filepath.Join(directories[cursor].absolute, entry.Name()),
					relative: path,
				})
			}
			indexed = append(indexed, FileReferenceCandidate{Path: path, Kind: kind})
			if len(indexed) >= config.MaxEntries {
				break
			}
		}
	}
	return indexed, nil
}

func listFileReferenceDirectory(ctx context.Context, root, displayDirectory, fragment string, config FileReferenceConfig) ([]FileReferenceCandidate, error) {
	excluded := stringSet(config.ExcludedDirectories)
	for _, segment := range strings.Split(strings.TrimSuffix(displayDirectory, "/"), "/") {
		if excluded[segment] {
			return []FileReferenceCandidate{}, nil
		}
	}
	absolute, ok, err := resolveFileReferenceDirectory(ctx, root, displayDirectory)
	if err != nil || !ok {
		return []FileReferenceCandidate{}, err
	}
	entries, err := readFileReferenceDirectory(ctx, absolute)
	if err != nil {
		return nil, err
	}
	candidates := make([]FileReferenceCandidate, 0, len(entries))
	for _, entry := range entries {
		if entry.Name() != "" && entry.Name()[0] == '.' && !strings.HasPrefix(fragment, ".") {
			continue
		}
		kind, ok := fileReferenceEntryKind(entry)
		if !ok || kind == "directory" && excluded[entry.Name()] {
			continue
		}
		candidates = append(candidates, FileReferenceCandidate{Path: displayDirectory + entry.Name(), Kind: kind})
	}
	return rankFileReferenceCandidates(candidates, fragment, config.MaxResults), nil
}

func resolveFileReferenceDirectory(ctx context.Context, root, displayDirectory string) (string, bool, error) {
	absolute := filepath.Clean(filepath.Join(root, filepath.FromSlash(displayDirectory)))
	relative, err := filepath.Rel(root, absolute)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", false, err
	}
	current := root
	if relative == "." {
		return current, true, nil
	}
	for _, segment := range strings.Split(relative, string(filepath.Separator)) {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		current = filepath.Join(current, segment)
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", false, nil
		}
	}
	return current, true, nil
}

func readFileReferenceDirectory(ctx context.Context, path string) ([]os.DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return []os.DirEntry{}, nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func fileReferenceEntryKind(entry os.DirEntry) (string, bool) {
	if entry.Type()&os.ModeSymlink != 0 {
		return "", false
	}
	if entry.IsDir() {
		return "directory", true
	}
	if entry.Type().IsRegular() {
		return "file", true
	}
	info, err := entry.Info()
	if err == nil && info.Mode().IsRegular() {
		return "file", true
	}
	return "", false
}

func rankFileReferenceCandidates(candidates []FileReferenceCandidate, query string, limit int) []FileReferenceCandidate {
	type ranked struct {
		candidate FileReferenceCandidate
		score     int
	}
	rows := make([]ranked, 0, len(candidates))
	for _, candidate := range candidates {
		if score, ok := fileReferenceCandidateScore(candidate, query); ok {
			rows = append(rows, ranked{candidate: candidate, score: score})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].score != rows[j].score {
			return rows[i].score > rows[j].score
		}
		if rows[i].candidate.Kind != rows[j].candidate.Kind {
			return rows[i].candidate.Kind == "directory"
		}
		if query != "" && len(rows[i].candidate.Path) != len(rows[j].candidate.Path) {
			return len(rows[i].candidate.Path) < len(rows[j].candidate.Path)
		}
		return rows[i].candidate.Path < rows[j].candidate.Path
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	result := make([]FileReferenceCandidate, len(rows))
	for index := range rows {
		result[index] = rows[index].candidate
	}
	return result
}

func fileReferenceCandidateScore(candidate FileReferenceCandidate, query string) (int, bool) {
	if query == "" {
		return 0, true
	}
	path, needle := strings.ToLower(candidate.Path), strings.ToLower(query)
	name := path
	if slash := strings.LastIndex(path, "/"); slash >= 0 {
		name = path[slash+1:]
	}
	bonus := 0
	if candidate.Kind == "directory" {
		bonus = 25
	}
	switch {
	case name == needle:
		return 1000 + bonus, true
	case strings.HasPrefix(name, needle):
		return 900 + bonus, true
	case strings.Contains(name, needle):
		return 700 + bonus, true
	case strings.Contains(path, needle):
		return 500 + bonus, true
	}
	targetIndex, gap := 0, 0
	for _, character := range needle {
		found := strings.IndexRune(path[targetIndex:], character)
		if found < 0 {
			return 0, false
		}
		gap += found
		targetIndex += found + len(string(character))
	}
	return 300 + max(0, 100-gap) + bonus, true
}

func pathHasHiddenSegment(path string) bool {
	for _, segment := range strings.Split(path, "/") {
		if strings.HasPrefix(segment, ".") {
			return true
		}
	}
	return false
}

func stringSet(values []string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}
