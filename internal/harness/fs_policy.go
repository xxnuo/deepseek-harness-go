package harness

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type fsTarget struct {
	displayPath string
	targetKey   string
}

type fsFileVersion struct {
	info   fs.FileInfo
	digest [sha256.Size]byte
}

type fsObservation struct {
	present bool
	version fsFileVersion
}

type fsWriteIntent struct {
	create  bool
	version fsFileVersion
}

type fsObservationState struct {
	mu       sync.Mutex
	observed map[string]map[string]fsObservation
	locks    map[string]*sync.Mutex
}

func newFSObservationState() *fsObservationState {
	return &fsObservationState{
		observed: make(map[string]map[string]fsObservation),
		locks:    make(map[string]*sync.Mutex),
	}
}

func resolveFSTarget(workspace, requested string) (fsTarget, error) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return fsTarget{}, errors.New("file_path must be a non-empty string")
	}
	path := requested
	if !filepath.IsAbs(path) {
		path = filepath.Join(workspace, path)
	}
	displayPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return fsTarget{}, err
	}
	return fsTarget{displayPath: displayPath, targetKey: canonicalMutationPath(displayPath)}, nil
}

func (s *fsObservationState) lock(targetKey string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock := s.locks[targetKey]
	if lock == nil {
		lock = &sync.Mutex{}
		s.locks[targetKey] = lock
	}
	return lock
}

func (s *fsObservationState) observe(sessionID string, target fsTarget, observation fsObservation) {
	if sessionID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	byTarget := s.observed[sessionID]
	if byTarget == nil {
		byTarget = make(map[string]fsObservation)
		s.observed[sessionID] = byTarget
	}
	byTarget[target.targetKey] = observation
}

func (s *fsObservationState) editIntent(sessionID string, target fsTarget) (fsFileVersion, error) {
	if sessionID == "" {
		return fsFileVersion{}, fsPolicyError("FS_NOT_OBSERVED", fmt.Sprintf("edit requires reading %q first", target.displayPath))
	}
	s.mu.Lock()
	observation, ok := s.observed[sessionID][target.targetKey]
	s.mu.Unlock()
	if !ok {
		return fsFileVersion{}, fsPolicyError("FS_NOT_OBSERVED", fmt.Sprintf("edit requires reading %q first", target.displayPath))
	}
	if !observation.present {
		return fsFileVersion{}, fsPolicyError("FS_NOT_FOUND", fmt.Sprintf("cannot edit %q: not found", target.displayPath))
	}
	return observation.version, nil
}

func (s *fsObservationState) writeIntent(sessionID string, target fsTarget) fsWriteIntent {
	if sessionID == "" {
		return fsWriteIntent{create: true}
	}
	s.mu.Lock()
	observation, ok := s.observed[sessionID][target.targetKey]
	s.mu.Unlock()
	if ok && observation.present {
		return fsWriteIntent{version: observation.version}
	}
	return fsWriteIntent{create: true}
}

func fsPolicyError(code, message string) error {
	return fmt.Errorf("%s: %s", code, message)
}

func readVersionedFile(path string) ([]byte, fsFileVersion, fs.FileMode, error) {
	return readVersionedFileWithin(path, -1)
}

func readVersionedFileWithin(path string, maxBytes int64) ([]byte, fsFileVersion, fs.FileMode, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fsFileVersion{}, 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fsFileVersion{}, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, fsFileVersion{}, info.Mode(), fsPolicyError("FS_NOT_REGULAR_FILE", fmt.Sprintf("cannot read %q: not a regular file", path))
	}
	if maxBytes >= 0 && info.Size() > maxBytes {
		return nil, fsFileVersion{}, info.Mode(), fsPolicyError("FS_TOO_LARGE", fmt.Sprintf("cannot read %q: %d bytes exceeds the %d-byte limit", path, info.Size(), maxBytes))
	}
	reader := io.Reader(file)
	if maxBytes >= 0 {
		reader = io.LimitReader(file, maxBytes+1)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fsFileVersion{}, info.Mode(), err
	}
	if maxBytes >= 0 && int64(len(data)) > maxBytes {
		return nil, fsFileVersion{}, info.Mode(), fsPolicyError("FS_TOO_LARGE", fmt.Sprintf("cannot read %q: content exceeds the %d-byte limit", path, maxBytes))
	}
	return data, fsFileVersion{info: info, digest: sha256.Sum256(data)}, info.Mode(), nil
}

func sameFSVersion(left, right fsFileVersion) bool {
	return left.info != nil && right.info != nil &&
		os.SameFile(left.info, right.info) &&
		left.info.Size() == right.info.Size() &&
		left.info.ModTime().Equal(right.info.ModTime()) &&
		left.info.Mode() == right.info.Mode() &&
		left.digest == right.digest
}

func writeFileAtomic(path string, data []byte, mode fs.FileMode, createIfAbsent bool) (err error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	stagingDir, err := os.MkdirTemp(directory, "."+filepath.Base(path)+".*.tmpdir")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(stagingDir) }()
	if err := os.Chmod(stagingDir, 0o700); err != nil {
		return err
	}
	tempPath := filepath.Join(stagingDir, filepath.Base(path)+".tmp")
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Chmod(mode.Perm()); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	closed = true
	if createIfAbsent {
		if err := os.Link(tempPath, path); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return fsPolicyError("FS_NOT_OBSERVED", fmt.Sprintf("cannot overwrite existing %q without reading it first", path))
			}
			return err
		}
		return nil
	}
	return os.Rename(tempPath, path)
}

func (e *Engine) guardedFSWrite(target fsTarget, data []byte, intent fsWriteIntent) (fsFileVersion, bool, string, error) {
	lock := e.fsState.lock(target.targetKey)
	lock.Lock()
	defer lock.Unlock()

	before, current, mode, err := readVersionedFile(target.targetKey)
	exists := err == nil
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fsFileVersion{}, false, "", err
	}
	if intent.create {
		if exists {
			return fsFileVersion{}, false, "", fsPolicyError("FS_NOT_OBSERVED", fmt.Sprintf("cannot overwrite existing %q without reading it first", target.displayPath))
		}
		mode = 0o644
	} else {
		if !exists || !sameFSVersion(current, intent.version) {
			return fsFileVersion{}, false, "", fsPolicyError("FS_STALE_VERSION", fmt.Sprintf("cannot write %q: file changed since it was read", target.displayPath))
		}
	}
	if err := writeFileAtomic(target.targetKey, data, mode, intent.create); err != nil {
		return fsFileVersion{}, false, "", err
	}
	_, version, _, err := readVersionedFile(target.targetKey)
	return version, exists, string(before), err
}

func (e *Engine) guardedFSEdit(target fsTarget, expected fsFileVersion, apply func(string) (string, error)) (fsFileVersion, error) {
	lock := e.fsState.lock(target.targetKey)
	lock.Lock()
	defer lock.Unlock()

	data, current, mode, err := readVersionedFile(target.targetKey)
	if errors.Is(err, fs.ErrNotExist) {
		return fsFileVersion{}, fsPolicyError("FS_STALE_VERSION", fmt.Sprintf("cannot edit %q: file changed since it was read", target.displayPath))
	}
	if err != nil {
		return fsFileVersion{}, err
	}
	if !sameFSVersion(current, expected) {
		return fsFileVersion{}, fsPolicyError("FS_STALE_VERSION", fmt.Sprintf("cannot edit %q: file changed since it was read", target.displayPath))
	}
	updated, err := apply(string(data))
	if err != nil {
		return fsFileVersion{}, err
	}
	if err := writeFileAtomic(target.targetKey, []byte(updated), mode, false); err != nil {
		return fsFileVersion{}, err
	}
	_, version, _, err := readVersionedFile(target.targetKey)
	return version, err
}

// sandboxMutationPathWithMode resolves a model supplied target and applies
// the same writable-root policy used by the shell sandbox. Reads intentionally
// do not use this helper: only mutations need a write fence.
func (e *Engine) sandboxMutationPathWithMode(call ToolCall, target, mode string) (string, error) {
	if _, ok := sandboxPresetModes[mode]; !ok {
		return "", fmt.Errorf("unknown sandbox mode %q", mode)
	}
	if mode == sandboxReadOnly {
		return "", sandboxDenied(mode, "file mutation")
	}
	workspace, err := filepath.Abs(call.Workspace)
	if err != nil {
		return "", err
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("path is required")
	}
	path := target
	if !filepath.IsAbs(path) {
		path = filepath.Join(workspace, path)
	}
	path, err = filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	if mode == sandboxDangerFull {
		return path, nil
	}
	// Re-check the canonical identity immediately before the mutation. This
	// catches existing symlinks and symlinked ancestors while still allowing
	// creation below a not-yet-existing leaf.
	canonical := canonicalMutationPath(path)
	for _, root := range sandboxWritableRoots(workspace) {
		if sandboxPathUnder(canonical, root) {
			return canonical, nil
		}
	}
	return "", sandboxPolicyDenied(mode, "file mutation")
}

func sandboxWritableRoots(workspace string) []string {
	roots := []string{workspace, "/tmp"}
	if temp := os.TempDir(); temp != "" {
		roots = append(roots, temp)
	}
	out := make([]string, 0, len(roots))
	seen := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		abs, err := filepath.Abs(filepath.Clean(root))
		if err != nil {
			continue
		}
		canonical := canonicalMutationPath(abs)
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		out = append(out, canonical)
	}
	return out
}

func sandboxPathUnder(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// canonicalMutationPath resolves the deepest existing ancestor and appends
// the missing suffix. filepath.EvalSymlinks fails for a new file, but denying
// every new path would make workspace-write unusable; resolving the ancestor
// preserves containment for both existing and newly-created targets.
func canonicalMutationPath(path string) string {
	path = filepath.Clean(path)
	current := path
	suffix := make([]string, 0, 4)
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(suffix) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, suffix[index])
			}
			return filepath.Clean(resolved)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return path
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}
