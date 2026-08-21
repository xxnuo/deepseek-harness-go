package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"
)

type E2BFSInfo struct {
	Version string
	Type    E2BFileType
	Size    int64
}

type E2BFSWriteIntent struct {
	Kind    string
	Version string
}

type E2BFSWriteOutcome struct {
	Operation string
	Version   string
	Before    string
	After     string
}

type E2BFileSystem struct {
	Runtime *E2BRuntime
	mu      sync.Mutex
	locks   map[string]*sync.Mutex
}

func NewE2BFileSystem(runtime *E2BRuntime) *E2BFileSystem {
	return &E2BFileSystem{Runtime: runtime, locks: map[string]*sync.Mutex{}}
}

func (f *E2BFileSystem) Resolve(ctx context.Context, requested string) (string, error) {
	if strings.TrimSpace(requested) == "" {
		return "", errors.New("file_path must be a non-empty string")
	}
	if f == nil || f.Runtime == nil {
		return "", errors.New("dsh-e2b: filesystem has no runtime")
	}
	base := f.Runtime.CWD
	if path.IsAbs(requested) {
		return path.Clean(requested), nil
	}
	return path.Join(base, requested), nil
}

func (f *E2BFileSystem) Stat(ctx context.Context, target string) (E2BFSInfo, error) {
	sandbox, err := f.Runtime.GetSandbox(ctx)
	if err != nil {
		return E2BFSInfo{}, err
	}
	entry, err := sandbox.Files().GetInfo(ctx, target)
	if err != nil {
		return E2BFSInfo{}, mapE2BFS(err, target, "stat")
	}
	return E2BFSInfo{Version: e2bVersion(entry), Type: entry.Type, Size: entry.Size}, nil
}

func (f *E2BFileSystem) ReadText(ctx context.Context, target string) (string, error) {
	sandbox, err := f.Runtime.GetSandbox(ctx)
	if err != nil {
		return "", err
	}
	b, err := sandbox.Files().Read(ctx, target)
	if err != nil {
		return "", mapE2BFS(err, target, "read")
	}
	if strings.IndexByte(string(b), 0) >= 0 {
		return "", fmt.Errorf("cannot read %q: binary file", target)
	}
	return string(b), nil
}

func (f *E2BFileSystem) ReadBytes(ctx context.Context, target string, maxBytes int64) ([]byte, error) {
	sandbox, err := f.Runtime.GetSandbox(ctx)
	if err != nil {
		return nil, err
	}
	b, err := sandbox.Files().Read(ctx, target)
	if err != nil {
		return nil, mapE2BFS(err, target, "read")
	}
	if maxBytes >= 0 && int64(len(b)) > maxBytes {
		return nil, fmt.Errorf("cannot read %q: content exceeds %d bytes", target, maxBytes)
	}
	return b, nil
}

func (f *E2BFileSystem) ListDir(ctx context.Context, target string) ([]E2BEntryInfo, error) {
	sandbox, err := f.Runtime.GetSandbox(ctx)
	if err != nil {
		return nil, err
	}
	entries, err := sandbox.Files().List(ctx, target, 1)
	if err != nil {
		return nil, mapE2BFS(err, target, "list")
	}
	return entries, nil
}

func (f *E2BFileSystem) WriteText(ctx context.Context, target, content string, intent *E2BFSWriteIntent) (E2BFSWriteOutcome, error) {
	lock := f.targetLock(target)
	lock.Lock()
	defer lock.Unlock()
	sandbox, err := f.Runtime.GetSandbox(ctx)
	if err != nil {
		return E2BFSWriteOutcome{}, err
	}
	var before string
	existing, statErr := sandbox.Files().GetInfo(ctx, target)
	if statErr == nil {
		if existing.Type != E2BFile {
			return E2BFSWriteOutcome{}, errors.New("target is not a regular file")
		}
		beforeBytes, readErr := sandbox.Files().Read(ctx, target)
		if readErr != nil {
			return E2BFSWriteOutcome{}, readErr
		}
		before = string(beforeBytes)
	} else if intent != nil && intent.Kind == "replaceIfVersion" {
		return E2BFSWriteOutcome{}, errors.New("FS_STALE_VERSION: file changed since it was read")
	}
	if intent != nil && intent.Kind == "createIfAbsent" && statErr == nil {
		return E2BFSWriteOutcome{}, errors.New("FS_NOT_OBSERVED: cannot overwrite existing file")
	}
	if intent != nil && intent.Kind == "replaceIfVersion" && (statErr != nil || e2bVersion(existing) != intent.Version) {
		return E2BFSWriteOutcome{}, errors.New("FS_STALE_VERSION: file changed since it was read")
	}
	entry, err := sandbox.Files().Write(ctx, target, []byte(content), map[string]string{"dsh-version": fmt.Sprintf("%d", time.Now().UnixNano())})
	if err != nil {
		return E2BFSWriteOutcome{}, mapE2BFS(err, target, "write")
	}
	operation := "update"
	if statErr != nil {
		operation = "create"
	}
	return E2BFSWriteOutcome{Operation: operation, Version: e2bVersion(entry), Before: before, After: content}, nil
}

func (f *E2BFileSystem) EditText(ctx context.Context, target, oldString, newString, expectedVersion string, replaceAll bool) (E2BFSWriteOutcome, error) {
	lock := f.targetLock(target)
	lock.Lock()
	defer lock.Unlock()
	before, err := f.ReadText(ctx, target)
	if err != nil {
		return E2BFSWriteOutcome{}, err
	}
	sandbox, err := f.Runtime.GetSandbox(ctx)
	if err != nil {
		return E2BFSWriteOutcome{}, err
	}
	info, err := sandbox.Files().GetInfo(ctx, target)
	if err != nil {
		return E2BFSWriteOutcome{}, err
	}
	if expectedVersion != "" && e2bVersion(info) != expectedVersion {
		return E2BFSWriteOutcome{}, errors.New("FS_STALE_VERSION: file changed since it was read")
	}
	if oldString == "" {
		return E2BFSWriteOutcome{}, errors.New("FS_EDIT_NOT_FOUND: old_string must be non-empty")
	}
	count := strings.Count(before, oldString)
	if count == 0 {
		return E2BFSWriteOutcome{}, errors.New("FS_EDIT_NOT_FOUND: old_string was not found")
	}
	if count > 1 && !replaceAll {
		return E2BFSWriteOutcome{}, errors.New("FS_AMBIGUOUS_EDIT: old_string matched multiple times")
	}
	after := before
	if replaceAll {
		after = strings.ReplaceAll(before, oldString, newString)
	} else {
		after = strings.Replace(before, oldString, newString, 1)
	}
	entry, err := sandbox.Files().Write(ctx, target, []byte(after), map[string]string{"dsh-version": fmt.Sprintf("%d", time.Now().UnixNano())})
	if err != nil {
		return E2BFSWriteOutcome{}, mapE2BFS(err, target, "edit")
	}
	return E2BFSWriteOutcome{Operation: "update", Version: e2bVersion(entry), Before: before, After: after}, nil
}

func (f *E2BFileSystem) targetLock(target string) *sync.Mutex {
	f.mu.Lock()
	defer f.mu.Unlock()
	if lock := f.locks[target]; lock != nil {
		return lock
	}
	lock := &sync.Mutex{}
	f.locks[target] = lock
	return lock
}
func e2bVersion(entry E2BEntryInfo) string {
	if value := entry.Metadata["dsh-version"]; value != "" {
		return "e2b:" + value
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%d\x00%s\x00%s", entry.Path, entry.Type, entry.Size, entry.ModifiedTime.UTC().Format(time.RFC3339Nano), hex.EncodeToString([]byte(entry.SymlinkTarget)))
	return "e2b:" + hex.EncodeToString(h.Sum(nil))
}
func mapE2BFS(err error, target, op string) error {
	if errors.Is(err, context.Canceled) {
		return err
	}
	return fmt.Errorf("cannot %s %q: %w", op, target, err)
}
