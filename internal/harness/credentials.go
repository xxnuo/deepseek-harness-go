package harness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	credentialDocumentLockWait = 30 * time.Second
	credentialWatchInterval    = 100 * time.Millisecond
)

type CredentialRef string
type CredentialKey string
type CredentialRecordKind string

const (
	CredentialRecordAPIKey CredentialRecordKind = "api-key"
	CredentialRecordGrant  CredentialRecordKind = "grant"
)

func ParseCredentialRef(value string) (CredentialRef, error) {
	if !validCredentialRef(value) {
		return "", fmt.Errorf("credential ref %q must be a POSIX identifier", value)
	}
	return CredentialRef(value), nil
}

func IsCredentialKeySegment(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for i := 1; i < len(value); i++ {
		c := value[i]
		if c != '-' && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func NewCredentialKey(scope, id string) (CredentialKey, error) {
	if !IsCredentialKeySegment(scope) {
		return "", fmt.Errorf("credential key segment %q must be a lowercase hyphenated identifier", scope)
	}
	if !IsCredentialKeySegment(id) {
		return "", fmt.Errorf("credential key segment %q must be a lowercase hyphenated identifier", id)
	}
	return CredentialKey(scope + "/" + id), nil
}

func ParseCredentialKey(value string) (CredentialKey, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("credential key %q must be <scope>/<id>", value)
	}
	return NewCredentialKey(parts[0], parts[1])
}

func (key CredentialKey) Scope() string {
	value := string(key)
	if i := strings.IndexByte(value, '/'); i >= 0 {
		return value[:i]
	}
	return ""
}

func (key CredentialKey) ID() string {
	value := string(key)
	if i := strings.IndexByte(value, '/'); i >= 0 {
		return value[i+1:]
	}
	return ""
}

type CredentialRecord struct {
	Kind    CredentialRecordKind `json:"kind"`
	Key     string               `json:"key,omitempty"`
	Env     map[string]string    `json:"env,omitempty"`
	Payload any                  `json:"payload,omitempty"`
}

type ResolvedCredential struct {
	Value  string `json:"value"`
	Source string `json:"source"`
}

type CredentialInfo struct {
	Configured bool   `json:"configured"`
	Source     string `json:"source,omitempty"`
	Writable   bool   `json:"writable"`
}

type CredentialRecordInfo struct {
	Configured bool                 `json:"configured"`
	Kind       CredentialRecordKind `json:"kind,omitempty"`
	Writable   bool                 `json:"writable"`
}

type CredentialRecordEntry struct {
	Key  CredentialKey        `json:"key"`
	Kind CredentialRecordKind `json:"kind"`
}

type CredentialEventKind string

const (
	CredentialReferenceUpdated CredentialEventKind = "credentials/reference-updated"
	CredentialRecordUpdated    CredentialEventKind = "credentials/record-updated"
)

type CredentialEvent struct {
	Kind CredentialEventKind `json:"kind"`
	Ref  CredentialRef       `json:"ref,omitempty"`
	Key  CredentialKey       `json:"key,omitempty"`
}

type credentialDocument struct {
	refs        map[string]string
	records     map[CredentialKey]CredentialRecord
	recordOrder []CredentialKey
}

type CredentialService struct {
	engine  *Engine
	write   sync.Mutex
	events  sync.RWMutex
	subs    map[chan CredentialEvent]struct{}
	gen     map[CredentialKey]uint64
	closed  atomic.Bool
	watch   context.CancelFunc
	watchWG sync.WaitGroup
}

func newCredentialService(engine *Engine) *CredentialService {
	return &CredentialService{
		engine: engine,
		subs:   map[chan CredentialEvent]struct{}{},
		gen:    map[CredentialKey]uint64{},
	}
}

func (service *CredentialService) Subscribe(ctx context.Context) <-chan CredentialEvent {
	if ctx == nil {
		ctx = context.Background()
	}
	ch := make(chan CredentialEvent, 16)
	service.events.Lock()
	if service.closed.Load() {
		close(ch)
		service.events.Unlock()
		return ch
	}
	service.subs[ch] = struct{}{}
	service.events.Unlock()
	go func() {
		<-ctx.Done()
		service.events.Lock()
		if _, ok := service.subs[ch]; ok {
			delete(service.subs, ch)
			close(ch)
		}
		service.events.Unlock()
	}()
	return ch
}

func (service *CredentialService) Resolve(ref CredentialRef) (*ResolvedCredential, error) {
	if _, err := ParseCredentialRef(string(ref)); err != nil {
		return nil, err
	}
	value, source, ok := service.engine.resolveCredential(string(ref))
	if !ok {
		return nil, nil
	}
	return &ResolvedCredential{Value: value, Source: source}, nil
}

func (service *CredentialService) Describe(ref CredentialRef) (CredentialInfo, error) {
	if _, err := ParseCredentialRef(string(ref)); err != nil {
		return CredentialInfo{}, err
	}
	configured, source, writable := service.engine.credentialInfo(string(ref))
	return CredentialInfo{Configured: configured, Source: source, Writable: writable}, nil
}

func (service *CredentialService) Set(ctx context.Context, ref CredentialRef, value string) error {
	return service.set(ctx, nil, ref, value)
}

func (service *CredentialService) set(ctx context.Context, origin *dynamicCordisRun, ref CredentialRef, value string) error {
	if _, err := ParseCredentialRef(string(ref)); err != nil {
		return err
	}
	if value == "" {
		return fmt.Errorf("an empty value cannot be stored for %q; use Unset", ref)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	service.write.Lock()
	defer service.write.Unlock()
	if service.closed.Load() {
		return errors.New("credential service is closed")
	}
	if _, shadowed := service.engine.inheritedCredential(string(ref)); shadowed {
		return fmt.Errorf("credential %q is supplied read-only by the environment", ref)
	}
	changed, err := service.mutateDocument(ctx, func(document *credentialDocument, node *yaml.Node) (bool, error) {
		previous, exists := document.refs[string(ref)]
		if exists && previous == value {
			return false, nil
		}
		setCredentialYAMLValue(node, "refs", string(ref), credentialStringNode(value))
		document.refs[string(ref)] = value
		return true, nil
	})
	if err != nil || !changed {
		return err
	}
	return service.notify(origin, CredentialEvent{Kind: CredentialReferenceUpdated, Ref: ref})
}

func (service *CredentialService) Unset(ctx context.Context, ref CredentialRef) error {
	return service.unset(ctx, nil, ref)
}

func (service *CredentialService) unset(ctx context.Context, origin *dynamicCordisRun, ref CredentialRef) error {
	if _, err := ParseCredentialRef(string(ref)); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	service.write.Lock()
	defer service.write.Unlock()
	if service.closed.Load() {
		return errors.New("credential service is closed")
	}
	if _, shadowed := service.engine.inheritedCredential(string(ref)); shadowed {
		return fmt.Errorf("credential %q is supplied read-only by the environment", ref)
	}
	changed, err := service.mutateDocument(ctx, func(document *credentialDocument, node *yaml.Node) (bool, error) {
		if _, exists := document.refs[string(ref)]; !exists {
			return false, nil
		}
		deleteCredentialYAMLValue(node, "refs", string(ref))
		delete(document.refs, string(ref))
		return true, nil
	})
	if err != nil || !changed {
		return err
	}
	return service.notify(origin, CredentialEvent{Kind: CredentialReferenceUpdated, Ref: ref})
}

func (service *CredentialService) ReadRecord(key CredentialKey) (*CredentialRecord, error) {
	if _, err := ParseCredentialKey(string(key)); err != nil {
		return nil, err
	}
	service.engine.mu.RLock()
	record, ok := service.engine.credentialRecords[key]
	service.engine.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	copy := cloneCredentialRecord(record)
	return &copy, nil
}

func (service *CredentialService) DescribeRecord(key CredentialKey) (CredentialRecordInfo, error) {
	record, err := service.ReadRecord(key)
	if err != nil {
		return CredentialRecordInfo{}, err
	}
	if record == nil {
		return CredentialRecordInfo{Writable: true}, nil
	}
	return CredentialRecordInfo{Configured: true, Kind: record.Kind, Writable: true}, nil
}

func (service *CredentialService) ListRecords() []CredentialRecordEntry {
	service.engine.mu.RLock()
	entries := make([]CredentialRecordEntry, 0, len(service.engine.credentialRecordOrder))
	for _, key := range service.engine.credentialRecordOrder {
		if record, ok := service.engine.credentialRecords[key]; ok {
			entries = append(entries, CredentialRecordEntry{Key: key, Kind: record.Kind})
		}
	}
	service.engine.mu.RUnlock()
	return entries
}

func (service *CredentialService) ModifyRecord(
	ctx context.Context,
	key CredentialKey,
	mutate func(context.Context, *CredentialRecord) (*CredentialRecord, error),
) (*CredentialRecord, error) {
	if _, err := ParseCredentialKey(string(key)); err != nil {
		return nil, err
	}
	if mutate == nil {
		return nil, errors.New("credential record mutation is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	service.write.Lock()
	defer service.write.Unlock()
	if service.closed.Load() {
		return nil, errors.New("credential service is closed")
	}
	var result *CredentialRecord
	changed, err := service.mutateDocument(ctx, func(document *credentialDocument, node *yaml.Node) (bool, error) {
		var current *CredentialRecord
		if record, ok := document.records[key]; ok {
			copy := cloneCredentialRecord(record)
			current = &copy
		}
		next, err := mutate(ctx, current)
		if err != nil {
			return false, err
		}
		if next == nil {
			result = current
			return false, nil
		}
		validated, err := validateCredentialRecord(key, *next)
		if err != nil {
			return false, err
		}
		setCredentialYAMLValue(node, "records", string(key), credentialRecordNode(validated))
		if _, exists := document.records[key]; !exists {
			document.recordOrder = append(document.recordOrder, key)
		}
		document.records[key] = validated
		copy := cloneCredentialRecord(validated)
		result = &copy
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if changed {
		if err := service.notify(nil, CredentialEvent{Kind: CredentialRecordUpdated, Key: key}); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (service *CredentialService) DeleteRecord(ctx context.Context, key CredentialKey) error {
	if _, err := ParseCredentialKey(string(key)); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	service.write.Lock()
	defer service.write.Unlock()
	if service.closed.Load() {
		return errors.New("credential service is closed")
	}
	changed, err := service.mutateDocument(ctx, func(document *credentialDocument, node *yaml.Node) (bool, error) {
		if _, exists := document.records[key]; !exists {
			return false, nil
		}
		deleteCredentialYAMLValue(node, "records", string(key))
		delete(document.records, key)
		document.recordOrder = removeCredentialKey(document.recordOrder, key)
		return true, nil
	})
	if err != nil || !changed {
		return err
	}
	return service.notify(nil, CredentialEvent{Kind: CredentialRecordUpdated, Key: key})
}

func (service *CredentialService) recordGeneration(key CredentialKey) uint64 {
	service.events.RLock()
	generation := service.gen[key]
	service.events.RUnlock()
	return generation
}

func (service *CredentialService) startWatcher() {
	if !service.engine.cfg.Persist || service.closed.Load() || service.watch != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	service.watch = cancel
	service.watchWG.Add(1)
	go func() {
		defer service.watchWG.Done()
		path := credentialsYAMLPathFor(service.engine)
		last := ""
		ticker := time.NewTicker(credentialWatchInterval)
		defer ticker.Stop()
		for {
			fingerprint := credentialWatchFingerprint(path)
			if fingerprint != last {
				last = fingerprint
				if err := service.refreshFromDisk(); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("deepseek-harness: credentials reload failed at %s; keeping the last good document: %v", path, err)
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func credentialWatchFingerprint(path string) string {
	data, err := os.ReadFile(path)
	if err == nil {
		return fmt.Sprintf("data:%x", sha256.Sum256(data))
	}
	if errors.Is(err, os.ErrNotExist) {
		return "absent"
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		return "error:" + err.Error() + ":" + statErr.Error()
	}
	return fmt.Sprintf("error:%v:%d:%d:%o", err, info.Size(), info.ModTime().UnixNano(), info.Mode().Perm())
}

func (service *CredentialService) refreshFromDisk() error {
	service.write.Lock()
	if service.closed.Load() {
		service.write.Unlock()
		return context.Canceled
	}
	path := credentialsYAMLPathFor(service.engine)
	if err := assertOwnerOnlyCompat(path); err != nil {
		service.write.Unlock()
		return err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		data = nil
	} else if err != nil {
		service.write.Unlock()
		return err
	}
	next, _, err := parseCredentialYAML(data, path)
	if err != nil {
		service.write.Unlock()
		return err
	}
	service.engine.mu.RLock()
	previous := credentialDocument{
		refs:    cloneCredentialRefs(service.engine.credentials),
		records: cloneCredentialRecords(service.engine.credentialRecords),
	}
	service.engine.mu.RUnlock()
	changedRefs := changedCredentialRefs(previous.refs, next.refs)
	changedRecords := changedCredentialRecords(previous.records, next.records)
	service.replaceSnapshot(next)
	service.write.Unlock()
	for _, ref := range changedRefs {
		if err := service.notify(nil, CredentialEvent{Kind: CredentialReferenceUpdated, Ref: CredentialRef(ref)}); err != nil {
			return err
		}
	}
	for _, key := range changedRecords {
		if err := service.notify(nil, CredentialEvent{Kind: CredentialRecordUpdated, Key: key}); err != nil {
			return err
		}
	}
	return nil
}

func changedCredentialRefs(previous, next map[string]string) []string {
	changed := make([]string, 0)
	seen := make(map[string]bool, len(previous)+len(next))
	for ref, value := range previous {
		seen[ref] = true
		if next[ref] != value {
			changed = append(changed, ref)
		}
	}
	for ref := range next {
		if !seen[ref] {
			changed = append(changed, ref)
		}
	}
	sort.Strings(changed)
	return changed
}

func changedCredentialRecords(previous, next map[CredentialKey]CredentialRecord) []CredentialKey {
	changed := make([]CredentialKey, 0)
	seen := make(map[CredentialKey]bool, len(previous)+len(next))
	for key, record := range previous {
		seen[key] = true
		if !reflect.DeepEqual(record, next[key]) {
			changed = append(changed, key)
		}
	}
	for key := range next {
		if !seen[key] {
			changed = append(changed, key)
		}
	}
	sort.Slice(changed, func(i, j int) bool { return changed[i] < changed[j] })
	return changed
}

func (service *CredentialService) close() {
	if !service.closed.CompareAndSwap(false, true) {
		return
	}
	if service.watch != nil {
		service.watch()
	}
	service.watchWG.Wait()
	service.write.Lock()
	service.write.Unlock()
	service.events.Lock()
	for ch := range service.subs {
		delete(service.subs, ch)
		close(ch)
	}
	service.events.Unlock()
}

func (service *CredentialService) notify(origin *dynamicCordisRun, event CredentialEvent) error {
	service.events.Lock()
	if event.Kind == CredentialRecordUpdated {
		service.gen[event.Key]++
	}
	for ch := range service.subs {
		select {
		case ch <- event:
		default:
		}
	}
	service.events.Unlock()
	if event.Kind == CredentialReferenceUpdated {
		service.engine.emitRemoteEventFrom(origin, string(event.Kind), string(event.Ref))
		return nil
	}
	return service.engine.emitDynamicCordisEventFrom(origin, string(event.Kind), string(event.Key))
}

func (service *CredentialService) mutateDocument(
	ctx context.Context,
	mutate func(*credentialDocument, *yaml.Node) (bool, error),
) (bool, error) {
	if !service.engine.cfg.Persist {
		service.engine.mu.RLock()
		document := credentialDocument{
			refs:        cloneCredentialRefs(service.engine.credentials),
			records:     cloneCredentialRecords(service.engine.credentialRecords),
			recordOrder: append([]CredentialKey(nil), service.engine.credentialRecordOrder...),
		}
		service.engine.mu.RUnlock()
		node := newCredentialYAMLDocument()
		changed, err := mutate(&document, node)
		if err != nil {
			return false, err
		}
		service.replaceSnapshot(document)
		return changed, nil
	}
	path := credentialsYAMLPathFor(service.engine)
	changed := false
	err := withOwnerFileLockContext(ctx, path, credentialDocumentLockWait, func() error {
		if err := assertOwnerOnlyCompat(path); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			data = nil
		} else if err != nil {
			return err
		}
		document, node, err := parseCredentialYAML(data, path)
		if err != nil {
			return err
		}
		changed, err = mutate(&document, node)
		if err != nil {
			return err
		}
		if changed {
			setCredentialDocumentVersion(node)
			encoded, err := yaml.Marshal(node)
			if err != nil {
				return err
			}
			if err := writeOwnerOnlyFile(path, encoded); err != nil {
				return err
			}
		}
		service.replaceSnapshot(document)
		return nil
	})
	return changed, err
}

func (service *CredentialService) replaceSnapshot(document credentialDocument) {
	service.engine.mu.Lock()
	service.engine.credentials = cloneCredentialRefs(document.refs)
	service.engine.credentialRecords = cloneCredentialRecords(document.records)
	service.engine.credentialRecordOrder = append([]CredentialKey(nil), document.recordOrder...)
	service.engine.mu.Unlock()
}

func loadCredentialYAMLDocument(engine *Engine) (bool, error) {
	path := credentialsYAMLPathFor(engine)
	if err := assertOwnerOnlyCompat(path); err != nil {
		return true, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if migrated, ok := renderFlatCredentialMigration(data); ok {
		err = withOwnerFileLockContext(context.Background(), path, credentialDocumentLockWait, func() error {
			current, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if next, recognized := renderFlatCredentialMigration(current); recognized {
				if err := writeOwnerOnlyFile(path, next); err != nil {
					return err
				}
				data = next
				return nil
			}
			data = current
			return nil
		})
		_ = migrated
		if err != nil {
			return true, err
		}
	}
	document, _, err := parseCredentialYAML(data, path)
	if err != nil {
		return true, err
	}
	engine.credentials = cloneCredentialRefs(document.refs)
	engine.credentialRecords = cloneCredentialRecords(document.records)
	engine.credentialRecordOrder = append([]CredentialKey(nil), document.recordOrder...)
	return true, nil
}

func parseCredentialYAML(data []byte, filename string) (credentialDocument, *yaml.Node, error) {
	empty := credentialDocument{refs: map[string]string{}, records: map[CredentialKey]CredentialRecord{}}
	if len(strings.TrimSpace(string(data))) == 0 {
		return empty, newCredentialYAMLDocument(), nil
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return empty, nil, fmt.Errorf("invalid credentials document at %s: %w", filename, err)
	}
	root := credentialYAMLRoot(&node)
	if root == nil {
		return empty, &node, nil
	}
	if root.Kind != yaml.MappingNode {
		return empty, nil, fmt.Errorf("credentials document %s must contain a mapping", filename)
	}
	fields, err := credentialMapping(root, "credentials document", filename)
	if err != nil {
		return empty, nil, err
	}
	if len(fields) == 0 {
		return empty, &node, nil
	}
	version, exists := fields["version"]
	if !exists {
		return empty, nil, fmt.Errorf("credentials document %s uses an unrecognized unversioned layout", filename)
	}
	if version.Kind != yaml.ScalarNode || version.Tag != "!!int" || version.Value != "1" {
		return empty, nil, fmt.Errorf("credentials document %s declares an unsupported version", filename)
	}
	for field := range fields {
		if field != "version" && field != "refs" && field != "records" {
			return empty, nil, fmt.Errorf("credentials document %s has unknown top-level key %q", filename, field)
		}
	}
	if refs := fields["refs"]; refs != nil && !credentialNullNode(refs) {
		pairs, err := credentialMapping(refs, "refs", filename)
		if err != nil {
			return empty, nil, err
		}
		for name, value := range pairs {
			if _, err := ParseCredentialRef(name); err != nil {
				return empty, nil, err
			}
			text, err := credentialYAMLString(value)
			if err != nil || text == "" {
				return empty, nil, fmt.Errorf("credential reference %q in %s must hold a non-empty string", name, filename)
			}
			empty.refs[name] = text
		}
	}
	if records := fields["records"]; records != nil && !credentialNullNode(records) {
		if records.Kind != yaml.MappingNode {
			return empty, nil, fmt.Errorf("records in %s must be a mapping", filename)
		}
		seen := map[string]bool{}
		for i := 0; i < len(records.Content); i += 2 {
			name, err := credentialYAMLString(records.Content[i])
			if err != nil || seen[name] {
				return empty, nil, fmt.Errorf("records in %s contain an invalid or duplicate key", filename)
			}
			seen[name] = true
			key, err := ParseCredentialKey(name)
			if err != nil {
				return empty, nil, err
			}
			record, err := parseCredentialRecordYAML(key, records.Content[i+1], filename)
			if err != nil {
				return empty, nil, err
			}
			empty.records[key] = record
			empty.recordOrder = append(empty.recordOrder, key)
		}
	}
	return empty, &node, nil
}

func parseCredentialRecordYAML(key CredentialKey, node *yaml.Node, filename string) (CredentialRecord, error) {
	fields, err := credentialMapping(node, fmt.Sprintf("record %q", key), filename)
	if err != nil {
		return CredentialRecord{}, err
	}
	kind, err := credentialYAMLString(fields["kind"])
	if err != nil {
		return CredentialRecord{}, fmt.Errorf("record %q in %s has no valid kind", key, filename)
	}
	switch CredentialRecordKind(kind) {
	case CredentialRecordAPIKey:
		for field := range fields {
			if field != "kind" && field != "key" && field != "env" {
				return CredentialRecord{}, fmt.Errorf("record %q in %s has unknown field %q", key, filename, field)
			}
		}
		record := CredentialRecord{Kind: CredentialRecordAPIKey}
		if raw := fields["key"]; raw != nil {
			record.Key, err = credentialYAMLString(raw)
			if err != nil || record.Key == "" {
				return CredentialRecord{}, fmt.Errorf("record %q in %s has a non-string or empty key", key, filename)
			}
		}
		if raw := fields["env"]; raw != nil {
			pairs, err := credentialMapping(raw, fmt.Sprintf("record %q env", key), filename)
			if err != nil {
				return CredentialRecord{}, err
			}
			record.Env = make(map[string]string, len(pairs))
			for name, value := range pairs {
				if _, err := ParseCredentialRef(name); err != nil {
					return CredentialRecord{}, err
				}
				text, err := credentialYAMLString(value)
				if err != nil || text == "" {
					return CredentialRecord{}, fmt.Errorf("record %q env %q in %s must be a non-empty string", key, name, filename)
				}
				record.Env[name] = text
			}
		}
		return validateCredentialRecord(key, record)
	case CredentialRecordGrant:
		for field := range fields {
			if field != "kind" && field != "payload" {
				return CredentialRecord{}, fmt.Errorf("record %q in %s has unknown field %q", key, filename, field)
			}
		}
		payload, exists := fields["payload"]
		if !exists {
			return CredentialRecord{}, fmt.Errorf("record %q in %s has no payload", key, filename)
		}
		var raw any
		if err := payload.Decode(&raw); err != nil {
			return CredentialRecord{}, fmt.Errorf("record %q payload in %s: %w", key, filename, err)
		}
		normalized, err := normalizeYAMLValue(raw)
		if err != nil {
			return CredentialRecord{}, err
		}
		return validateCredentialRecord(key, CredentialRecord{Kind: CredentialRecordGrant, Payload: normalized})
	default:
		return CredentialRecord{}, fmt.Errorf("record %q in %s has unknown kind %q", key, filename, kind)
	}
}

func validateCredentialRecord(key CredentialKey, record CredentialRecord) (CredentialRecord, error) {
	switch record.Kind {
	case CredentialRecordAPIKey:
		if record.Payload != nil {
			return CredentialRecord{}, fmt.Errorf("api-key record %q cannot carry a payload", key)
		}
		for name, value := range record.Env {
			if _, err := ParseCredentialRef(name); err != nil {
				return CredentialRecord{}, err
			}
			if value == "" {
				return CredentialRecord{}, fmt.Errorf("api-key record %q env %q must be non-empty", key, name)
			}
		}
		return cloneCredentialRecord(record), nil
	case CredentialRecordGrant:
		if record.Key != "" || record.Env != nil {
			return CredentialRecord{}, fmt.Errorf("grant record %q cannot carry api-key fields", key)
		}
		if err := validateJSONValue(record.Payload, map[visit]bool{}); err != nil {
			return CredentialRecord{}, fmt.Errorf("record %q payload: %w", key, err)
		}
		return cloneCredentialRecord(record), nil
	default:
		return CredentialRecord{}, fmt.Errorf("record %q has unknown kind %q", key, record.Kind)
	}
}

type visit struct {
	typ reflect.Type
	ptr uintptr
}

func validateJSONValue(value any, seen map[visit]bool) error {
	if value == nil {
		return nil
	}
	v := reflect.ValueOf(value)
	for v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String, reflect.Bool:
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return nil
	case reflect.Float32, reflect.Float64:
		if math.IsInf(v.Float(), 0) || math.IsNaN(v.Float()) {
			return errors.New("holds a non-finite number")
		}
		return nil
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return errors.New("holds a map with non-string keys")
		}
		marker := visit{typ: v.Type(), ptr: v.Pointer()}
		if seen[marker] {
			return errors.New("is cyclic")
		}
		seen[marker] = true
		defer delete(seen, marker)
		iter := v.MapRange()
		for iter.Next() {
			if err := validateJSONValue(iter.Value().Interface(), seen); err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		marker := visit{typ: v.Type(), ptr: v.Pointer()}
		if seen[marker] {
			return errors.New("is cyclic")
		}
		seen[marker] = true
		defer delete(seen, marker)
		for i := 0; i < v.Len(); i++ {
			if err := validateJSONValue(v.Index(i).Interface(), seen); err != nil {
				return err
			}
		}
		return nil
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if err := validateJSONValue(v.Index(i).Interface(), seen); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("holds a value of type %s that JSON cannot represent", v.Type())
	}
}

func renderFlatCredentialMigration(data []byte) ([]byte, bool) {
	text := string(data)
	if strings.TrimSpace(text) == "" {
		return nil, false
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "%") || line == "---" || line == "..." {
			return nil, false
		}
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, false
	}
	root := credentialYAMLRoot(&node)
	if root == nil || root.Kind != yaml.MappingNode || len(root.Content) == 0 {
		return nil, false
	}
	seen := map[string]bool{}
	for i := 0; i < len(root.Content); i += 2 {
		name, err := credentialYAMLString(root.Content[i])
		if err != nil || name == "version" || seen[name] || !validCredentialRef(name) {
			return nil, false
		}
		seen[name] = true
		value, err := credentialYAMLString(root.Content[i+1])
		if err != nil || value == "" {
			return nil, false
		}
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = "  " + line
		}
	}
	body := strings.Join(lines, "\n")
	if strings.HasSuffix(text, "\n") {
		body = strings.TrimSuffix(body, "\n")
	}
	return []byte("version: 1\nrefs:\n" + body + "\n"), true
}

func credentialMapping(node *yaml.Node, subject, filename string) (map[string]*yaml.Node, error) {
	if node == nil || credentialNullNode(node) {
		return map[string]*yaml.Node{}, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s in %s must be a mapping", subject, filename)
	}
	fields := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		name, err := credentialYAMLString(node.Content[i])
		if err != nil || name == "" {
			return nil, fmt.Errorf("%s in %s has a non-string key", subject, filename)
		}
		if _, exists := fields[name]; exists {
			return nil, fmt.Errorf("%s in %s has duplicate key %q", subject, filename, name)
		}
		fields[name] = node.Content[i+1]
	}
	return fields, nil
}

func credentialYAMLString(node *yaml.Node) (string, error) {
	if node == nil || node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return "", errors.New("not a string")
	}
	return node.Value, nil
}

func credentialNullNode(node *yaml.Node) bool {
	return node != nil && node.Kind == yaml.ScalarNode && node.Tag == "!!null"
}

func credentialYAMLRoot(document *yaml.Node) *yaml.Node {
	if document == nil {
		return nil
	}
	if document.Kind == yaml.DocumentNode {
		if len(document.Content) == 0 {
			return nil
		}
		return document.Content[0]
	}
	return document
}

func newCredentialYAMLDocument() *yaml.Node {
	root := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	return &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{root}}
}

func setCredentialDocumentVersion(document *yaml.Node) {
	root := credentialYAMLRoot(document)
	if root == nil {
		root = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		document.Kind = yaml.DocumentNode
		document.Content = []*yaml.Node{root}
	}
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value == "version" {
			root.Content[i+1] = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: "1"}
			return
		}
	}
	root.Content = append([]*yaml.Node{
		{Kind: yaml.ScalarNode, Tag: "!!str", Value: "version"},
		{Kind: yaml.ScalarNode, Tag: "!!int", Value: "1"},
	}, root.Content...)
}

func setCredentialYAMLValue(document *yaml.Node, section, key string, value *yaml.Node) {
	root := credentialYAMLRoot(document)
	if root == nil {
		root = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		document.Kind = yaml.DocumentNode
		document.Content = []*yaml.Node{root}
	}
	var mapping *yaml.Node
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value == section {
			mapping = root.Content[i+1]
			if mapping.Kind != yaml.MappingNode {
				mapping = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
				root.Content[i+1] = mapping
			}
			break
		}
	}
	if mapping == nil {
		mapping = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: section}, mapping)
	}
	for i := 0; i < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			mapping.Content[i+1] = value
			return
		}
	}
	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, value)
}

func deleteCredentialYAMLValue(document *yaml.Node, section, key string) {
	root := credentialYAMLRoot(document)
	if root == nil {
		return
	}
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value != section || root.Content[i+1].Kind != yaml.MappingNode {
			continue
		}
		mapping := root.Content[i+1]
		for j := 0; j < len(mapping.Content); j += 2 {
			if mapping.Content[j].Value == key {
				mapping.Content = append(mapping.Content[:j], mapping.Content[j+2:]...)
				return
			}
		}
	}
}

func credentialStringNode(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

func credentialRecordNode(record CredentialRecord) *yaml.Node {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	appendField := func(name string, value *yaml.Node) {
		node.Content = append(node.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name}, value)
	}
	appendField("kind", credentialStringNode(string(record.Kind)))
	if record.Kind == CredentialRecordAPIKey {
		if record.Key != "" {
			appendField("key", credentialStringNode(record.Key))
		}
		if record.Env != nil {
			env := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
			names := make([]string, 0, len(record.Env))
			for name := range record.Env {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				env.Content = append(env.Content, credentialStringNode(name), credentialStringNode(record.Env[name]))
			}
			appendField("env", env)
		}
		return node
	}
	var payload yaml.Node
	_ = payload.Encode(record.Payload)
	appendField("payload", &payload)
	return node
}

func cloneCredentialRefs(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	copy := make(map[string]string, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func cloneCredentialRecord(record CredentialRecord) CredentialRecord {
	copy := record
	copy.Env = cloneCredentialRefs(record.Env)
	if record.Payload != nil {
		if data, err := json.Marshal(record.Payload); err == nil {
			var payload any
			if json.Unmarshal(data, &payload) == nil {
				copy.Payload = payload
			}
		}
	}
	return copy
}

func cloneCredentialRecords(source map[CredentialKey]CredentialRecord) map[CredentialKey]CredentialRecord {
	copy := make(map[CredentialKey]CredentialRecord, len(source))
	for key, record := range source {
		copy[key] = cloneCredentialRecord(record)
	}
	return copy
}

func removeCredentialKey(keys []CredentialKey, remove CredentialKey) []CredentialKey {
	for i, key := range keys {
		if key == remove {
			return append(keys[:i], keys[i+1:]...)
		}
	}
	return keys
}
