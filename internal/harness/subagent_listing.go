package harness

import (
	"context"
	"errors"
	"sort"
	"sync"
)

const subagentColdReadConcurrency = 4

type subagentListingRecord struct {
	header   SessionHeader
	session  *Session
	attached bool
	running  bool
}

type subagentPositionedRecord struct {
	record   subagentListingRecord
	parentID string
	depth    int
}

type subagentListingEntry struct {
	Kind        string
	ID          string
	Mode        string
	Label       string
	Activity    string
	HasChildren bool
	Reason      string
	ParentID    string
	Depth       int
	running     bool
	attached    bool
}

type subagentListingRuntime struct {
	corpus          map[string]subagentListingRecord
	subagentParents map[string]bool
}

type subagentProjectionIdentity struct {
	mode  string
	label string
	seq   int
}

func (e *Engine) prepareSubagentListing(ctx context.Context) (subagentListingRuntime, error) {
	if err := ctx.Err(); err != nil {
		return subagentListingRuntime{}, err
	}
	corpus := map[string]subagentListingRecord{}
	if e.sessionStore != nil {
		snapshots, err := e.sessionStore.ListSnapshots(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return subagentListingRuntime{}, ctxErr
			}
			return subagentListingRuntime{}, err
		}
		if err := ctx.Err(); err != nil {
			return subagentListingRuntime{}, err
		}
		for _, snapshot := range snapshots {
			corpus[snapshot.Header.ID] = subagentListingRecord{header: snapshot.Header}
		}
	}

	e.mu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.mu.RUnlock()
	for _, session := range sessions {
		if err := ctx.Err(); err != nil {
			return subagentListingRuntime{}, err
		}
		session.mu.Lock()
		record := subagentListingRecord{
			header: session.Header, session: session,
			attached: session.attached, running: session.Running,
		}
		session.mu.Unlock()
		if !record.attached {
			continue
		}
		corpus[record.header.ID] = record
	}

	subagentParents := map[string]bool{}
	for _, record := range corpus {
		if record.header.Origin == "subagent" && record.header.ParentSession != "" {
			subagentParents[record.header.ParentSession] = true
		}
	}
	return subagentListingRuntime{corpus: corpus, subagentParents: subagentParents}, nil
}

func compareSubagentListingRecords(left, right subagentListingRecord) bool {
	if left.header.CreatedAt != right.header.CreatedAt {
		return left.header.CreatedAt < right.header.CreatedAt
	}
	return left.header.ID < right.header.ID
}

func directSubagentCandidates(runtime subagentListingRuntime, parentID string) []subagentPositionedRecord {
	result := make([]subagentPositionedRecord, 0)
	for _, record := range runtime.corpus {
		if record.header.ParentSession == parentID && record.header.Origin == "subagent" {
			result = append(result, subagentPositionedRecord{record: record, parentID: parentID, depth: 1})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return compareSubagentListingRecords(result[i].record, result[j].record)
	})
	return result
}

func descendantSubagentCandidates(runtime subagentListingRuntime, rootID string) []subagentPositionedRecord {
	children := map[string][]subagentListingRecord{}
	for _, record := range runtime.corpus {
		if record.header.ParentSession != "" {
			children[record.header.ParentSession] = append(children[record.header.ParentSession], record)
		}
	}
	for parentID := range children {
		sort.Slice(children[parentID], func(i, j int) bool {
			return compareSubagentListingRecords(children[parentID][i], children[parentID][j])
		})
	}
	stack := make([]subagentPositionedRecord, 0, len(children[rootID]))
	for index := len(children[rootID]) - 1; index >= 0; index-- {
		stack = append(stack, subagentPositionedRecord{record: children[rootID][index], parentID: rootID, depth: 1})
	}
	visited := map[string]bool{rootID: true}
	result := make([]subagentPositionedRecord, 0)
	for len(stack) > 0 {
		last := len(stack) - 1
		position := stack[last]
		stack = stack[:last]
		id := position.record.header.ID
		if visited[id] {
			continue
		}
		visited[id] = true
		if position.record.header.Origin == "subagent" {
			result = append(result, position)
		}
		descendants := children[id]
		for index := len(descendants) - 1; index >= 0; index-- {
			stack = append(stack, subagentPositionedRecord{
				record: descendants[index], parentID: id, depth: position.depth + 1,
			})
		}
	}
	return result
}

func subagentIdentityFromProjection(value any) (*subagentProjectionIdentity, bool) {
	if value == nil {
		return nil, true
	}
	record, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	mode, modeOK := record["mode"].(string)
	seq, seqOK := projectionNonnegativeInt(record["seq"])
	if !modeOK || !seqOK || mode != "one-shot" && mode != "continuable" {
		return nil, false
	}
	label, hasLabel := record["label"].(string)
	if mode == "continuable" && !hasLabel {
		return nil, false
	}
	for key := range record {
		if key != "mode" && key != "label" && key != "seq" {
			return nil, false
		}
	}
	if _, present := record["label"]; present && !hasLabel {
		return nil, false
	}
	return &subagentProjectionIdentity{mode: mode, label: label, seq: seq}, true
}

func subagentChildListingEntry(position subagentPositionedRecord, identity *subagentProjectionIdentity, activity string, hasChildren bool) subagentListingEntry {
	return subagentListingEntry{
		Kind: "child", ID: position.record.header.ID, Mode: identity.mode, Label: identity.label,
		Activity: activity, HasChildren: hasChildren, ParentID: position.parentID, Depth: position.depth,
		running: position.record.running, attached: position.record.attached,
	}
}

func subagentDiagnosticListingEntry(position subagentPositionedRecord, reason string) subagentListingEntry {
	return subagentListingEntry{
		Kind: "diagnostic", ID: position.record.header.ID, Reason: reason,
		ParentID: position.parentID, Depth: position.depth,
	}
}

func (e *Engine) resolveLiveSubagentCandidate(position subagentPositionedRecord, hasChildren bool) *subagentListingEntry {
	snapshot, err := e.sessionProjections.Snapshot(position.record.session)
	if err != nil {
		entry := subagentDiagnosticListingEntry(position, "corrupt")
		return &entry
	}
	identity, valid := subagentIdentityFromProjection(snapshot.Values["subagent"])
	if !valid {
		entry := subagentDiagnosticListingEntry(position, "corrupt")
		return &entry
	}
	if identity == nil {
		return nil
	}
	activity := "inactive"
	if position.record.attached {
		activity = "running"
	}
	entry := subagentChildListingEntry(position, identity, activity, hasChildren)
	return &entry
}

func (e *Engine) resolveColdSubagentCandidate(ctx context.Context, position subagentPositionedRecord, hasChildren bool) (subagentListingEntry, error) {
	header := position.record.header
	if e.projectionCache != nil {
		if snapshot, ok := e.projectionCache.medium.identitySnapshot(header, e.sessionProjections.Signature()); ok {
			identity, valid := subagentIdentityFromProjection(snapshot.Values["subagent"])
			if valid && identity != nil && identity.seq >= header.SeedLength {
				return subagentChildListingEntry(position, identity, "inactive", hasChildren), nil
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return subagentListingEntry{}, err
	}
	inspection, err := e.sessionStore.Inspect(ctx, header.ID)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return subagentListingEntry{}, ctxErr
		}
		return subagentDiagnosticListingEntry(position, "unavailable"), nil
	}
	if err := ctx.Err(); err != nil {
		return subagentListingEntry{}, err
	}
	if !sessionQueryHeadersCompatible(inspection.Meta, header) {
		return subagentDiagnosticListingEntry(position, "corrupt"), nil
	}
	snapshot, err := e.sessionProjections.snapshotDetached(inspection.Events)
	if err != nil {
		return subagentDiagnosticListingEntry(position, "corrupt"), nil
	}
	identity, valid := subagentIdentityFromProjection(snapshot.Values["subagent"])
	if !valid || identity == nil {
		return subagentDiagnosticListingEntry(position, "corrupt"), nil
	}
	return subagentChildListingEntry(position, identity, "inactive", hasChildren), nil
}

func (e *Engine) resolveSubagentCandidates(ctx context.Context, runtime subagentListingRuntime, candidates []subagentPositionedRecord) ([]subagentListingEntry, error) {
	rows := make([]*subagentListingEntry, len(candidates))
	type coldJob struct {
		index    int
		position subagentPositionedRecord
	}
	cold := make([]coldJob, 0)
	for index, position := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		hasChildren := runtime.subagentParents[position.record.header.ID]
		if position.record.session != nil {
			rows[index] = e.resolveLiveSubagentCandidate(position, hasChildren)
			continue
		}
		cold = append(cold, coldJob{index: index, position: position})
	}

	if len(cold) > 0 {
		jobs := make(chan coldJob)
		var workers sync.WaitGroup
		var firstErr error
		var errorMu sync.Mutex
		workerCount := subagentColdReadConcurrency
		if len(cold) < workerCount {
			workerCount = len(cold)
		}
		workers.Add(workerCount)
		for worker := 0; worker < workerCount; worker++ {
			go func() {
				defer workers.Done()
				for job := range jobs {
					entry, err := e.resolveColdSubagentCandidate(ctx, job.position, runtime.subagentParents[job.position.record.header.ID])
					if err != nil {
						errorMu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						errorMu.Unlock()
						continue
					}
					value := entry
					rows[job.index] = &value
				}
			}()
		}
		for _, job := range cold {
			select {
			case jobs <- job:
			case <-ctx.Done():
				break
			}
		}
		close(jobs)
		workers.Wait()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		errorMu.Lock()
		err := firstErr
		errorMu.Unlock()
		if err != nil {
			return nil, err
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make([]subagentListingEntry, 0, len(rows))
	for _, row := range rows {
		if row != nil {
			result = append(result, *row)
		}
	}
	return result, nil
}

func (e *Engine) listSubagentEntries(ctx context.Context, rootID string, descendants bool) ([]subagentListingEntry, bool, error) {
	runtime, err := e.prepareSubagentListing(ctx)
	if err != nil {
		return nil, false, err
	}
	candidates := directSubagentCandidates(runtime, rootID)
	if descendants {
		candidates = descendantSubagentCandidates(runtime, rootID)
	}
	entries, err := e.resolveSubagentCandidates(ctx, runtime, candidates)
	if err != nil {
		return nil, false, err
	}
	_, parentAvailable := runtime.corpus[rootID]
	return entries, parentAvailable, nil
}

func subagentListingError(err error) *RPCError {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return rpcError("cancelled", "subagent listing was cancelled", nil)
	}
	return rpcError("subagent-list-failed", err.Error(), nil)
}
