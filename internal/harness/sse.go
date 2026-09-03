package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type streamFrame struct {
	method  string
	payload any
	rpcID   string
}

type pluginBundleWatch struct {
	path    string
	rev     string
	mtimeNS int64
	size    int64
	dirty   bool
}

// streamFrames returns a cancellable stream and owns all subscriptions until
// the request ends. Closing the output happens after watcher goroutines stop,
// which prevents a send racing with channel teardown.
func (e *Engine) streamFrames(ctx context.Context, kind string) <-chan streamFrame {
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan streamFrame, 64)
	host := e.SubscribeHost(ctx)
	mux := e.SubscribeMux(ctx)
	send := func(frame streamFrame) bool {
		select {
		case out <- frame:
			return true
		case <-ctx.Done():
			return false
		}
	}

	go func() {
		defer close(out)
		defer cancel()
		if kind == "host" {
			for _, row := range e.ListSessions() {
				if !send(streamFrame{method: "host/session-added", payload: map[string]any{
					"type": "host/session-added", "sessionId": row.SessionID, "blank": row.Blank,
					"cwd": row.CWD, "agentPreset": row.AgentPreset,
				}}) {
					return
				}
			}
			for {
				select {
				case <-ctx.Done():
					return
				case frame, ok := <-host:
					if !ok {
						return
					}
					if typ, _ := frame["type"].(string); typ != "" && !send(streamFrame{method: typ, payload: frame}) {
						return
					}
				}
			}
		}
		for _, pending := range e.pendingInteractions() {
			if !send(streamFrame{method: pending.method, payload: pending.payload, rpcID: pending.id}) {
				return
			}
		}

		// Subscribe to the host stream before taking the baseline so a session
		// created during startup is added by the same path as later sessions.
		seen := make(map[string]bool)
		var watchers sync.WaitGroup
		startSession := func(id string) {
			if id == "" || seen[id] {
				return
			}
			seen[id] = true
			ch := e.Subscribe(ctx, id)
			baselineLastSeq := -1
			if s, err := e.getSession(id); err == nil {
				s.mu.Lock()
				events := append([]Event(nil), s.Events...)
				s.mu.Unlock()
				baselineLastSeq = len(events) - 1
				if !send(streamFrame{method: "session/subscribed", payload: map[string]any{
					"type": "session/subscribed", "sessionId": id, "lastSeq": len(events) - 1,
				}}) {
					return
				}
				for _, event := range events {
					if !send(streamFrame{method: "session/event", payload: map[string]any{
						"type": "session/event", "sessionId": id, "event": event,
					}}) {
						return
					}
				}
				queue := e.queueFrame(s)
				if items, _ := queue["items"].([]map[string]any); len(items) > 0 {
					if !send(streamFrame{method: "session/queue", payload: queue}) {
						return
					}
				}
			}
			watchers.Add(1)
			go func() {
				defer watchers.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case event, ok := <-ch:
						if !ok {
							return
						}
						// Subscribe happens before the baseline snapshot to avoid a
						// delivery gap. An event can therefore be present in both;
						// sequence numbers are the authoritative deduplication key.
						if int(event.Seq) <= baselineLastSeq {
							continue
						}
						if !send(streamFrame{method: "session/event", payload: map[string]any{
							"type": "session/event", "sessionId": id, "event": event,
						}}) {
							return
						}
					}
				}
			}()
		}
		for _, row := range e.ListSessions() {
			startSession(row.SessionID)
		}
		for {
			select {
			case <-ctx.Done():
				watchers.Wait()
				return
			case frame, ok := <-host:
				if !ok {
					watchers.Wait()
					return
				}
				typ, _ := frame["type"].(string)
				if typ == "host/pending-request" {
					id, _ := frame["rpcId"].(string)
					method, _ := frame["method"].(string)
					if method != "" && id != "" {
						_ = send(streamFrame{method: method, payload: frame["payload"], rpcID: id})
					}
				} else if typ == "host/session-added" {
					if id, _ := frame["sessionId"].(string); id != "" {
						startSession(id)
					}
				}
			case frame, ok := <-mux:
				if !ok {
					watchers.Wait()
					return
				}
				typ, _ := frame["type"].(string)
				if typ == "host/pending-request" {
					id, _ := frame["rpcId"].(string)
					method, _ := frame["method"].(string)
					if method != "" && id != "" {
						_ = send(streamFrame{method: method, payload: frame["payload"], rpcID: id})
					}
				} else if typ != "" {
					_ = send(streamFrame{method: typ, payload: frame})
				}
			}
		}
	}()
	return out
}

// servePluginEvents is the HMR-compatible graph and rebuild channel.
func (e *Engine) servePluginEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusNotImplemented)
		return
	}
	snapshot, err := e.buildBootSnapshot()
	if err != nil {
		http.Error(w, "failed to build plugin graph", http.StatusInternalServerError)
		return
	}
	graph := snapshot.graph
	watches := make(map[string]*pluginBundleWatch, len(graph.Entries))
	for index := range graph.Entries {
		entry := &graph.Entries[index]
		baseline, exists := snapshot.baselines[entry.ID]
		watch := &pluginBundleWatch{path: baseline.path, rev: entry.Rev, mtimeNS: baseline.mtimeNS, size: baseline.size, dirty: !exists}
		watches[entry.ID] = watch
	}
	// Catch up writes that landed after the module host captured its baseline
	// but before this HMR endpoint installed its watches.
	for id, watch := range watches {
		stat, statErr := os.Stat(watch.path)
		if statErr != nil {
			watch.dirty = true
			continue
		}
		if watch.mtimeNS == stat.ModTime().UnixNano() && watch.size == stat.Size() {
			continue
		}
		rev, changed, rebuildErr := e.rebuildBootArtifact(id)
		if rebuildErr != nil {
			watch.dirty = true
			continue
		}
		watch.mtimeNS, watch.size, watch.dirty, watch.rev = stat.ModTime().UnixNano(), stat.Size(), false, rev
		if changed {
			watch.rev = rev
		}
	}
	if refreshed, refreshErr := e.buildBootSnapshot(); refreshErr == nil {
		graph = refreshed.graph
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = fmt.Fprint(w, ": connected\n\n")
	data, _ := json.Marshal(map[string]any{"type": "graph", "graph": graph})
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
	flusher.Flush()
	ticker := time.NewTicker(e.cfg.ClientHMRPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			for id, watch := range watches {
				stat, err := os.Stat(watch.path)
				if err != nil {
					watch.dirty = true
					continue
				}
				mtimeNS := stat.ModTime().UnixNano()
				if !watch.dirty && watch.mtimeNS == mtimeNS && watch.size == stat.Size() {
					continue
				}
				rev, changed, rebuildErr := e.rebuildBootArtifact(id)
				if rebuildErr != nil {
					watch.dirty = true
					continue
				}
				shouldNotify := changed || watch.rev != rev
				watch.mtimeNS, watch.size, watch.dirty, watch.rev = mtimeNS, stat.Size(), false, rev
				if !shouldNotify {
					continue
				}
				frame, _ := json.Marshal(map[string]any{"type": "rebuilt", "id": id, "rev": rev})
				if _, err := fmt.Fprintf(w, "data: %s\n\n", frame); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}
}
