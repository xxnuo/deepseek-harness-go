package harness

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	jobWaitDefault = 30 * time.Second
	jobWaitMax     = 10 * time.Minute
	jobStopGrace   = 2 * time.Second
	jobActiveLimit = 10
)

type jobStatus string

const (
	jobRunning   jobStatus = "running"
	jobStopping  jobStatus = "stopping"
	jobCompleted jobStatus = "completed"
	jobKilled    jobStatus = "killed"
	jobFailed    jobStatus = "failed"
)

func terminalJobStatus(status jobStatus) bool {
	return status == jobCompleted || status == jobKilled || status == jobFailed
}

type jobStream struct {
	mu     sync.Mutex
	data   []byte
	base   int64
	end    int64
	cursor int64
	limit  int
}

func (s *jobStream) Write(p []byte) (int, error) {
	written := len(p)
	s.mu.Lock()
	s.data = append(s.data, p...)
	s.end += int64(len(p))
	limit := s.limit
	if limit <= 0 {
		limit = toolOutputLimit
	}
	if len(s.data) > limit {
		drop := len(s.data) - limit
		s.data = append([]byte(nil), s.data[drop:]...)
		s.base += int64(drop)
	}
	s.mu.Unlock()
	return written, nil
}

func (s *jobStream) read() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lossy := s.cursor < s.base
	if lossy {
		s.cursor = s.base
	}
	start := int(s.cursor - s.base)
	text := string(append([]byte(nil), s.data[start:]...))
	s.cursor = s.end
	return text, lossy
}

type backgroundJob struct {
	id              string
	kind            string
	label           string
	owner           string
	status          jobStatus
	detail          string
	output          string
	startedAt       int64
	finishedAt      int64
	cmd             *exec.Cmd
	childState      shellChildState
	managed         *managedJobHandle
	stdout          *jobStream
	stderr          *jobStream
	done            chan struct{}
	cancelRequested bool
	reported        bool
	waiters         int
	sandboxMode     string
	resultLimit     int
}

type jobSnapshot struct {
	ID          string
	Kind        string
	Label       string
	Owner       string
	Status      jobStatus
	Detail      string
	StartedAt   int64
	FinishedAt  int64
	ResultLimit int
	Reported    bool
}

type managedJobResult struct {
	Status jobStatus
	Detail string
	Output string
}

type managedJobHandle struct {
	Done         <-chan managedJobResult
	Cancel       func(string) error
	CancelInline func(string) error
	ReadOutput   func() (string, bool)
}

type jobRegistry struct {
	mu               sync.Mutex
	jobs             map[string]*backgroundJob
	order            []string
	counters         map[string]int
	starting         map[string]int
	controllerRefs   map[string]map[*dynamicCordisRun]int
	doneListeners    map[uint64]func(jobSnapshot)
	changedListeners map[uint64]func(string)
	nextListener     uint64
	closed           bool
}

func newJobRegistry() *jobRegistry {
	return &jobRegistry{
		jobs:             map[string]*backgroundJob{},
		counters:         map[string]int{},
		starting:         map[string]int{},
		controllerRefs:   map[string]map[*dynamicCordisRun]int{},
		doneListeners:    map[uint64]func(jobSnapshot){},
		changedListeners: map[uint64]func(string){},
	}
}

// attachController records one independently disposable controller reference.
// The registry is shared by all dynamic runs, so admission does not need to
// inspect the dynamic plugin table while it is changing.
func (r *jobRegistry) attachController(owner string, run *dynamicCordisRun) (func(), error) {
	owner = strings.TrimSpace(owner)
	if run == nil {
		return nil, errors.New("job controller run is required")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("background jobs are closed")
	}
	refs := r.controllerRefs[owner]
	if refs == nil {
		refs = map[*dynamicCordisRun]int{}
		r.controllerRefs[owner] = refs
	}
	refs[run]++
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() { r.releaseController(owner, run) })
	}, nil
}

func (r *jobRegistry) releaseController(owner string, run *dynamicCordisRun) {
	r.mu.Lock()
	defer r.mu.Unlock()
	refs := r.controllerRefs[owner]
	if refs == nil {
		return
	}
	if count := refs[run]; count > 1 {
		refs[run] = count - 1
	} else {
		delete(refs, run)
	}
	if len(refs) == 0 {
		delete(r.controllerRefs, owner)
	}
}

// releaseControllers is also called from run cleanup so a forgotten JS
// disposer cannot leave the admission gate open.
func (r *jobRegistry) releaseControllers(owner string, run *dynamicCordisRun) {
	owner = strings.TrimSpace(owner)
	if run == nil {
		return
	}
	r.mu.Lock()
	refs := r.controllerRefs[owner]
	if refs != nil {
		delete(refs, run)
		if len(refs) == 0 {
			delete(r.controllerRefs, owner)
		}
	}
	r.mu.Unlock()
}

// hasController intentionally treats dynamic controllers as process-wide.
// Dynamic runs are host-level compositions in this port; the job owner fence
// still prevents cross-session reads, waits, and kills.
func (r *jobRegistry) hasController(_ string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	for _, refs := range r.controllerRefs {
		if len(refs) != 0 {
			return true
		}
	}
	return false
}

func (r *jobRegistry) onDone(listener func(jobSnapshot)) func() {
	r.mu.Lock()
	r.nextListener++
	id := r.nextListener
	r.doneListeners[id] = listener
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.doneListeners, id)
			r.mu.Unlock()
		})
	}
}

func (r *jobRegistry) onChanged(listener func(string)) func() {
	r.mu.Lock()
	r.nextListener++
	id := r.nextListener
	r.changedListeners[id] = listener
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.changedListeners, id)
			r.mu.Unlock()
		})
	}
}

func (r *jobRegistry) emitChanged(owner string) {
	r.mu.Lock()
	listeners := make([]func(string), 0, len(r.changedListeners))
	for _, listener := range r.changedListeners {
		listeners = append(listeners, listener)
	}
	r.mu.Unlock()
	for _, listener := range listeners {
		listener(owner)
	}
}

func (r *jobRegistry) emitDone(snapshot jobSnapshot) {
	r.mu.Lock()
	listeners := make([]func(jobSnapshot), 0, len(r.doneListeners))
	for _, listener := range r.doneListeners {
		listeners = append(listeners, listener)
	}
	r.mu.Unlock()
	for _, listener := range listeners {
		listener(snapshot)
	}
}

func (r *jobRegistry) start(owner, kind, label, sandboxMode string, cmd *exec.Cmd, childState shellChildState) (string, error) {
	if strings.TrimSpace(kind) == "" || strings.TrimSpace(label) == "" {
		return "", errors.New("background job kind and label are required")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return "", errors.New("background jobs are closed")
	}
	active := r.starting[owner]
	for _, job := range r.jobs {
		if job.owner == owner && !terminalJobStatus(job.status) {
			active++
		}
	}
	if active >= jobActiveLimit {
		r.mu.Unlock()
		return "", fmt.Errorf("background job limit reached for this owner (limit: %d); use job_kill to stop an unneeded job, wait for it to finish, then retry", jobActiveLimit)
	}
	r.starting[owner]++
	r.mu.Unlock()

	stdout, stderr := &jobStream{}, &jobStream{}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	startErr := startShellChild(cmd, childState)

	r.mu.Lock()
	r.starting[owner]--
	if startErr != nil {
		r.mu.Unlock()
		return "", startErr
	}
	if r.closed {
		r.mu.Unlock()
		_ = killChildProcess(cmd)
		_ = waitShellChild(cmd, childState)
		return "", errors.New("background jobs are closed")
	}
	r.counters[kind]++
	id := fmt.Sprintf("%s-%d", kind, r.counters[kind])
	job := &backgroundJob{
		id: id, kind: kind, label: label, owner: owner, status: jobRunning,
		startedAt: time.Now().UnixMilli(), cmd: cmd, childState: childState, stdout: stdout, stderr: stderr, done: make(chan struct{}), sandboxMode: sandboxMode,
	}
	r.jobs[id] = job
	r.order = append(r.order, id)
	r.mu.Unlock()
	r.emitChanged(owner)

	go r.wait(job)
	return id, nil
}

func (r *jobRegistry) startManaged(owner, kind, label string, resultLimit int, start func() (*managedJobHandle, error)) (string, error) {
	if kind == "" || label == "" {
		return "", errors.New("background job kind and label are required")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return "", errors.New("background jobs are closed")
	}
	active := r.starting[owner]
	for _, job := range r.jobs {
		if job.owner == owner && !terminalJobStatus(job.status) {
			active++
		}
	}
	if active >= jobActiveLimit {
		r.mu.Unlock()
		return "", fmt.Errorf("background job limit reached for this owner (limit: %d); use job_kill to stop an unneeded job, wait for it to finish, then retry", jobActiveLimit)
	}
	r.starting[owner]++
	r.mu.Unlock()

	handle, startErr := start()
	r.mu.Lock()
	r.starting[owner]--
	if startErr != nil {
		r.mu.Unlock()
		return "", startErr
	}
	if handle == nil || handle.Done == nil {
		r.mu.Unlock()
		if handle != nil && handle.Cancel != nil {
			_ = handle.Cancel("background job registration failed")
		}
		return "", errors.New("managed background job returned an incomplete handle")
	}
	if r.closed {
		r.mu.Unlock()
		if handle.Cancel != nil {
			if err := handle.Cancel("background jobs are closed"); err != nil {
				return "", errors.New("background jobs are closed")
			}
		}
		<-handle.Done
		return "", errors.New("background jobs are closed")
	}
	r.counters[kind]++
	id := fmt.Sprintf("%s-%d", kind, r.counters[kind])
	job := &backgroundJob{
		id: id, kind: kind, label: label, owner: owner, status: jobRunning,
		startedAt: time.Now().UnixMilli(), managed: handle, done: make(chan struct{}), resultLimit: resultLimit,
	}
	r.jobs[id] = job
	r.order = append(r.order, id)
	r.mu.Unlock()
	r.emitChanged(owner)

	go r.waitManaged(job)
	return id, nil
}

func (r *jobRegistry) wait(job *backgroundJob) {
	err := waitShellChild(job.cmd, job.childState)
	status, detail := jobCompleted, "exit code: 0"
	if exit, ok := err.(*exec.ExitError); ok {
		if wait, ok := exit.Sys().(syscall.WaitStatus); ok && wait.Signaled() {
			status, detail = jobKilled, "signal: "+jobSignalName(wait.Signal())
		} else if signal := shellSignal(exit.ExitCode()); signal != 0 {
			// ponytail: bwrap collapses child signals into 128+signal exits; use a runner status channel if deliberate 13x exits must remain distinguishable.
			status, detail = jobKilled, "signal: "+jobSignalName(signal)
		} else {
			detail = fmt.Sprintf("exit code: %d", exit.ExitCode())
		}
	} else if err != nil {
		status, detail = jobFailed, err.Error()
	}

	r.mu.Lock()
	if job.cancelRequested && status != jobKilled {
		status, detail = jobKilled, "killed before exit"
	}
	job.status = status
	job.detail = detail
	job.finishedAt = time.Now().UnixMilli()
	if job.waiters > 0 {
		job.reported = true
	}
	snapshot := snapshotJob(job)
	r.mu.Unlock()
	r.emitChanged(job.owner)
	r.emitDone(snapshot)
	// Wake waiters after listener callbacks have been queued. This preserves the
	// upstream ordering where a waiter cannot resume before completion observers
	// have seen the committed terminal record.
	close(job.done)
}

func (r *jobRegistry) waitManaged(job *backgroundJob) {
	result, ok := <-job.managed.Done
	if !ok {
		result = managedJobResult{Status: jobFailed, Detail: "managed job ended without a result"}
	}
	if result.Status != jobCompleted && result.Status != jobKilled && result.Status != jobFailed {
		result = managedJobResult{Status: jobFailed, Detail: "managed job returned an invalid status"}
	}
	r.mu.Lock()
	if terminalJobStatus(job.status) {
		r.mu.Unlock()
		return
	}
	job.status, job.detail, job.output = result.Status, result.Detail, result.Output
	job.finishedAt = time.Now().UnixMilli()
	if job.waiters > 0 {
		job.reported = true
	}
	snapshot := snapshotJob(job)
	r.mu.Unlock()
	r.emitChanged(job.owner)
	r.emitDone(snapshot)
	close(job.done)
}

func shellSignal(exitCode int) syscall.Signal {
	switch exitCode {
	case 128 + int(syscall.SIGINT):
		return syscall.SIGINT
	case 128 + int(syscall.SIGKILL):
		return syscall.SIGKILL
	case 128 + int(syscall.SIGTERM):
		return syscall.SIGTERM
	default:
		return 0
	}
}

func jobSignalName(signal syscall.Signal) string {
	switch signal {
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGTERM:
		return "SIGTERM"
	default:
		return signal.String()
	}
}

func (r *jobRegistry) lookupLocked(owner, id string) (*backgroundJob, error) {
	job := r.jobs[id]
	if job == nil {
		return nil, fmt.Errorf("unknown job %s", id)
	}
	if job.owner != "" && job.owner != owner {
		return nil, fmt.Errorf("job %s belongs to another session", id)
	}
	return job, nil
}

func snapshotJob(job *backgroundJob) jobSnapshot {
	return jobSnapshot{
		ID: job.id, Kind: job.kind, Label: job.label, Owner: job.owner, Status: job.status, Detail: job.detail,
		StartedAt: job.startedAt, FinishedAt: job.finishedAt, ResultLimit: job.resultLimit, Reported: job.reported,
	}
}

func (r *jobRegistry) list(owner string) []jobSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	rows := make([]jobSnapshot, 0, len(r.order))
	for _, id := range r.order {
		job := r.jobs[id]
		if job.owner == "" || job.owner == owner {
			rows = append(rows, snapshotJob(job))
		}
	}
	return rows
}

func (r *jobRegistry) read(owner, id string) (string, jobSnapshot, error) {
	r.mu.Lock()
	job, err := r.lookupLocked(owner, id)
	if err != nil {
		r.mu.Unlock()
		return "", jobSnapshot{}, err
	}
	if job.managed != nil && job.managed.ReadOutput == nil {
		out := ""
		if terminalJobStatus(job.status) {
			out = job.output
			job.reported = true
		}
		snapshot := snapshotJob(job)
		r.mu.Unlock()
		return out, snapshot, nil
	}
	r.mu.Unlock()
	var out string
	var outLossy, errLossy bool
	errText := ""
	if job.managed != nil {
		out, outLossy = job.managed.ReadOutput()
	} else {
		out, outLossy = job.stdout.read()
		errText, errLossy = job.stderr.read()
	}
	if errText != "" {
		if out != "" && !strings.HasSuffix(out, "\n") {
			out += "\n"
		}
		out += "[stderr]\n" + errText
	}
	if outLossy || errLossy {
		if out != "" && !strings.HasSuffix(out, "\n") {
			out += "\n"
		}
		out += "[some output was dropped from memory; full output: (unavailable)]"
	}
	if job.sandboxMode != "" && job.sandboxMode != sandboxDangerFull && sandboxOutputDenied(out) {
		out = appendShellStatus(out, sandboxDenialMarker(job.sandboxMode))
		out = appendShellStatus(out, sandboxEscalationHint("command"))
	}
	r.mu.Lock()
	if terminalJobStatus(job.status) {
		job.reported = true
	}
	snapshot := snapshotJob(job)
	r.mu.Unlock()
	return out, snapshot, nil
}

func (r *jobRegistry) waitFor(ctx context.Context, owner, id string, timeout time.Duration) (jobSnapshot, error) {
	r.mu.Lock()
	job, err := r.lookupLocked(owner, id)
	if err != nil {
		r.mu.Unlock()
		return jobSnapshot{}, err
	}
	if terminalJobStatus(job.status) {
		done := job.done
		r.mu.Unlock()
		<-done
		r.mu.Lock()
		job.reported = true
		snapshot := snapshotJob(job)
		r.mu.Unlock()
		return snapshot, nil
	}
	done := job.done
	job.waiters++
	r.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		// Settlement marks the snapshot reported before releasing this waiter.
	case <-timer.C:
		// A timeout is a successful bounded wait; it does not report a live job.
	case <-ctx.Done():
		r.mu.Lock()
		job.waiters--
		if terminalJobStatus(job.status) {
			job.reported = true
			snapshot := snapshotJob(job)
			r.mu.Unlock()
			return snapshot, nil
		}
		r.mu.Unlock()
		return jobSnapshot{}, errors.New("wait aborted")
	}
	r.mu.Lock()
	job.waiters--
	if terminalJobStatus(job.status) {
		job.reported = true
	}
	snapshot := snapshotJob(job)
	r.mu.Unlock()
	return snapshot, nil
}

func (r *jobRegistry) signalProcessLocked(job *backgroundJob, reason string) error {
	if job.cancelRequested {
		return nil
	}
	if err := terminateChildProcess(job.cmd); err != nil {
		return err
	}
	job.cancelRequested = true
	job.status = jobStopping
	done := job.done
	pid := job.cmd.Process.Pid
	go func() {
		timer := time.NewTimer(jobStopGrace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			_ = killChildProcessPID(pid)
		}
	}()
	return nil
}

func (r *jobRegistry) kill(owner, id, reason string) (jobSnapshot, bool, error) {
	return r.killWithMode(owner, id, reason, false)
}

func (r *jobRegistry) killInline(owner, id, reason string) (jobSnapshot, bool, error) {
	return r.killWithMode(owner, id, reason, true)
}

func (r *jobRegistry) killWithMode(owner, id, reason string, inline bool) (jobSnapshot, bool, error) {
	r.mu.Lock()
	job, err := r.lookupLocked(owner, id)
	if err != nil {
		r.mu.Unlock()
		return jobSnapshot{}, false, err
	}
	if terminalJobStatus(job.status) {
		job.reported = true
		snapshot := snapshotJob(job)
		r.mu.Unlock()
		return snapshot, true, nil
	}
	if job.managed == nil {
		if err := r.signalProcessLocked(job, reason); err != nil {
			r.mu.Unlock()
			return jobSnapshot{}, false, err
		}
		job.reported = true
		snapshot := snapshotJob(job)
		changedOwner := job.owner
		r.mu.Unlock()
		go r.emitChanged(changedOwner)
		return snapshot, false, nil
	}
	cancel := job.managed.Cancel
	if inline && job.managed.CancelInline != nil {
		cancel = job.managed.CancelInline
	}
	r.mu.Unlock()
	if cancel != nil {
		if err := cancel(reason); err != nil {
			return jobSnapshot{}, false, err
		}
	}

	r.mu.Lock()
	if !terminalJobStatus(job.status) && !job.cancelRequested {
		job.cancelRequested = true
		job.status = jobStopping
	}
	job.reported = true
	snapshot := snapshotJob(job)
	changedOwner := job.owner
	r.mu.Unlock()
	go r.emitChanged(changedOwner)
	return snapshot, false, nil
}

func (r *jobRegistry) close() {
	type managedCancellation struct {
		job    *backgroundJob
		cancel func(string) error
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.controllerRefs = map[string]map[*dynamicCordisRun]int{}
	done := make([]<-chan struct{}, 0, len(r.jobs))
	managed := make([]managedCancellation, 0, len(r.jobs))
	for _, job := range r.jobs {
		if !terminalJobStatus(job.status) {
			job.reported = true
			if job.managed != nil {
				managed = append(managed, managedCancellation{job: job, cancel: job.managed.Cancel})
			} else {
				_ = r.signalProcessLocked(job, "background jobs are closed")
			}
			done = append(done, job.done)
		}
	}
	r.mu.Unlock()
	for _, pending := range managed {
		var cancelErr error
		if pending.cancel != nil {
			cancelErr = pending.cancel("background jobs are closed")
		}
		r.mu.Lock()
		if !terminalJobStatus(pending.job.status) {
			if cancelErr != nil {
				pending.job.status = jobFailed
				pending.job.detail = "cancel threw during teardown; work may be orphaned: " + cancelErr.Error()
				pending.job.finishedAt = time.Now().UnixMilli()
				close(pending.job.done)
			} else if !pending.job.cancelRequested {
				pending.job.cancelRequested = true
				pending.job.status = jobStopping
			}
		}
		r.mu.Unlock()
	}
	for _, settled := range done {
		<-settled
	}
}

func jobWaitDuration(milliseconds float64) time.Duration {
	if milliseconds >= float64(math.MaxInt64)/float64(time.Millisecond) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(milliseconds * float64(time.Millisecond))
}

func statusLine(job jobSnapshot) string {
	if job.Detail != "" {
		return fmt.Sprintf("[status: %s, %s]", job.Status, job.Detail)
	}
	return fmt.Sprintf("[status: %s]", job.Status)
}

func validateJobID(id string) error {
	if id == "" {
		return errors.New(`invalid job_id: expected a non-empty string, got ""`)
	}
	return nil
}

func publicJobValue(job jobSnapshot) map[string]any {
	value := map[string]any{"id": job.ID, "kind": job.Kind, "label": job.Label, "status": string(job.Status), "startedAt": job.StartedAt}
	if job.Detail != "" {
		value["detail"] = job.Detail
	}
	if job.FinishedAt != 0 {
		value["finishedAt"] = job.FinishedAt
	}
	return value
}

func publicJobSchema() map[string]any {
	return objectSchema(map[string]any{
		"id": map[string]any{"type": "string"}, "kind": map[string]any{"type": "string"}, "label": map[string]any{"type": "string"},
		"status": map[string]any{"type": "string", "enum": []string{"running", "stopping", "completed", "killed", "failed"}},
		"detail": map[string]any{"type": "string"}, "startedAt": map[string]any{"type": "integer"}, "finishedAt": map[string]any{"type": "integer"},
	}, "id", "kind", "label", "status", "startedAt")
}

func builtinJobOutputTool(e *Engine) Tool {
	type input struct {
		JobID     string   `json:"job_id"`
		Wait      bool     `json:"wait"`
		TimeoutMS *float64 `json:"timeout_ms"`
	}
	return Tool{
		Schema: ToolSchema{
			Name:        "job_output",
			Description: "Read a background job. Stream jobs return only output since the previous read. Every response ends with [status: ...]. Reads are non-blocking unless wait is true.",
			Parameters: objectSchema(map[string]any{
				"job_id":     map[string]any{"type": "string", "description": "Job id returned by the tool that started the background work."},
				"wait":       map[string]any{"type": "boolean", "description": "Block until the job reaches a terminal status or the timeout expires."},
				"timeout_ms": map[string]any{"type": "number", "description": "Max wait in milliseconds. Defaults to 30 seconds and is capped at 10 minutes."},
			}, "job_id"),
			Output: objectSchema(map[string]any{"text": map[string]any{"type": "string"}, "job": publicJobSchema()}, "text", "job"),
		},
		Execute: func(ctx context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if err := validateJobID(in.JobID); err != nil {
				return ToolResult{}, err
			}
			if in.Wait {
				timeout := jobWaitDefault
				if in.TimeoutMS != nil {
					if *in.TimeoutMS <= 0 {
						return ToolResult{}, fmt.Errorf("invalid wait timeout: expected a positive number of milliseconds, got %v", *in.TimeoutMS)
					}
					milliseconds := *in.TimeoutMS
					if milliseconds > float64(jobWaitMax/time.Millisecond) {
						milliseconds = float64(jobWaitMax / time.Millisecond)
					}
					timeout = time.Duration(milliseconds * float64(time.Millisecond))
				}
				if _, err := e.jobs.waitFor(ctx, call.SessionID, in.JobID, timeout); err != nil {
					return ToolResult{}, err
				}
			}
			text, job, err := e.jobs.read(call.SessionID, in.JobID)
			if err != nil {
				return ToolResult{}, err
			}
			if text == "" {
				text = "(no new output)"
			}
			if !strings.HasSuffix(text, "\n") {
				text += "\n"
			}
			text += statusLine(job)
			if job.ResultLimit > 0 {
				text = boundTerminalText(text, job.ResultLimit)
			}
			result := textToolResult(text)
			result.Value = map[string]any{"text": strings.TrimSuffix(strings.TrimSuffix(text, statusLine(job)), "\n"), "job": publicJobValue(job)}
			return result, nil
		},
	}
}

func builtinJobListTool(e *Engine) Tool {
	return Tool{
		Schema: ToolSchema{
			Name: "job_list", Description: "List your background jobs (running and finished) with their ids, kinds, and statuses.",
			Parameters: objectSchema(map[string]any{}),
			Output:     map[string]any{"type": "array", "items": publicJobSchema()},
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			jobs := e.jobs.list(call.SessionID)
			if len(jobs) == 0 {
				result := textToolResult("(no background jobs)")
				result.Value = []any{}
				return result, nil
			}
			rows := make([]string, 0, len(jobs))
			values := make([]any, 0, len(jobs))
			for _, job := range jobs {
				rows = append(rows, fmt.Sprintf("%s [%s] %s \u2014 %s", job.ID, job.Kind, job.Status, job.Label))
				values = append(values, publicJobValue(job))
			}
			result := textToolResult(strings.Join(rows, "\n"))
			result.Value = values
			return result, nil
		},
	}
}

func builtinJobKillTool(e *Engine) Tool {
	type input struct {
		JobID  string `json:"job_id"`
		Reason string `json:"reason"`
	}
	return Tool{
		Schema: ToolSchema{
			Name: "job_kill", Description: "Request cancellation of a running background job by job id. Returns immediately; the job settles as killed once its work actually stops.",
			Parameters: objectSchema(map[string]any{
				"job_id": map[string]any{"type": "string", "description": "Job id returned by the tool that started the background work."},
				"reason": map[string]any{"type": "string", "description": "Optional short reason recorded with the cancellation request."},
			}, "job_id"),
			Output: objectSchema(map[string]any{"outcome": map[string]any{"type": "string", "enum": []string{"cancellation-requested", "already-finished"}}, "job": publicJobSchema()}, "outcome", "job"),
		},
		Execute: func(_ context.Context, call ToolCall) (ToolResult, error) {
			var in input
			if err := decodeToolArguments(call, &in); err != nil {
				return ToolResult{}, err
			}
			if err := validateJobID(in.JobID); err != nil {
				return ToolResult{}, err
			}
			job, finished, err := e.jobs.kill(call.SessionID, in.JobID, in.Reason)
			if err != nil {
				return ToolResult{}, err
			}
			if finished {
				result := textToolResult(fmt.Sprintf("job %s had already finished %s", job.ID, statusLine(job)))
				result.Value = map[string]any{"outcome": "already-finished", "job": publicJobValue(job)}
				return result, nil
			}
			result := textToolResult("requested cancellation of job " + job.ID)
			result.Value = map[string]any{"outcome": "cancellation-requested", "job": publicJobValue(job)}
			return result, nil
		},
	}
}
