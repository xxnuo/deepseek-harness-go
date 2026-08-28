package harness

import (
	"errors"
	"fmt"
	"sync"
)

// ContinuableSubagentSetup installs one deployment capability into a child
// before its Activation is published and returns that installation's disposer.
type ContinuableSubagentSetup func(*Session) (func() error, error)

type subagentActivationSetupRegistration struct {
	contribution  ContinuableSubagentSetup
	removed       bool
	installations []*subagentActivationSetupInstallation
}

type subagentActivationSetupInstallation struct {
	registration *subagentActivationSetupRegistration
	child        *Session
	dispose      func() error
	released     bool
	done         chan struct{}
	releaseErr   error
	transaction  *subagentActivationSetupTransaction
}

type subagentActivationSetupTransaction struct {
	registry      *subagentActivationSetupRegistry
	installations []*subagentActivationSetupInstallation
	invalidated   bool
	committed     bool
}

type subagentActivationSetupRegistry struct {
	mu            sync.Mutex
	registrations []*subagentActivationSetupRegistration
	byChild       map[*Session][]*subagentActivationSetupInstallation
}

func newSubagentActivationSetupRegistry() *subagentActivationSetupRegistry {
	return &subagentActivationSetupRegistry{byChild: map[*Session][]*subagentActivationSetupInstallation{}}
}

func (r *subagentActivationSetupRegistry) register(contribution ContinuableSubagentSetup) (func() error, error) {
	if contribution == nil {
		return nil, errors.New("continuable subagent setup contribution is required")
	}
	registration := &subagentActivationSetupRegistration{contribution: contribution}
	r.mu.Lock()
	r.registrations = append(r.registrations, registration)
	r.mu.Unlock()
	return func() error {
		r.mu.Lock()
		if registration.removed {
			r.mu.Unlock()
			return nil
		}
		registration.removed = true
		for index, candidate := range r.registrations {
			if candidate == registration {
				r.registrations = append(r.registrations[:index], r.registrations[index+1:]...)
				break
			}
		}
		installations := append([]*subagentActivationSetupInstallation(nil), registration.installations...)
		r.mu.Unlock()
		return r.releaseAll(installations, "contribution removal")
	}, nil
}

func (r *subagentActivationSetupRegistry) apply(child *Session) (*subagentActivationSetupTransaction, error) {
	transaction := &subagentActivationSetupTransaction{registry: r}
	r.mu.Lock()
	registrations := append([]*subagentActivationSetupRegistration(nil), r.registrations...)
	r.mu.Unlock()
	for _, registration := range registrations {
		r.mu.Lock()
		removed := registration.removed
		r.mu.Unlock()
		if removed {
			continue
		}
		dispose, err := callContinuableSubagentSetup(registration.contribution, child)
		if err != nil {
			_ = r.releaseAll(transaction.installations, "setup rollback")
			return nil, err
		}
		if dispose == nil {
			dispose = func() error { return nil }
		}
		installation := &subagentActivationSetupInstallation{
			registration: registration, child: child, dispose: dispose, done: make(chan struct{}), transaction: transaction,
		}
		r.mu.Lock()
		registration.installations = append(registration.installations, installation)
		transaction.installations = append(transaction.installations, installation)
		r.byChild[child] = append(r.byChild[child], installation)
		removed = registration.removed
		r.mu.Unlock()
		if removed {
			_ = r.release(installation)
		}
	}
	return transaction, nil
}

func (t *subagentActivationSetupTransaction) commit() error {
	if t == nil || t.registry == nil {
		return nil
	}
	t.registry.mu.Lock()
	defer t.registry.mu.Unlock()
	if t.invalidated {
		return subagentServiceError(
			"ACTIVATION_SETUP_REVOKED",
			"a continuable-subagent setup contribution was revoked while this child was being built; the child was not established",
			nil,
		)
	}
	if t.committed {
		return nil
	}
	t.committed = true
	for _, installation := range t.installations {
		installation.transaction = nil
	}
	return nil
}

func (r *subagentActivationSetupRegistry) releaseChild(child *Session) error {
	r.mu.Lock()
	installations := append([]*subagentActivationSetupInstallation(nil), r.byChild[child]...)
	r.mu.Unlock()
	return r.releaseAll(installations, "child scope disposal")
}

func (r *subagentActivationSetupRegistry) releaseAll(installations []*subagentActivationSetupInstallation, during string) error {
	var failures []error
	for _, installation := range installations {
		if err := r.release(installation); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) == 0 {
		return nil
	}
	details := ""
	for index, failure := range failures {
		if index > 0 {
			details += "; "
		}
		details += failure.Error()
	}
	return subagentServiceError(
		"ACTIVATION_SETUP_RELEASE_FAILED",
		fmt.Sprintf("continuable-subagent setup %s failed to release %d installation(s): %s", during, len(failures), details),
		errors.Join(failures...),
	)
}

func (r *subagentActivationSetupRegistry) release(installation *subagentActivationSetupInstallation) error {
	r.mu.Lock()
	if installation.released {
		done := installation.done
		r.mu.Unlock()
		<-done
		return installation.releaseErr
	}
	installation.released = true
	for index, candidate := range installation.registration.installations {
		if candidate == installation {
			installation.registration.installations = append(
				installation.registration.installations[:index],
				installation.registration.installations[index+1:]...,
			)
			break
		}
	}
	if indexed := r.byChild[installation.child]; indexed != nil {
		for index, candidate := range indexed {
			if candidate == installation {
				indexed = append(indexed[:index], indexed[index+1:]...)
				break
			}
		}
		if len(indexed) == 0 {
			delete(r.byChild, installation.child)
		} else {
			r.byChild[installation.child] = indexed
		}
	}
	if installation.transaction != nil {
		installation.transaction.invalidated = true
	}
	r.mu.Unlock()

	err := callContinuableSubagentSetupDisposer(installation.dispose)
	r.mu.Lock()
	installation.releaseErr = err
	close(installation.done)
	r.mu.Unlock()
	return err
}

func callContinuableSubagentSetup(contribution ContinuableSubagentSetup, child *Session) (dispose func() error, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("continuable subagent setup panicked: %v", recovered)
		}
	}()
	return contribution(child)
}

func callContinuableSubagentSetupDisposer(dispose func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("continuable subagent setup disposer panicked: %v", recovered)
		}
	}()
	return dispose()
}

// RegisterContinuableSubagentSetup installs a capability into every future
// resident continuable child. The returned remover is idempotent and revokes
// all live installations before returning.
func (e *Engine) RegisterContinuableSubagentSetup(contribution ContinuableSubagentSetup) (func() error, error) {
	return e.subagentActivationSetups.register(contribution)
}
