/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package lease

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres"
)

const (
	// defaultLeaseDuration is how long a lease is valid before it expires.
	defaultLeaseDuration = 15 * time.Second

	// defaultRenewDeadline is how long the holder tries to renew before giving up.
	defaultRenewDeadline = 10 * time.Second

	// defaultRetryPeriod is how frequently a non-holder retries acquiring the lease.
	// It matches the conventional Kubernetes leader-election retry period: a non-holder
	// polls a cleanly released lease every retry period, so a smaller value shortens the
	// switchover hand-over without shortening the take-over wait (defaultLeaseDuration)
	// that holds back a premature promotion.
	defaultRetryPeriod = 2 * time.Second

	// defaultReleasedLeaseDuration is the TTL written when explicitly releasing
	// the lease. An empty HolderIdentity already marks the lease as free, but
	// setting the duration to 1 second mirrors the k8s leaderelection.release()
	// behaviour and acts as belt-and-suspenders: even if an acquirer does not
	// treat an empty identity as immediately available, it waits at most one
	// second before the TTL expires and it can take the lease.
	defaultReleasedLeaseDuration = 1 * time.Second
)

// Config holds the tunable timings of the primary lease. They mirror the
// underlying Kubernetes leader-election parameters and are sourced from the
// Cluster's `.spec.primaryLease` stanza (falling back to the defaults above
// when unset).
type Config struct {
	// LeaseDuration is how long a lease is valid before it expires.
	LeaseDuration time.Duration

	// RenewDeadline is how long the holder tries to renew before giving up.
	RenewDeadline time.Duration

	// RetryPeriod is how frequently a non-holder retries acquiring the lease.
	RetryPeriod time.Duration

	// ReleasedLeaseDuration is the TTL written when explicitly releasing the lease.
	ReleasedLeaseDuration time.Duration
}

// defaultConfig returns the built-in primary lease timings.
func defaultConfig() Config {
	return Config{
		LeaseDuration:         defaultLeaseDuration,
		RenewDeadline:         defaultRenewDeadline,
		RetryPeriod:           defaultRetryPeriod,
		ReleasedLeaseDuration: defaultReleasedLeaseDuration,
	}
}

// Runnable manages the primary lease for this instance.
// It starts idle and enters the acquisition/renewal loop only after Acquire is called.
type Runnable struct {
	instance *postgres.Instance
	lock     resourcelock.Interface

	// config holds the lease timings. It is initialised to the defaults by New
	// and overwritten by the first Acquire call before the runnable activates.
	config Config

	// activateCh is closed by the first Acquire call to unblock Start.
	activateCh   chan struct{}
	activateOnce sync.Once

	// mu guards heldCh and hold, which the run loop writes and Acquire and
	// Yield read from the reconciler's goroutine.
	mu sync.Mutex

	// heldCh is closed while the lease is held. Yield replaces it with an open
	// channel, so a later Acquire blocks until the lease is really held again.
	heldCh chan struct{}

	// hold is the current hold of the lease, nil while it is not held.
	hold *hold

	// yielded is set by Yield and cleared by the next hold. While set,
	// tryTakeOver does not renew a record that still names this pod unless it
	// is the target primary: the record is left to go stale for the new target.
	yielded atomic.Bool

	// observedRecord is the lease state tryTakeOver is timing for liveness and
	// observedTime is when it first saw that state; see tryTakeOver for how they
	// detect a holder that has stopped renewing without comparing against the
	// holder's clock. Accessed only from the single preAcquire goroutine, so
	// they need no synchronisation.
	observedRecord *resourcelock.LeaderElectionRecord
	observedTime   time.Time

	// lastConfirmed is the time, with its monotonic reading, taken before the
	// last lease write that named this pod as the holder started, stamped by
	// the confirmingLock wrapping r.lock. It is how monitorLeaseAge measures
	// how stale our hold is.
	lastConfirmed atomic.Pointer[time.Time]

	// checkStepDown reports whether this primary should stop believing it is
	// still the legitimate primary. It is a field (defaulting to
	// shouldStepDown) rather than a direct call so the step-down decision in
	// monitorLeaseAge can be unit-tested without real network calls.
	checkStepDown func(cluster *apiv1.Cluster, ourIdentity string) (bool, error)

	// hardStop is what monitorLeaseAge runs when the lease is about to expire
	// and the step-down has not completed. It is a field (defaulting to
	// killPostgresAndExit) so that tests can observe it instead of dying.
	hardStop func()

	// monitorParent bounds the life of monitorLeaseAge when it is set. The
	// monitor then outlives the runnable's own context, which the manager
	// cancels on any runnable's error while the lifecycle manager is still
	// stopping PostgreSQL; see runLeaderElection. Tests leave it nil, which
	// ties the monitor to the runnable's context.
	monitorParent context.Context
}

// hold is one period of holding the lease, from the write that took it to the
// Yield that ends it. The elector and the step-down monitor of that period run
// under ctx; cancel ends both. done is closed once the renewal loop has returned
// after a Yield.
type hold struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// SetMonitorContext makes the step-down monitor run until ctx is done, instead
// of until the runnable's own context is. ctx must end when the process is about
// to exit, as the lifecycle manager's global context does. It must be called
// before the runnable starts.
func (r *Runnable) SetMonitorContext(ctx context.Context) {
	r.monitorParent = ctx
}

// New creates a new Runnable.
func New(
	kubeClient kubernetes.Interface,
	instance *postgres.Instance,
) *Runnable {
	result := &Runnable{
		instance:      instance,
		config:        defaultConfig(),
		activateCh:    make(chan struct{}),
		heldCh:        make(chan struct{}),
		checkStepDown: shouldStepDown,
	}
	result.hardStop = result.killPostgresAndExit

	// Every lease write the elector makes goes through le.config.Lock, which is
	// this same lock, so wrapping it here is enough for no confirmed renewal to
	// bypass the timestamp monitorLeaseAge reads.
	result.lock = &confirmingLock{
		Interface: &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Namespace: instance.GetNamespaceName(),
				Name:      instance.GetClusterName(),
			},
			Client: kubeClient.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				// Identity is the pod name alone. The operator creates Pods
				// directly and reuses a pod name only after the previous
				// incarnation is confirmed dead, so any process running under
				// this Identity is the legitimate holder.
				Identity: instance.GetPodName(),
			},
		},
		lastConfirmed: &result.lastConfirmed,
	}

	return result
}

// Acquire signals the runnable to start competing for the lease using the
// provided configuration, then blocks until the lease is held or ctx is
// cancelled. The configuration is only applied by the first call; subsequent
// calls reuse the timings captured at activation time.
func (r *Runnable) Acquire(ctx context.Context, config Config) error {
	contextLogger := log.FromContext(ctx)

	r.activateOnce.Do(func() {
		// Set the config before closing activateCh: the channel close
		// establishes the happens-before edge that lets Start read it safely.
		r.config = config
		contextLogger.Info("Acquiring primary lease")
		close(r.activateCh)
	})

	r.mu.Lock()
	heldCh := r.heldCh
	r.mu.Unlock()

	select {
	case <-heldCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Yield stops holding the lease without stopping PostgreSQL, so that a pod that
// was named target primary and then replaced before it promoted does not keep
// the new target from taking the lease. It stops the renewal loop and the
// step-down monitor, then waits until the renewal loop has returned or ctx is
// done. When the lease is not held yet, the mark it leaves makes the hold that
// is being taken at this very moment end at once instead of renewing.
//
// Nothing is written: the record keeps naming this pod and the new target takes
// it over once it has seen it unchanged for a full LeaseDuration, as for any
// holder that stopped renewing. The empty-holder fast path stays reserved for a
// primary that stopped cleanly.
//
// The caller decides when yielding is safe: only the instance reconciler calls
// it, from the same goroutine that promotes, and only for a pod whose
// PostgreSQL is in recovery and was never asked to promote. After a Yield the
// runnable is back to polling the lease, which it takes again only once this
// pod is the target primary once more.
func (r *Runnable) Yield(ctx context.Context) error {
	r.mu.Lock()
	r.yielded.Store(true)
	h := r.hold
	if h == nil {
		r.mu.Unlock()
		return nil
	}
	r.hold = nil
	r.heldCh = make(chan struct{})
	r.mu.Unlock()

	log.FromContext(ctx).Info(
		"No longer renewing the primary lease: this pod is a replica and no longer the target primary")
	h.cancel()

	select {
	case <-h.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release explicitly releases the lease so that a replica can promote without
// waiting for the TTL to expire. It is a no-op if this pod is not the current
// holder (including if the lease does not exist yet).
// Callers must pass a fresh (non-cancelled) context; the previous run context
// is already cancelled by the time this is called.
// When invoked from the defer in cmd.go, controller-runtime has already waited
// for all runnables (including PostgresLifecycle) to finish, so PostgreSQL is
// down and releasing the lease lets a replica promote at once.
//
// An in-place instance-manager online upgrade is the exception: PostgresLifecycle
// keeps PostgreSQL running while InstanceManagerIsUpgrading is set, so this
// instance is still the primary. Releasing then would blank the holder while we
// keep serving writes, opening a window for a replica to promote. We keep the
// lease held instead; the Identity is the pod name, which the replacement
// instance manager reuses, so it re-adopts the lease with no hand-over and no
// free-lease window.
//
// This guard is reliable on a successful upgrade because reloadInstanceManager
// replaces the process with syscall.Exec, which discards the deferred reset of
// InstanceManagerIsUpgrading: the flag therefore stays set whenever this defer
// can still run. Only a failed exec resets it, and there the operator's
// online-upgrade failover delay covers the pod restart.
func (r *Runnable) Release(ctx context.Context) error {
	contextLogger := log.FromContext(ctx)

	if r.instance.InstanceManagerIsUpgrading.Load() {
		contextLogger.Info("Instance manager is upgrading in place, keeping the primary lease held")
		return nil
	}

	// A yielded pod never promoted. Blanking its stale record would open the
	// empty-holder fast path to the new target, which is reserved for a primary
	// that stopped cleanly.
	if r.yielded.Load() {
		contextLogger.Info("Primary lease yielded earlier, leaving the record for the new target to take over")
		return nil
	}

	// Unlike tryTakeOver/runLeaderElection, this Get is deliberately not
	// bounded at all: Release makes no retry of its own, so any deadline here
	// could return before the Update below ever runs. The caller in cmd.go
	// avoids a timeout on the whole call for the same reason: giving up early
	// would abandon the release and force replicas onto the slow TTL-expiry
	// path instead of the fast empty-holder hand-over.
	record, _, err := r.lock.Get(ctx)
	if errors.IsNotFound(err) {
		contextLogger.Debug("Primary lease does not exist, nothing to release")
		return nil
	}
	if err != nil {
		return err
	}
	if record.HolderIdentity != r.lock.Identity() {
		contextLogger.Debug("Primary lease held by another identity, nothing to release",
			"holder", record.HolderIdentity)
		return nil
	}

	contextLogger.Info("Releasing primary lease")
	// Mirror client-go's leaderelection.release(): preserve LeaderTransitions
	// so the lease's hand-over counter stays accurate, and write a current
	// RenewTime/AcquireTime so any acquirer that does fall back to TTL-based
	// expiry observes a defined moment rather than a zero timestamp.
	now := metav1.NewTime(time.Now())
	return r.lock.Update(ctx, resourcelock.LeaderElectionRecord{
		LeaderTransitions:    record.LeaderTransitions,
		LeaseDurationSeconds: int(r.config.ReleasedLeaseDuration / time.Second),
		RenewTime:            now,
		AcquireTime:          now,
	})
}

// Start implements controller-runtime's Runnable interface.
// The runnable stays idle until Acquire is called, then enters the
// lease acquisition/renewal loop.
func (r *Runnable) Start(ctx context.Context) error {
	select {
	case <-r.activateCh:
		// proceed to active mode
	case <-ctx.Done():
		return nil
	}
	return r.runLeaderElection(ctx)
}

// leaseCheckOutcome is the result of inspecting the primary lease after the
// leader-election loop exits unexpectedly (i.e. without a context cancellation).
type leaseCheckOutcome int

const (
	// leaseMissing means the lease object no longer exists; the cluster
	// controller recreates it on its deletion watch, so we retry.
	leaseMissing leaseCheckOutcome = iota
	// leaseUnverifiable means the lease could not be read (e.g. the API server is
	// unreachable). We have no evidence of preemption, so we retry; whether we
	// have to step down anyway is monitorLeaseAge's decision, not this one.
	leaseUnverifiable
	// leasePreempted means the lease is held by a different (or empty) identity:
	// we no longer own it. This is terminal: the primary must stop.
	leasePreempted
	// leaseStillHeld means we are still the holder: a transient renewal blip, retry.
	leaseStillHeld
)

// classifyLeaseAfterRun decides what the primary should do after the
// leader-election loop returns unexpectedly, based on the post-run read of the
// lease. It is intentionally pure so the high-consequence preemption branch
// (the one that stops PostgreSQL) can be unit-tested without driving a real
// election loop.
func classifyLeaseAfterRun(
	checkErr error,
	record *resourcelock.LeaderElectionRecord,
	ourIdentity string,
) leaseCheckOutcome {
	switch {
	case errors.IsNotFound(checkErr):
		return leaseMissing
	case checkErr != nil:
		return leaseUnverifiable
	// A nil checkErr means the lock returned a non-nil record, so the deref below
	// is safe.
	case record.HolderIdentity != ourIdentity:
		return leasePreempted
	default:
		return leaseStillHeld
	}
}

// tryTakeOver attempts a single take-over of the primary lease, run by
// preAcquire before each renewal loop. It returns true if the lease is now
// held by this pod (either it already was, or we just won it).
//
// Only the target primary takes a lease over; see isTargetPrimary. A cleanly
// released lease (empty holder) is taken immediately. A lease still naming
// another holder is taken over only after we have locally observed the record
// unchanged for a full LeaseDuration, counted from the moment this pod became
// the target, mirroring client-go's observedTime check:
// a live holder renews within that window, so an unchanged record proves it has
// stopped. The comparison is local and equality-only, so clock skew between
// this pod and the previous holder cannot trigger a take-over.
//
// The window is timed in process memory, so a restart of this process re-times
// it from scratch. That is the inherent cost of not trusting the holder's
// clock (the same limitation client-go's elector has); it only adds latency in
// the rare case of a restart mid-take-over, never correctness.
func (r *Runnable) tryTakeOver(ctx context.Context) (bool, error) {
	// Bounded so a Get against an unreachable API server fails instead of
	// blocking the whole take-over loop for however long the underlying
	// transport takes to give up on its own. The budget is RenewDeadline, not
	// the RetryPeriod cadence at which the caller polls: an API server that
	// answers, but more slowly than that cadence, has to be able to answer,
	// otherwise every attempt times out here, the observation window never
	// starts and the take-over never happens.
	getCtx, cancel := context.WithTimeout(ctx, r.config.RenewDeadline)
	record, _, err := r.lock.Get(getCtx)
	cancel()
	if errors.IsNotFound(err) {
		// The cluster controller owns lease creation; nothing to take over yet.
		r.observedRecord = nil
		return false, nil
	}
	if err != nil {
		return false, err
	}

	identity := r.lock.Identity()

	// Already ours, for instance after the instance manager restarted. Refresh
	// the record with a write that is conditional on the version just read.
	// Holding the lease is then settled by the same compare-and-swap that a
	// challenger's claim goes through, not by a read that a claim may have
	// overtaken, and the monitor starts from a write that was really confirmed.
	if record.HolderIdentity == identity {
		// After a Yield the record still names us, but we are a replica that
		// is no longer the target: leave it alone, so the new target sees it
		// go stale, and take it back only if we become the target again.
		if r.yielded.Load() && !r.isTargetPrimary() {
			r.observedRecord = nil
			return false, nil
		}
		return r.renew(ctx, record)
	}

	// Empty holder: the previous primary released cleanly. An empty identity is
	// unambiguous regardless of clocks, so claim it immediately. A switchover or
	// a clean shutdown takes this path, keeping that hand-over fast.
	if record.HolderIdentity == "" {
		if !r.isTargetPrimary() {
			return false, nil
		}
		return r.claim(ctx, record)
	}

	// Another pod still holds the lease. The window only runs while this pod is
	// the target primary: the flip of the target is what tells the old primary
	// to step down, through the peers' /failsafe answers, so a window that ran
	// before the flip would let this pod claim right after it, before the old
	// primary can have learned that it was replaced.
	if !r.isTargetPrimary() {
		r.observedRecord = nil
		return false, nil
	}

	// Take over only once the record has sat unchanged for a full
	// LeaseDuration: a live holder renews well within that window, so an
	// unchanged record means it has stopped. RenewTime is compared for equality
	// only (did the holder write since we last looked), never ordering, so this
	// never trusts the holder's clock.
	now := time.Now()
	if r.observedRecord == nil || !sameHolder(r.observedRecord, record) {
		r.observedRecord = record
		r.observedTime = now
		return false, nil
	}
	if now.Sub(r.observedTime) < r.config.LeaseDuration {
		return false, nil
	}
	return r.claim(ctx, record)
}

// isTargetPrimary reports whether the Cluster the instance manager last
// reconciled names this pod as target primary. The runnable polls for as long
// as the process lives, so only the target may take the lease over: any other
// pod would hold it as a replica and keep the real target from getting it. A
// stale cache only delays the claim, as it delays a promotion. The designated
// primary of a replica cluster is never a target primary here: it is a standby.
func (r *Runnable) isTargetPrimary() bool {
	cluster := r.instance.GetClusterOrDefault()
	return !cluster.IsReplica() && cluster.Status.TargetPrimary == r.instance.GetPodName()
}

// sameHolder reports whether two reads describe the same un-renewed lease
// state. RenewTime advances on every renew, so an equal holder and RenewTime
// means the holder has not written since we last looked.
func sameHolder(a, b *resourcelock.LeaderElectionRecord) bool {
	return a.HolderIdentity == b.HolderIdentity && a.RenewTime.Equal(&b.RenewTime)
}

// renew rewrites a record that already names this pod, with a fresh RenewTime
// and without touching the transition counter or the acquire time. Like claim,
// the Update is conditional on the ResourceVersion of the preceding Get. It is
// bounded by RenewDeadline, as that Get is, so that an API server that does not
// answer cannot hold the loop for longer than a renewal may take.
func (r *Runnable) renew(ctx context.Context, current *resourcelock.LeaderElectionRecord) (bool, error) {
	updateCtx, cancel := context.WithTimeout(ctx, r.config.RenewDeadline)
	defer cancel()

	if err := r.lock.Update(updateCtx, resourcelock.LeaderElectionRecord{
		HolderIdentity:       current.HolderIdentity,
		LeaseDurationSeconds: int(r.config.LeaseDuration / time.Second),
		RenewTime:            metav1.NewTime(time.Now()),
		AcquireTime:          current.AcquireTime,
		LeaderTransitions:    current.LeaderTransitions,
	}); err != nil {
		return false, err
	}
	r.observedRecord = nil
	return true, nil
}

// claim writes this pod as the lease holder, bumping the transition counter.
// The Update is a ResourceVersion-conditional write (the preceding Get
// populated it), so a competing candidate that wins the race makes it fail with
// a conflict; we report not-acquired and let the caller retry on the next poll.
func (r *Runnable) claim(ctx context.Context, current *resourcelock.LeaderElectionRecord) (bool, error) {
	now := metav1.NewTime(time.Now())
	if err := r.lock.Update(ctx, resourcelock.LeaderElectionRecord{
		HolderIdentity:       r.lock.Identity(),
		LeaseDurationSeconds: int(r.config.LeaseDuration / time.Second),
		RenewTime:            now,
		AcquireTime:          now,
		LeaderTransitions:    current.LeaderTransitions + 1,
	}); err != nil {
		return false, err
	}
	r.observedRecord = nil
	return true, nil
}

// preAcquire polls tryTakeOver at a deterministic RetryPeriod cadence (no
// jitter) until this pod holds the primary lease or ctx is cancelled. Polling
// without jitter keeps the take-over cadence bounded by RetryPeriod rather than
// client-go's jittered 2.2*RetryPeriod, and tryTakeOver adopts a cleanly
// released lease, when this pod is the target primary, on the first poll
// instead of waiting out an observation window, so a graceful failover stays
// quick without having to inflate LeaseDuration.
//
// Whether this primary should step down while the lease stays unverifiable is
// not decided here: monitorLeaseAge watches the age of the last confirmed lease
// write on its own cadence, which also covers the case where the elector never
// gets far enough for this loop to run at all.
func (r *Runnable) preAcquire(ctx context.Context) error {
	contextLogger := log.FromContext(ctx).WithName("primary-lease")

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		acquired, err := r.tryTakeOver(ctx)
		switch {
		case err != nil:
			// A cancelled ctx surfaces here as a context error: return it
			// rather than logging a misleading take-over failure on shutdown.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			contextLogger.Debug("Primary lease take-over attempt failed, retrying", "error", err)
		case acquired:
			return nil
		default:
			contextLogger.Debug("Primary lease not yet available, retrying")
		}
		timer.Reset(r.config.RetryPeriod)
	}
}

// runLeaderElection acquires the primary lease and then keeps it renewed. It
// is the core of the primary lease mechanism.
//
// Each iteration first calls preAcquire, a jitter-free loop that takes the
// lease over, then keepHeld renews it until the hold ends. A hold ends in one
// of two ways: the process is shutting down, and Release() is called by the
// deferred shutdown path in cmd.go, or the reconciler called Yield, and the
// loop goes back to competing for the lease.
//
// Instance-level fencing (cnpg.io/fencedInstances) does not release the lease
// either: the operator deliberately skips switchover while the current primary
// is fenced, so holding the lease aligns with the freeze-the-cluster intent.
// Unfencing resumes the same primary without any lease transition.
func (r *Runnable) runLeaderElection(ctx context.Context) error {
	contextLogger := log.FromContext(ctx).WithName("primary-lease")
	// Carry the "primary-lease" name into the context handed to the elector
	// below: client-go's leaderelection package logs through klog.FromContext,
	// which reads this same context, not the contextLogger local variable. It
	// would otherwise log its acquire/renew bookkeeping under the ambient
	// logger name (e.g. "instance-manager"), reading as an unrelated,
	// still-in-progress lease race rather than what it is: confirmation of a
	// lease preAcquire has already taken.
	ctx = log.IntoContext(ctx, contextLogger)

	// The monitor outlives this loop when a monitor context was set: the manager
	// cancels ctx as soon as any runnable fails, and the lease is then no longer
	// renewed while PostgreSQL may still take longer than the lease lasts to
	// shut down.
	monitorCtx := ctx
	if r.monitorParent != nil {
		monitorCtx = log.IntoContext(r.monitorParent, contextLogger)
	}

	for {
		if err := r.preAcquire(ctx); err != nil {
			// preAcquire only returns ctx.Err(); treat as clean shutdown.
			return nil
		}
		h := r.newHold(ctx, monitorCtx)

		var err error
		if h.ctx.Err() == nil {
			err = r.keepHeld(h.ctx)
		}
		close(h.done)
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}

		// Yielded: the elector has returned, so nothing writes the record any
		// more. The loop goes back to polling, where tryTakeOver leaves the
		// record alone until this pod is the target again.
	}
}

// newHold records that the lease is held, unblocking Acquire, and starts the
// step-down monitor for this hold. The monitor runs under monitorCtx, not under
// ctx, so that it outlives a shutdown of the runnable; a Yield cancels both.
//
// A Yield that came while this lease was being taken found no hold to end, and
// so the hold is returned already ended when this pod is not the target.
func (r *Runnable) newHold(ctx, monitorCtx context.Context) *hold {
	holdCtx, cancelHold := context.WithCancel(ctx)
	monitorHoldCtx, cancelMonitor := context.WithCancel(monitorCtx)
	h := &hold{
		ctx:    holdCtx,
		cancel: func() { cancelHold(); cancelMonitor() },
		done:   make(chan struct{}),
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.yielded.Load() && !r.isTargetPrimary() {
		h.cancel()
		return h
	}
	r.hold = h
	r.yielded.Store(false)
	close(r.heldCh)

	log.FromContext(ctx).Info("Acquired primary lease")
	go r.monitorLeaseAge(monitorHoldCtx)
	return h
}

// keepHeld renews a lease preAcquire has just taken until ctx is done or the
// lease is lost. It hands off to client-go's elector for renewal; the jitter
// inside the elector is harmless there because a lease we already hold is
// uncontended. The elector's le.Run can then return in three distinct ways:
//
//  1. ctx is done: the manager is shutting down, or the reconciler yielded the
//     lease. We return nil.
//
//  2. Transient renewal failure: the primary cannot reach the Kubernetes API
//     server for renewDeadline (10s by default) and le.Run returns, but the
//     lease TTL (15s from last renewal by default) has not yet expired and no
//     other pod has promoted. We detect this by reading the lease after le.Run
//     returns: if our pod is still the HolderIdentity, the lease is intact and
//     we loop back into le.Run to resume renewal, through preAcquire, which
//     refreshes the record.
//
//  3. Preemption: the lease has expired and another pod has acquired it, or the
//     lease object no longer exists. In this case reading the lease reveals a
//     different (or empty) holder. We return a fatal error so controller-runtime
//     shuts down the manager and stops PostgreSQL.
//
// If the post-exit read itself fails (API server still unreachable), we have no
// evidence of preemption and simply retry; a retryPeriod-sized timeout is used
// for the lease-verification read to avoid blocking indefinitely. Whether this
// primary has to step down anyway is decided elsewhere, by monitorLeaseAge,
// which watches the age of the last confirmed lease write on its own cadence
// (see monitorLeaseAge for why the trigger cannot hang off this control flow).
func (r *Runnable) keepHeld(ctx context.Context) error {
	contextLogger := log.FromContext(ctx)

	for {
		// preAcquire has just confirmed (or re-confirmed) that we hold the
		// lease. The elector we hand off to next re-runs its own
		// acquire-then-renew cycle regardless, so the "Attempting to acquire
		// leader lease..." / "Successfully acquired lease" pair it logs below
		// is expected renewal bookkeeping for the lease we already hold, not a
		// new election. Logged at Info to sit next to those Info-level lines.
		contextLogger.Info("Handing off to the leader-election renewal loop for the lease we already hold")

		le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock:            r.lock,
			LeaseDuration:   r.config.LeaseDuration,
			RenewDeadline:   r.config.RenewDeadline,
			RetryPeriod:     r.config.RetryPeriod,
			ReleaseOnCancel: false, // lease is released explicitly via Release()
			Name:            r.instance.GetClusterName(),
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(context.Context) {
					// preAcquire has already taken the lease, so the elector
					// always starts as leader; this is just the renewal loop
					// starting. The acquisition itself is logged above.
					contextLogger.Debug("Primary lease renewal loop started")
				},
				OnStoppedLeading: func() {
					// leaderelection invokes this on every Run exit, including
					// clean ctx cancellation. The meaningful "we lost the lease"
					// signal is the fatal error returned from runLeaderElection.
					contextLogger.Debug("leaderelection.Run exited")
				},
				OnNewLeader: func(string) {},
			},
		})
		if err != nil {
			return err
		}

		le.Run(ctx)

		// Scenario 1: shutdown or yield.
		if ctx.Err() != nil {
			return nil
		}

		// le.Run exited unexpectedly. Read the lease to distinguish a transient
		// renewal failure (we still hold it) from genuine preemption (another pod
		// holds it or the object is gone).
		checkCtx, checkCancel := context.WithTimeout(ctx, r.config.RetryPeriod)
		record, _, checkErr := r.lock.Get(checkCtx)
		checkCancel()

		switch classifyLeaseAfterRun(checkErr, record, r.lock.Identity()) {
		case leaseMissing:
			// The lease object is gone (e.g. someone deleted it). The cluster
			// controller will recreate it on its next reconcile; loop and let
			// le.Run re-acquire it once it reappears.
			contextLogger.Warning("Primary lease object missing, waiting for it to be recreated")
		case leaseUnverifiable:
			// We have no evidence of preemption, but also can't confirm we
			// still hold the lease. We don't check the step-down condition
			// here: monitorLeaseAge is already watching the age of the last
			// confirmed write on its own cadence, for as long as this persists.
			contextLogger.Warning("Primary lease object unverifiable, will retry take-over", "error", checkErr)
		case leasePreempted:
			// A different identity holds the lease: we have been preempted. This is
			// a terminal event: the returned error shuts down the manager and stops
			// PostgreSQL. Log it explicitly at the point of detection so the cause
			// is visible, rather than only surfacing as a generic manager error.
			err := fmt.Errorf("primary lease is now held by %q", record.HolderIdentity)
			contextLogger.Error(err, "Primary lease preempted, shutting down",
				"newHolder", record.HolderIdentity)
			return err
		case leaseStillHeld:
			// We still hold the lease: transient API server blip. Loop.
			contextLogger.Warning("Primary lease renewal failed transiently, retrying")
		}

		if err := r.preAcquire(ctx); err != nil {
			return nil
		}
	}
}
