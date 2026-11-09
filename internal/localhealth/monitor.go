// Package localhealth samples the machine this Relay runs on with the host health collector.
// It only observes: unlike connection health, no decision here refuses or stops work.
package localhealth

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/hostidentity"
)

// ErrUnavailable means no valid observation of this machine exists yet.
var ErrUnavailable = errors.New("local machine health unavailable")

const maxIdentityBytes = 64 << 10

// Monitor keeps this process's observations in memory, newest first. Reads sample on
// demand at most once per hosthealth.DemandCache; concurrent readers share one probe.
type Monitor struct {
	executor hosthealth.Executor
	now      func() time.Time
	probe    chan struct{}
	mu       sync.Mutex
	resource string
	history  []hosthealth.Snapshot
	since    time.Time
}

// New observes this machine with its fixed system commands.
func New() *Monitor { return NewWith(localExecutor{}, time.Now) }

// NewWith observes through executor and stamps observations with now; tests use it.
func NewWith(executor hosthealth.Executor, now func() time.Time) *Monitor {
	return &Monitor{executor: executor, now: now, probe: make(chan struct{}, 1)}
}

// Read returns the current view, sampling first when the latest observation is older than
// the demand cache or lies in the future after a clock change.
func (m *Monitor) Read(ctx context.Context) (hosthealth.View, error) {
	select {
	case m.probe <- struct{}{}:
	case <-ctx.Done():
		return hosthealth.View{}, ctx.Err()
	}
	defer func() { <-m.probe }()
	if m.due() {
		m.sample(ctx)
	}
	return m.view()
}

func (m *Monitor) due() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.history) == 0 {
		return true
	}
	age := m.now().UTC().Sub(m.history[0].ObservedAt)
	return age < 0 || age >= hosthealth.DemandCache
}

func (m *Monitor) sample(ctx context.Context) {
	m.mu.Lock()
	resource := m.resource
	m.mu.Unlock()
	if resource == "" {
		if resource = m.identity(ctx); resource == "" {
			return
		}
	}
	snapshot := hosthealth.Collect(ctx, m.executor, resource, hosthealth.Config{})
	// The monitor's clock stamps observations so its cache and history agree with the reader's clock.
	snapshot.ObservedAt = m.now().UTC()
	if snapshot.Validate() != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resource = resource
	if m.since.IsZero() {
		m.since = snapshot.ObservedAt
	}
	cutoff := snapshot.ObservedAt.Add(-hosthealth.HistoryMaxAge)
	kept := []hosthealth.Snapshot{snapshot}
	for _, s := range m.history {
		if len(kept) == hosthealth.HistoryMaxRows {
			break
		}
		if s.ObservedAt.Before(snapshot.ObservedAt) && !s.ObservedAt.Before(cutoff) {
			kept = append(kept, s)
		}
	}
	m.history = kept
}

// identity reads the same opaque resource identity an SSH connection to this Mac reports.
func (m *Monitor) identity(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, hosthealth.CommandTimeout)
	defer cancel()
	out := limited{max: maxIdentityBytes}
	if err := m.executor.Run(ctx, []string{"/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice"}, &out, io.Discard); err != nil || out.exceeded {
		return ""
	}
	digest, ok := hostidentity.DarwinPlatform(out.buffer.Bytes())
	if !ok {
		return ""
	}
	return "resource_" + digest
}

func (m *Monitor) view() (hosthealth.View, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.history) == 0 {
		return hosthealth.View{}, ErrUnavailable
	}
	latest := m.history[0]
	policy := hosthealth.DefaultPolicy()
	return hosthealth.View{ResourceID: m.resource, Latest: &latest, History: append([]hosthealth.Snapshot(nil), m.history...),
		Fresh: true, ReobservedSince: m.since, Policy: &policy}, nil
}

// localExecutor runs the collector's fixed absolute commands with only a system PATH,
// so no ambient Docker or shell setting reaches them.
type localExecutor struct{}

func (localExecutor) Run(ctx context.Context, argv []string, stdout, stderr io.Writer) error {
	if len(argv) == 0 {
		return ErrUnavailable
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Env = []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin"}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.WaitDelay = time.Second
	return cmd.Run()
}

type limited struct {
	buffer   bytes.Buffer
	max      int
	exceeded bool
}

func (l *limited) Write(p []byte) (int, error) {
	if len(p) > l.max-l.buffer.Len() {
		l.exceeded = true
		return 0, errors.New("local identity output limit")
	}
	return l.buffer.Write(p)
}
