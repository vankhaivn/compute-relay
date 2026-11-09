package localhealth

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/hostidentity"
)

const uuid = "A1234567-89AB-CDEF-0123-456789ABCDEF"

// fakeMac answers the collector's fixed commands like an idle Intel Mac on AC power.
type fakeMac struct {
	mu    sync.Mutex
	ioreg string
	runs  map[string]int
}

func newFakeMac() *fakeMac {
	return &fakeMac{ioreg: `    "IOPlatformUUID" = "` + uuid + `"` + "\n", runs: map[string]int{}}
}

func (f *fakeMac) Run(_ context.Context, argv []string, stdout, _ io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	command := strings.Join(argv, " ")
	f.runs[command]++
	var output string
	switch {
	case strings.HasPrefix(command, "/usr/sbin/ioreg "):
		output = f.ioreg
	case command == "/usr/bin/uname -sm":
		output = "Darwin x86_64\n"
	case command == "/usr/sbin/sysctl -n hw.cputype":
		output = "7\n"
	case strings.HasPrefix(command, "/usr/bin/osascript "):
		output = "0\n"
	case command == "/usr/sbin/sysctl hw.ncpu hw.memsize vm.loadavg":
		output = "hw.ncpu: 16\nhw.memsize: 68719476736\nvm.loadavg: { 1.0 1.0 1.0 }\n"
	case command == "/usr/bin/pmset -g therm":
		output = "CPU_Speed_Limit = 100\n"
	case command == "/usr/bin/pmset -g batt":
		output = "Now drawing from 'AC Power'\n"
	}
	if output == "" {
		return errors.New("source not exposed")
	}
	_, err := io.WriteString(stdout, output)
	return err
}

func (f *fakeMac) count(command string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs[command]
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func read(t *testing.T, m *Monitor) hosthealth.View {
	t.Helper()
	view, err := m.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return view
}

func TestMonitorSamplesOncePerDemandWindowAndDropsFutureRows(t *testing.T) {
	mac, c := newFakeMac(), &clock{now: time.Date(2026, 11, 9, 6, 0, 0, 0, time.UTC)}
	m := NewWith(mac, c.Now)
	first := read(t, m)
	want := "resource_" + hostidentity.Digest("darwin-platform-uuid", strings.ToLower(uuid))
	if first.ResourceID != want || len(first.History) != 1 || !first.Fresh || first.Policy == nil || *first.Policy != hosthealth.DefaultPolicy() {
		t.Fatalf("first view %+v", first)
	}
	if level := hosthealth.Evaluate(first, *first.Policy, c.Now()).Level; level != hosthealth.LevelOK {
		t.Fatal("idle Intel Mac evaluated", level)
	}
	c.advance(hosthealth.DemandCache - time.Second)
	read(t, m)
	if mac.count("/usr/bin/uname -sm") != 1 {
		t.Fatal("read inside the demand cache sampled again")
	}
	c.advance(time.Second)
	second := read(t, m)
	if mac.count("/usr/bin/uname -sm") != 2 || len(second.History) != 2 || !second.History[0].ObservedAt.After(second.History[1].ObservedAt) ||
		!second.ReobservedSince.Equal(first.Latest.ObservedAt) || mac.count("/usr/sbin/ioreg -rd1 -c IOPlatformExpertDevice") != 1 {
		t.Fatalf("second view %+v", second)
	}
	c.advance(-time.Hour)
	rolled := read(t, m)
	if mac.count("/usr/bin/uname -sm") != 3 || len(rolled.History) != 1 || !rolled.Latest.ObservedAt.Equal(c.Now()) {
		t.Fatalf("clock rollback kept future rows: %+v", rolled.History)
	}
}

func TestMonitorBoundsHistoryByRowsAndAge(t *testing.T) {
	mac, c := newFakeMac(), &clock{now: time.Date(2026, 11, 9, 6, 0, 0, 0, time.UTC)}
	m := NewWith(mac, c.Now)
	var view hosthealth.View
	for range hosthealth.HistoryMaxRows + 10 {
		view = read(t, m)
		c.advance(hosthealth.DemandCache)
	}
	if len(view.History) != hosthealth.HistoryMaxRows {
		t.Fatal("history rows", len(view.History))
	}
	c.advance(hosthealth.HistoryMaxAge)
	if view = read(t, m); len(view.History) != 1 {
		t.Fatal("rows older than the history age survived", len(view.History))
	}
}

func TestMonitorWithoutAnIdentityHasNoView(t *testing.T) {
	for _, ioreg := range []string{"", `"IOPlatformUUID" = "00000000-0000-0000-0000-000000000000"` + "\n", strings.Repeat(`"IOPlatformUUID" = "`+uuid+`"`+"\n", 2)} {
		mac, c := newFakeMac(), &clock{now: time.Date(2026, 11, 9, 6, 0, 0, 0, time.UTC)}
		mac.ioreg = ioreg
		m := NewWith(mac, c.Now)
		if _, err := m.Read(context.Background()); !errors.Is(err, ErrUnavailable) || mac.count("/usr/bin/uname -sm") != 0 {
			t.Fatalf("ioreg %q: err=%v, collected without an identity", ioreg, err)
		}
		mac.ioreg = `"IOPlatformUUID" = "` + uuid + `"` + "\n"
		if view := read(t, m); len(view.History) != 1 {
			t.Fatal("identity was not retried on the next read")
		}
	}
}

func TestMonitorReadHonoursCancellationWhileAnotherProbeRuns(t *testing.T) {
	m := NewWith(newFakeMac(), time.Now)
	m.probe <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
