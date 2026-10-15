package hosthealth

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type Source string

const (
	Platform       Source = "platform"
	Thermal        Source = "thermal-pressure"
	Power          Source = "power-battery"
	Sleep          Source = "sleep-settings"
	Battery        Source = "smart-battery-ioreg"
	Sysctl         Source = "sysctl-metrics"
	Top            Source = "top-sample"
	MemoryPressure Source = "memory-pressure"
	Docker         Source = "docker-info"
	GPU            Source = "gpu-utilization"
	Sudo           Source = "sudo-list-noninteractive"
	Powermetrics   Source = "powermetrics"
)
const MaxSourceBytes = 64 << 10
const CommandTimeout = 4 * time.Second
const CollectionTimeout = 25 * time.Second

type Sample struct {
	Data   []byte
	Status Status
	Reason string
}

// Executor must honor context cancellation and must not retain output writers.
// Error text and stderr are intentionally discarded, since they can contain identifiers.
type Executor interface {
	Run(context.Context, []string, io.Writer, io.Writer) error
}

type command struct {
	source Source
	argv   []string
}

// The shell program emits scalar allowlisted battery fields only. No input is interpolated.
const batteryScript = `/usr/sbin/ioreg -rn AppleSmartBattery | /usr/bin/awk '/^[[:space:]]*"(Temperature|CycleCount|DesignCapacity|AppleRawMaxCapacity|MaxCapacity)" = [0-9]+[[:space:]]*$/ { gsub(/"/, ""); print $1 " = " $3 }'`

type Config struct {
	// Both values must come from the adapter's verified local Docker binding.
	DockerPath string
	DockerHost string
}

const gpuScript = `/usr/sbin/ioreg -r -c IOAccelerator -d 2 | /usr/bin/awk '{ while (match($0, /"Device Utilization %"[[:space:]]*=[[:space:]]*[0-9]+/)) { v=substr($0,RSTART,RLENGTH); gsub(/[^0-9]/,"",v); print "gpu_utilization_percent = " v; $0=substr($0,RSTART+RLENGTH) } }'`

func commands(config Config) []command {
	dockerPath := config.DockerPath
	socket, local := strings.CutPrefix(config.DockerHost, "unix://")
	local = local && path.IsAbs(socket) && path.Clean(socket) == socket && len(socket) <= 2048 && !strings.ContainsAny(socket, "\x00\r\n")
	out := []command{
		{Platform, []string{"/usr/bin/uname", "-sm"}},
		{Sysctl, []string{"/usr/sbin/sysctl", "hw.ncpu", "hw.memsize", "vm.loadavg", "machdep.xcpm.cpu_thermal_level"}},
		{Thermal, []string{"/usr/bin/pmset", "-g", "therm"}},
		{Power, []string{"/usr/bin/pmset", "-g", "batt"}},
		{Sleep, []string{"/usr/bin/pmset", "-g", "custom"}},
		{Battery, []string{"/bin/sh", "-c", batteryScript}},
		{GPU, []string{"/bin/sh", "-c", gpuScript}},
		{Top, []string{"/usr/bin/top", "-l", "2", "-n", "0"}},
		{MemoryPressure, []string{"/usr/bin/memory_pressure"}},
		{Sudo, []string{"/usr/bin/sudo", "-n", "-l"}},
	}
	if local && len(dockerPath) <= 2048 && filepath.IsAbs(dockerPath) && filepath.Clean(dockerPath) == dockerPath && !strings.ContainsAny(dockerPath, "\x00\r\n") {
		out = append(out, command{Docker, []string{dockerPath, "--host", config.DockerHost, "info", "--format", `{"NCPU":{{.NCPU}},"MemTotal":{{.MemTotal}}}`}})
	}
	return out
}

var errOutputLimit = errors.New("host health output limit")

type cappedBuffer struct {
	buffer   bytes.Buffer
	exceeded bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if n > MaxSourceBytes-b.buffer.Len() {
		p = p[:MaxSourceBytes-b.buffer.Len()]
		b.exceeded = true
	}
	_, _ = b.buffer.Write(p)
	if b.exceeded {
		return len(p), errOutputLimit
	}
	return n, nil
}

// Stderr has an independent byte cap and never retains host text.
type cappedDiscard struct {
	size     int
	exceeded bool
	cancel   context.CancelFunc
}

func (b *cappedDiscard) Write(p []byte) (int, error) {
	if len(p) > MaxSourceBytes-b.size {
		b.exceeded = true
		b.cancel()
		return 0, errOutputLimit
	}
	b.size += len(p)
	return len(p), nil
}

func collectSource(ctx context.Context, executor Executor, argv []string) Sample {
	ctx, cancel := context.WithTimeout(ctx, CommandTimeout)
	defer cancel()
	var out cappedBuffer
	stderr := cappedDiscard{cancel: cancel}
	err := executor.Run(ctx, append([]string(nil), argv...), &out, &stderr)
	if out.exceeded || stderr.exceeded {
		return Sample{Status: Unavailable, Reason: "output_limit"}
	}
	if ctx.Err() != nil {
		return Sample{Status: Unavailable, Reason: "collection_timeout"}
	}
	if err != nil {
		return Sample{Status: Unavailable, Reason: "command_failed"}
	}
	return Sample{Status: Available, Data: append([]byte(nil), out.buffer.Bytes()...)}
}

// Collect accepts only a Docker path already verified by the host adapter. It returns
// only typed values and fixed reasons; transient source text is never returned.
func Collect(ctx context.Context, executor Executor, resourceID string, config Config) Snapshot {
	ctx, cancel := context.WithTimeout(ctx, CollectionTimeout)
	defer cancel()
	samples := make(map[Source]Sample)
	if executor == nil {
		return Parse(resourceID, time.Now().UTC(), samples)
	}
	for _, cmd := range commands(config) {
		if cmd.source != Platform && !supported(samples[Platform]) {
			break
		}
		samples[cmd.source] = collectSource(ctx, executor, cmd.argv)
	}
	grant := ClassifyGrant(samples[Sudo])
	if supported(samples[Platform]) && (grant == "narrow" || grant == "broader") {
		samples[Powermetrics] = collectSource(ctx, executor, []string{"/usr/bin/sudo", "-n", "/usr/bin/powermetrics", "--samplers", "smc", "-n", "1", "-i", "1000"})
	}
	return Parse(resourceID, time.Now().UTC(), samples)
}
func supported(s Sample) bool {
	return strings.TrimSpace(sourceText(s)) == "Darwin x86_64"
}
