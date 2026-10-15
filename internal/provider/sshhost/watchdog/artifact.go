// Package watchdog renders the frozen, user-level macOS host watchdog.
// It performs no local or remote installation and never runs host commands.
package watchdog

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"io/fs"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

const (
	Version      = 1
	ScriptName   = "watchdog-v1.sh"
	PlistName    = "watchdog-v1.plist"
	ManifestName = "manifest-v1.txt"
	StatusName   = "status-v1.json"
	LedgerName   = "stop-ledger-v1.txt"
	MaxStatus    = 1024
	MaxStopIDs   = 64
	StopReason   = "host_critical_stopped"
)

var ErrConfig = errors.New("invalid host watchdog configuration")
var ErrStatus = errors.New("invalid host watchdog status")

// Config is private installation material, frozen into the rendered script. Home
// is the authenticated user's canonical home. DockerPath, DockerHost and EngineID
// must come from verification of that same machine and its local Docker daemon.
// CriticalThermalLevel uses raw machdep.xcpm.cpu_thermal_level units. The CPU
// speed, temperature and battery comparisons are inclusive. Missing optional die
// temperatures do not invalidate the unprivileged thermal and power samples.
type Config struct {
	Home                          string
	InstallationID                string
	DockerPath                    string
	DockerHost                    string
	EngineID                      string
	CriticalThermalLevel          int
	MinimumCPUSpeedPercent        int
	CriticalDieTemperatureCelsius float64
	CriticalDurationSeconds       int
	LowBatteryPercent             int
	SampleIntervalSeconds         int
	PowermetricsGranted           bool
}

// File is an exact immutable file to install privately inside Artifact.Directory.
type File struct {
	Name    string
	Mode    fs.FileMode
	Content []byte
	SHA256  string
}

// Artifact is deterministic for its Config. Installers must verify every file's
// digest and the user's ownership/mode 0700 of Directory before loading PlistName.
// The loaded plist pins ManifestDigest; each cycle verifies that manifest and
// its script digest. RuntimeFiles are the only additional files the script uses.
// Preserve the stop ledger on an idempotent install; only explicit removal may
// reset its one-shot stop history.
type Artifact struct {
	Version        int
	Directory      string
	Label          string
	SourceDigest   string
	ManifestDigest string
	Files          []File
	RuntimeFiles   []string
}

//go:embed watchdog.sh
var source string

type commands struct {
	sysctl, pmset, sudo, uname, date, stat, id, sleep string
	path, sync                                        string
}

var productionCommands = commands{
	sysctl: "/usr/sbin/sysctl", pmset: "/usr/bin/pmset", sudo: "/usr/bin/sudo", uname: "/usr/bin/uname",
	date: "/bin/date", stat: "/usr/bin/stat", id: "/usr/bin/id", sleep: "/bin/sleep",
	path: "/usr/bin:/bin:/usr/sbin:/sbin",
	sync: "/usr/bin/perl",
}

var nativeEngine = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:-]{15,127}$`)

func cleanAbsolute(value string) bool {
	if len(value) == 0 || len(value) > 1024 || !path.IsAbs(value) || path.Clean(value) != value || value == "/" {
		return false
	}
	for _, c := range value {
		if c < ' ' || c > '~' || c == '\\' {
			return false
		}
	}
	return true
}

func (c Config) Validate() error {
	socket, unix := strings.CutPrefix(c.DockerHost, "unix://")
	if !cleanAbsolute(c.Home) || !domain.RuntimeInstallationID(c.InstallationID).Valid() || !cleanAbsolute(c.DockerPath) ||
		!unix || !cleanAbsolute(socket) || !nativeEngine.MatchString(c.EngineID) ||
		c.CriticalThermalLevel < 0 || c.CriticalThermalLevel > 255 ||
		c.MinimumCPUSpeedPercent < 0 || c.MinimumCPUSpeedPercent > 100 ||
		math.IsNaN(c.CriticalDieTemperatureCelsius) || math.IsInf(c.CriticalDieTemperatureCelsius, 0) ||
		c.CriticalDieTemperatureCelsius <= 0 || c.CriticalDieTemperatureCelsius > 150 ||
		c.CriticalDurationSeconds < 1 || c.CriticalDurationSeconds > 3600 ||
		c.LowBatteryPercent < 0 || c.LowBatteryPercent > 100 ||
		c.SampleIntervalSeconds < 1 || c.SampleIntervalSeconds > 300 {
		return ErrConfig
	}
	return nil
}

// Render builds files only; it neither probes nor mutates the host.
func Render(c Config) (Artifact, error) { return render(c, productionCommands) }

func render(c Config, cmd commands) (Artifact, error) {
	if err := c.Validate(); err != nil {
		return Artifact{}, err
	}
	dir := path.Join(c.Home, ".compute-relay-host-watchdog-"+c.InstallationID)
	a := Artifact{
		Version: Version, Directory: dir, Label: "io.compute-relay.host-watchdog." + c.InstallationID,
		SourceDigest: digest([]byte(source)),
		RuntimeFiles: []string{StatusName, LedgerName, ".command-v1.out", ".status-v1.tmp", ".stop-ledger-v1.tmp"},
	}
	powermetrics := "0"
	if c.PowermetricsGranted {
		powermetrics = "1"
	}
	replace := strings.NewReplacer(
		"@@DIRECTORY@@", quote(dir), "@@INSTALLATION@@", quote(c.InstallationID),
		"@@DOCKER@@", quote(c.DockerPath), "@@DOCKER_HOST@@", quote(c.DockerHost), "@@ENGINE@@", quote(c.EngineID),
		"@@THERMAL@@", strconv.Itoa(c.CriticalThermalLevel), "@@CPU_SPEED@@", strconv.Itoa(c.MinimumCPUSpeedPercent),
		"@@TEMPERATURE@@", strconv.FormatFloat(c.CriticalDieTemperatureCelsius, 'f', -1, 64),
		"@@DURATION@@", strconv.Itoa(c.CriticalDurationSeconds), "@@BATTERY@@", strconv.Itoa(c.LowBatteryPercent),
		"@@INTERVAL@@", strconv.Itoa(c.SampleIntervalSeconds), "@@POWERMETRICS@@", powermetrics,
		"@@SOURCE_DIGEST@@", quote(a.SourceDigest),
		"@@SYSCTL@@", quote(cmd.sysctl), "@@PMSET@@", quote(cmd.pmset), "@@SUDO@@", quote(cmd.sudo),
		"@@UNAME@@", quote(cmd.uname), "@@DATE@@", quote(cmd.date), "@@STAT@@", quote(cmd.stat),
		"@@ID@@", quote(cmd.id), "@@SLEEP@@", quote(cmd.sleep),
		"@@PATH@@", quote(cmd.path),
		"@@SYNC@@", quote(cmd.sync),
	)
	script := []byte(replace.Replace(source))
	manifest := []byte("compute-relay-host-watchdog 1\nsource-sha256 " + a.SourceDigest + "\nscript-sha256 " + digest(script) + "\n")
	a.ManifestDigest = digest(manifest)
	plist := []byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
		"<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n" +
		"<plist version=\"1.0\"><dict>\n" +
		"<key>Label</key><string>" + escapeXML(a.Label) + "</string>\n" +
		"<key>ProgramArguments</key><array><string>/bin/sh</string><string>" + escapeXML(path.Join(dir, ScriptName)) +
		"</string><string>" + a.ManifestDigest + "</string></array>\n" +
		"<key>WorkingDirectory</key><string>" + escapeXML(dir) + "</string>\n" +
		"<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>\n" +
		"<key>ProcessType</key><string>Background</string><key>LowPriorityIO</key><true/>\n" +
		"<key>ThrottleInterval</key><integer>10</integer><key>Umask</key><integer>63</integer>\n" +
		"<key>StandardOutPath</key><string>/dev/null</string><key>StandardErrorPath</key><string>/dev/null</string>\n" +
		"</dict></plist>\n")
	a.Files = []File{
		{Name: ScriptName, Mode: 0700, Content: script, SHA256: digest(script)},
		{Name: ManifestName, Mode: 0600, Content: manifest, SHA256: a.ManifestDigest},
		{Name: PlistName, Mode: 0600, Content: plist, SHA256: digest(plist)},
	}
	return a, nil
}

func quote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
func escapeXML(value string) string {
	var out bytes.Buffer
	_ = xml.EscapeText(&out, []byte(value))
	return out.String()
}

// Status is a cycle fact, not a cancellation or resource-release receipt.
// confirmed_inactive requires immediate exact-ID terminal observations. Retained
// history from another cycle or restart remains stop_unknown; filtered absence
// never upgrades it. Explicit removal is required to reset exhausted history.
type Status struct {
	Version    int       `json:"version"`
	LastRun    time.Time `json:"last_run"`
	LastAction string    `json:"last_action"`
}

// ParseStatus accepts only bounded watchdog facts, never private paths or IDs.
func ParseStatus(raw []byte) (Status, error) {
	var s Status
	if len(raw) > MaxStatus {
		return s, ErrStatus
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&s) != nil || d.Decode(new(any)) != io.EOF || s.Version != Version || s.LastRun.IsZero() {
		return Status{}, ErrStatus
	}
	switch s.LastAction {
	case "idle", "unknown", "unsupported", "integrity_failed", "critical_wait", "stop_requested", "stop_unknown", "confirmed_inactive", "ledger_full", "attention_required":
		return s, nil
	default:
		return Status{}, ErrStatus
	}
}
