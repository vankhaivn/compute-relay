package sshhost

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/hostwatchdog"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost/watchdog"
	"io"
	"strings"
	"time"
)

// Only launchctl service classification consumes private stderr. Every other
// watchdog probe follows the health collector's diagnostic-discarding policy.
type watchdogBoundedExecutor struct{ executor }

func (e watchdogBoundedExecutor) Run(ctx context.Context, env map[string]string, args []string, in io.Reader, out io.Writer) (Result, error) {
	result, err := runBoundedHealthCommand(ctx, e.executor, env, args, in, out)
	if len(args) < 2 || args[0] != "/bin/launchctl" || args[1] != "print" {
		clear(result.Stderr)
		result.Stderr = nil
	}
	return result, err
}

// Only this exact direct bootout gets a longer command budget; the worker's
// enclosing 45-second action budget still applies.
const watchdogBootoutTimeout = 35 * time.Second

func (e watchdogBoundedExecutor) bootout(ctx context.Context, a watchdog.Artifact, p hostwatchdog.Plan) error {
	if p.Validate() != nil || a.Label != "io.compute-relay.host-watchdog."+p.Config.InstallationID {
		return ErrRequest
	}
	ctx, cancel := context.WithTimeout(ctx, watchdogBootoutTimeout)
	defer cancel()
	if closer, ok := e.executor.(io.Closer); ok {
		stop := context.AfterFunc(ctx, func() { _ = closer.Close() })
		defer stop()
	}
	result, err := e.executor.Run(ctx, map[string]string{"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL": "C"}, []string{"/bin/launchctl", "bootout", "gui/" + p.UID + "/" + a.Label}, nil, &limitedWriter{w: io.Discard, limit: watchdog.MaxStatus})
	clear(result.Stderr)
	if err != nil || result.ExitCode != 0 {
		return hostwatchdog.ErrIntegrity
	}
	return nil
}

type WatchdogRemote struct {
	credentials ports.CredentialResolver
	dial        func(context.Context, Target, Credential) (session, error)
}

func NewWatchdogRemote(c ports.CredentialResolver) *WatchdogRemote {
	return &WatchdogRemote{c, func(ctx context.Context, t Target, c Credential) (session, error) { return Dial(ctx, t, c) }}
}
func (w *WatchdogRemote) with(ctx context.Context, t hostwatchdog.Target, use func(watchdogBoundedExecutor, docker) error) error {
	return w.credentials.WithCredential(ctx, t.Health.CredentialRef, func(raw []byte) error {
		e, err := ParseEndpoint(raw)
		if err != nil {
			return ErrRequest
		}
		defer clear(e.Credential.Password)
		defer clear(e.Credential.PrivateKey)
		defer clear(e.Credential.Passphrase)
		e, err = e.Pin(t.Health.CanonicalAccount)
		if err != nil {
			return err
		}
		c, err := w.dial(ctx, e.Target, e.Credential)
		if err != nil {
			return err
		}
		defer c.Close()
		x := watchdogBoundedExecutor{c}
		machine, err := machineIdentity(ctx, x)
		if err != nil || "resource_"+machine != t.Health.ResourceID {
			return ErrResourceIdentity
		}
		platform, err := readIdentity(ctx, x, 32, "/usr/bin/uname", "-sm")
		if err != nil || strings.TrimSpace(string(platform)) != "Darwin x86_64" {
			return hostwatchdog.ErrUnsupported
		}
		native, err := readIdentity(ctx, x, 16, "/usr/sbin/sysctl", "-n", "hw.cputype")
		if err != nil || strings.TrimSpace(string(native)) != "7" {
			return hostwatchdog.ErrUnsupported
		}
		binary, err := findDocker(ctx, x, e.DockerPath)
		if err != nil {
			return err
		}
		d, err := (docker{exec: x, binary: binary}).localResource(ctx)
		if err != nil {
			return err
		}
		engine, err := d.engineIdentity(ctx)
		if err != nil || t.Engine == "" || engine != t.Engine {
			return ErrResourceIdentity
		}
		return use(x, d)
	})
}
func (w *WatchdogRemote) Plan(ctx context.Context, t hostwatchdog.Target) (hostwatchdog.Plan, error) {
	var p hostwatchdog.Plan
	err := w.with(ctx, t, func(x watchdogBoundedExecutor, d docker) error {
		raw, err := readIdentity(ctx, x, 2048, "/bin/sh", "-c", `cd -P "$HOME" && test ! -L "$HOME" && printf '%s\n' "$PWD" && /usr/bin/id -u && /usr/bin/stat -f '%u' "$PWD"`)
		if err != nil {
			return err
		}
		parts := strings.Split(strings.TrimSpace(string(raw)), "\n")
		if len(parts) != 3 || parts[2] != parts[1] {
			return ErrIdentity
		}
		engine, err := d.engineID(ctx)
		if err != nil {
			return err
		}
		policy := hosthealth.DefaultPolicy()
		if t.Health.Policy != nil {
			policy = *t.Health.Policy
		}
		p = hostwatchdog.Plan{UID: parts[1], Config: watchdog.Config{Home: parts[0], InstallationID: t.Installation, DockerPath: d.binary, DockerHost: d.host, EngineID: engine, CriticalThermalLevel: policy.CriticalThermalLevel, MinimumCPUSpeedPercent: policy.CriticalCPUSpeedPercent, CriticalDieTemperatureCelsius: policy.CriticalDieTemperatureCelsius, CriticalDurationSeconds: policy.CriticalDurationSeconds, LowBatteryPercent: policy.LowBatteryPercent, SampleIntervalSeconds: 30}}
		var grant bytes.Buffer
		result, grantErr := x.Run(ctx, nil, []string{"/usr/bin/sudo", "-n", "-l"}, nil, &limitedWriter{w: &grant, limit: 16 << 10})
		status := hosthealth.Available
		if grantErr != nil || result.ExitCode != 0 {
			status = hosthealth.Unavailable
		}
		classification := hosthealth.ClassifyGrant(hosthealth.Sample{Data: grant.Bytes(), Status: status})
		clear(result.Stderr)
		p.Config.PowermetricsGranted = classification == "narrow" || classification == "broader"
		return p.Validate()
	})
	return p, err
}
func watchdogQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'" }
func watchdogCheck(a watchdog.Artifact, p hostwatchdog.Plan) string {
	return watchdogCheckFiles(a, p, false)
}
func watchdogCheckFiles(a watchdog.Artifact, p hostwatchdog.Plan, partial bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "d=%s\nu=%s\ntest ! -L \"$d\" && test -d \"$d\" && test \"$(/usr/bin/stat -f '%%u:%%Lp' \"$d\")\" = \"$u:700\" || exit 30\n", watchdogQuote(a.Directory), watchdogQuote(p.UID))
	for _, f := range a.Files {
		q := watchdogQuote(a.Directory + "/" + f.Name)
		if partial {
			fmt.Fprintf(&b, "if test -e %s || test -L %s; then\n", q, q)
		}
		fmt.Fprintf(&b, "test ! -L %s && test -f %s && test \"$(/usr/bin/stat -f '%%u:%%Lp' %s)\" = '%s:%o' && test \"$(/usr/bin/shasum -a 256 %s | /usr/bin/awk '{print $1}')\" = '%s' || exit 31\n", q, q, q, p.UID, f.Mode.Perm(), q, f.SHA256)
		if partial {
			b.WriteString("fi\n")
		}
	}
	return b.String()
}

// serviceState accepts only launchd's specific service-absent result. Permission,
// missing GUI domain and transport errors remain unknown.
func watchdogServiceState(ctx context.Context, x executor, a watchdog.Artifact, p hostwatchdog.Plan) (bool, error) {
	var out bytes.Buffer
	r, err := x.Run(ctx, map[string]string{"LC_ALL": "C"}, []string{"/bin/launchctl", "print", "gui/" + p.UID + "/" + a.Label}, nil, &limitedWriter{w: &out, limit: 32 << 10})
	defer clear(r.Stderr)
	if err != nil {
		return false, ErrUnavailable
	}
	if r.ExitCode != 0 {
		message := string(r.Stderr)
		if r.ExitCode == 113 && strings.Contains(message, "Could not find service") && strings.Contains(message, a.Label) {
			return false, nil
		}
		return false, ErrUnavailable
	}
	matches := 0
	for _, line := range strings.Split(out.String(), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "path = ") {
			if strings.TrimPrefix(line, "path = ") != a.Directory+"/"+watchdog.PlistName {
				return false, hostwatchdog.ErrIntegrity
			}
			matches++
		}
	}
	if matches != 1 {
		return false, hostwatchdog.ErrIntegrity
	}
	return true, nil
}

// Successful bootout can precede service disappearance. Poll only that exact
// verified service; uncertain reads retain the files and never repeat bootout.
func watchdogAwaitRemoval(ctx context.Context, x executor, a watchdog.Artifact, p hostwatchdog.Plan) error {
	ctx, cancel := context.WithTimeout(ctx, watchdogBootoutTimeout)
	defer cancel()
	for {
		loaded, err := watchdogServiceState(ctx, x, a, p)
		if err != nil {
			return err
		}
		if !loaded {
			return nil
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

const watchdogShellPrelude = "set -eu\nPATH=/usr/bin:/bin:/usr/sbin:/sbin; LC_ALL=C; export PATH LC_ALL\nunset PERL5OPT PERL5LIB PERLLIB ENV BASH_ENV CDPATH\numask 077\n"

func watchdogRun(ctx context.Context, x executor, script string) ([]byte, error) {
	var out bytes.Buffer
	r, err := x.Run(ctx, nil, []string{"/usr/bin/env", "-i", "PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LC_ALL=C", "/bin/sh", "-s"}, strings.NewReader(script), &limitedWriter{w: &out, limit: watchdog.MaxStatus})
	clear(r.Stderr)
	if err != nil || r.ExitCode != 0 {
		return nil, hostwatchdog.ErrIntegrity
	}
	return out.Bytes(), nil
}
func (w *WatchdogRemote) Apply(ctx context.Context, t hostwatchdog.Target, p hostwatchdog.Plan, action string) error {
	a, err := watchdog.Render(p.Config)
	if err != nil || p.Validate() != nil {
		return hostwatchdog.ErrIntegrity
	}
	return w.with(ctx, t, func(x watchdogBoundedExecutor, d docker) error {
		engine, err := d.engineID(ctx)
		if err != nil || engine != p.Config.EngineID || d.host != p.Config.DockerHost || d.binary != p.Config.DockerPath {
			return ErrIdentity
		}
		loaded, err := watchdogServiceState(ctx, x, a, p)
		if err != nil {
			return err
		}
		if action == "remove_host_watchdog" && loaded {
			prelude := watchdogShellPrelude + watchdogCheckFiles(a, p, true)
			if _, err = watchdogRun(ctx, x, prelude); err != nil {
				return err
			}
			if err = x.bootout(ctx, a, p); err != nil {
				return err
			}
			if err = watchdogAwaitRemoval(ctx, x, a, p); err != nil {
				return err
			}
			loaded = false
		}
		script, err := watchdogMutation(a, p, action, loaded)
		if err != nil {
			return err
		}
		_, err = watchdogRun(ctx, x, script)
		if err != nil {
			return err
		}
		loaded, err = watchdogServiceState(ctx, x, a, p)
		if err != nil {
			return err
		}
		if loaded != (action == "install_host_watchdog") {
			return hostwatchdog.ErrIntegrity
		}
		return nil
	})
}
func watchdogMutation(a watchdog.Artifact, p hostwatchdog.Plan, action string, loaded bool) (string, error) {
	var b strings.Builder
	b.WriteString(watchdogShellPrelude)
	fmt.Fprintf(&b, "test \"$(/usr/bin/id -u)\" = %s || exit 32\nd=%s\n", watchdogQuote(p.UID), watchdogQuote(a.Directory))
	switch action {
	case "install_host_watchdog":
		b.WriteString("if test ! -e \"$d\" && test ! -L \"$d\"; then\n/bin/mkdir -m 700 \"$d\"\n")
		for _, f := range a.Files {
			q := watchdogQuote(a.Directory + "/" + f.Name)
			fmt.Fprintf(&b, "printf '%%s' '%s' | /usr/bin/base64 -D > %s\n/bin/chmod %o %s\n", base64.StdEncoding.EncodeToString(f.Content), q, f.Mode.Perm(), q)
		}
		b.WriteString("fi\n" + watchdogCheck(a, p))
		if !loaded {
			fmt.Fprintf(&b, "/bin/launchctl bootstrap %s %s\n", watchdogQuote("gui/"+p.UID), watchdogQuote(a.Directory+"/"+watchdog.PlistName))
		}

	case "remove_host_watchdog":
		if loaded {
			return "", ErrRequest
		}
		b.WriteString("if test ! -e \"$d\" && test ! -L \"$d\"; then exit 0; fi\n" + watchdogCheckFiles(a, p, true))

		for _, name := range a.RuntimeFiles {
			q := watchdogQuote(a.Directory + "/" + name)
			fmt.Fprintf(&b, "if test -e %s || test -L %s; then test ! -L %s && test -f %s && test \"$(/usr/bin/stat -f '%%u' %s)\" = %s || exit 33; fi\n", q, q, q, q, q, watchdogQuote(p.UID))
		}
		for _, f := range a.Files {
			fmt.Fprintf(&b, "/bin/rm -f -- %s\n", watchdogQuote(a.Directory+"/"+f.Name))
		}
		for _, name := range a.RuntimeFiles {
			fmt.Fprintf(&b, "/bin/rm -f -- %s\n", watchdogQuote(a.Directory+"/"+name))
		}
		b.WriteString("/bin/rmdir \"$d\"\n")
	default:
		return "", ErrRequest
	}
	return b.String(), nil
}
func (w *WatchdogRemote) Status(ctx context.Context, t hostwatchdog.Target, p hostwatchdog.Plan) (*watchdog.Status, error) {
	a, err := watchdog.Render(p.Config)
	if err != nil {
		return nil, err
	}
	var status *watchdog.Status
	err = w.with(ctx, t, func(x watchdogBoundedExecutor, _ docker) error {
		q := watchdogQuote(a.Directory + "/" + watchdog.StatusName)
		script := watchdogShellPrelude + "d=" + watchdogQuote(a.Directory) + "\nif test ! -e \"$d\" && test ! -L \"$d\"; then exit 0; fi\n" + watchdogCheck(a, p) + "if test ! -e " + q + "; then exit 0; fi\ntest ! -L " + q + " && test -f " + q + " || exit 34\n/usr/bin/head -c 1025 " + q + "\n"
		raw, err := watchdogRun(ctx, x, script)
		if err != nil || len(raw) == 0 {
			return err
		}
		s, err := watchdog.ParseStatus(raw)
		if err == nil {
			status = &s
		}
		return err
	})
	return status, err
}
func (w *WatchdogRemote) Observe(ctx context.Context, t hostwatchdog.Target, p hostwatchdog.Plan, action string) (bool, error) {
	a, err := watchdog.Render(p.Config)
	if err != nil {
		return false, err
	}
	confirmed := false
	err = w.with(ctx, t, func(x watchdogBoundedExecutor, _ docker) error {
		loaded, err := watchdogServiceState(ctx, x, a, p)
		if err != nil {
			return err
		}
		if loaded != (action == "install_host_watchdog") {
			return hostwatchdog.ErrIntegrity
		}
		script := watchdogShellPrelude
		if loaded {
			script += watchdogCheck(a, p)
		} else {
			q := watchdogQuote(a.Directory)
			script += "test ! -e " + q + " && test ! -L " + q + "\n"
		}
		_, err = watchdogRun(ctx, x, script)
		confirmed = err == nil
		return err
	})
	return confirmed, err
}
