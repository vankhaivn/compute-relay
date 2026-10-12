package sshhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

// TestCredentialsNeverLeaveTheLogin drives the adapter over the real SSH transport with
// each credential kind. No secret byte may reach a remote command, its input or anything
// the adapter returns.
func TestCredentialsNeverLeaveTheLogin(t *testing.T) {
	const password = "pw-canary-7f3a9c"
	passphrase := "pass-canary-51e0"
	key, public := newClientKey(t, []byte(passphrase))
	logins := map[string]map[string]string{
		"password": {FieldPassword: password},
		"key":      {FieldPrivateKey: string(key), FieldPassphrase: passphrase},
	}
	for name, login := range logins {
		t.Run(name, func(t *testing.T) {
			canaries := []string{password, passphrase, "PRIVATE KEY", string(key[40:80])}
			ctx := context.Background()
			fake := newFakeHost(t)
			var mu sync.Mutex
			var remote bytes.Buffer
			server := newTestServer(t)
			server.Password, server.ClientKey = password, public
			server.Handle = func(command string, stdin io.Reader, stdout, stderr io.Writer) int {
				input, _ := io.ReadAll(stdin)
				mu.Lock()
				remote.WriteString(command)
				remote.Write(input)
				mu.Unlock()
				if strings.HasSuffix(command, "'version' '--format' '{{.Server.Os}}/{{.Server.Arch}}'") {
					_, _ = io.WriteString(stdout, "linux/amd64\n")
					return 0
				}
				var in io.Reader
				if len(input) > 0 {
					in = bytes.NewReader(input)
				}
				result, err := fake.Run(ctx, nil, commandArgs(t, command), in, stdout)
				if err != nil {
					return 255
				}
				_, _ = stderr.Write(result.Stderr)
				return result.ExitCode
			}
			target := server.target()
			fields := map[string]string{FieldHost: target.Host, FieldPort: strconv.Itoa(target.Port), FieldUsername: target.User}
			for k, v := range login {
				fields[k] = v
			}
			raw, err := EncodeFields(fields)
			if err != nil {
				t.Fatal(err)
			}
			account, err := VerifyHost(ctx, raw)
			if err != nil {
				t.Fatal(err)
			}
			blobs := testBlobs{}
			job := testJob(t, testSpec, blobs)
			binding := provider.BindingSnapshot{Binding: job.Binding, AccountScope: "scope_fixture", CredentialRef: "vault:ssh-fixture"}
			adapter, err := NewAdapter(Config{InstanceID: "instance", Account: account, CredentialRef: "vault:ssh-fixture", MaxWallSeconds: 60, Image: StandardImage},
				binding, &testVault{raw: raw}, blobs, &testClock{now: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)})
			if err != nil {
				t.Fatal(err)
			}

			var returned []any
			record := func(values ...any) {
				for _, v := range values {
					if err, ok := v.(error); ok && err != nil {
						v = err.Error()
					}
					returned = append(returned, v)
				}
			}
			check, err := adapter.Check(ctx)
			record(check, err)
			plan, err := adapter.Validate(ctx, job)
			record(plan, err)
			prepared, err := adapter.Prepare(ctx, plan, testOperation)
			if err != nil {
				t.Fatal(err)
			}
			out := adapter.Submit(ctx, prepared)
			if out.Status != provider.SubmissionAccepted {
				t.Fatalf("submit = %+v", out)
			}
			ref := *out.Remote
			record(prepared, out)
			fake.finish(0, map[string]string{"control/stdout.log": "out\n", "control/stderr.log": "", "control/environment.json": "{}",
				"control/execution-result.json": resultManifest(map[string]string{"answer.txt": "42"}), "outputs/answer.txt": "42"})
			observation, err := adapter.Observe(ctx, ref)
			record(observation, err)
			logs, err := adapter.ReadLogs(ctx, ref, provider.PageRequest{Limit: 10})
			record(logs, err)
			listed, err := adapter.ListArtifacts(ctx, ref, provider.PageRequest{Limit: 10})
			record(listed, err)
			for _, artifact := range listed.Artifacts {
				var data bytes.Buffer
				receipt, err := adapter.FetchArtifact(ctx, ref, artifact, &data, 1<<20)
				record(receipt, err, data.String())
			}
			cleaned, err := adapter.Cleanup(ctx, provider.CleanupRequest{Remote: ref, LedgerID: "ledger_fixture", CreationOperationID: testOperation,
				ResultsCollected: true, Mode: provider.CleanupApply})
			record(cleaned, err)
			if !cleaned.Deleted {
				t.Fatalf("cleanup = %+v, %v", cleaned, err)
			}

			serialized, err := json.Marshal(returned)
			if err != nil {
				t.Fatal(err)
			}
			evidence := remote.String() + string(serialized) + fmt.Sprint(returned)
			if !strings.Contains(evidence, "'container' 'create'") || !strings.Contains(evidence, "relay/staging/input-000.bin") {
				t.Fatal("remote commands and staged input were not captured")
			}
			for _, canary := range canaries {
				if strings.Contains(evidence, canary) {
					t.Fatalf("credential bytes %q left the login", canary)
				}
			}
		})
	}
}

// commandArgs reverses the transport's quoting: optional NAME='value' assignments, then
// single-quoted arguments.
func commandArgs(t *testing.T, command string) []string {
	t.Helper()
	var args []string
	var word strings.Builder
	quoted, started := false, false
	for i := 0; i < len(command); i++ {
		switch c := command[i]; {
		case c == '\'':
			quoted, started = !quoted, true
		case c == '\\' && !quoted && i+1 < len(command):
			i++
			word.WriteByte(command[i])
		case c == ' ' && !quoted:
			if started {
				args = append(args, word.String())
			}
			word.Reset()
			started = false
		default:
			word.WriteByte(c)
			started = true
		}
	}
	if quoted {
		t.Fatalf("unterminated command %q", command)
	}
	if started {
		args = append(args, word.String())
	}
	for len(args) > 0 {
		name, _, ok := strings.Cut(args[0], "=")
		if !ok || !envName.MatchString(name) {
			break
		}
		args = args[1:]
	}
	return args
}
