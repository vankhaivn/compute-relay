package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
)

type lostImageCompletion struct {
	connections.Repository
	lose bool
}

func (r *lostImageCompletion) FinishConnection(ctx context.Context, record connections.Record, result connections.Completion) error {
	if r.lose && record.Operation.Action == "install_image" {
		r.lose = false
		return errors.New("synthetic lost completion")
	}
	return r.Repository.FinishConnection(ctx, record, result)
}

func TestManagedImageInstallationRecoveryNeverRepeatsMutation(t *testing.T) {
	for _, beforeBuild := range []bool{false, true} {
		t.Run(fmt.Sprint("crash-before-build-", beforeBuild), func(t *testing.T) {
			ctx := context.Background()
			installed := ""
			builds, recoveries := 0, 0
			adapter := &managedSSHAdapter{config: ManagedServeConfig{MaxRemoteWallSeconds: 600}}
			adapter.verify = func(ctx context.Context, raw []byte, account string) (sshhost.HostVerification, error) {
				v, err := sshFixtureVerify(ctx, raw, account)
				v.ImageID = installed
				return v, err
			}
			adapter.install = func(ctx context.Context, raw []byte, account, architecture string, op domain.OperationID, recoverOnly bool) (sshhost.HostVerification, error) {
				if account == "" || architecture != "amd64" || !op.Valid() {
					t.Fatal("installation lost its frozen identity")
				}
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 5*time.Minute {
					t.Fatal("unbounded installation")
				}
				if recoverOnly {
					recoveries++
					if installed == "" {
						return sshhost.HostVerification{}, sshhost.ErrImageInstallationUnknown
					}
				} else {
					builds++
					installed = "sha256:" + strings.Repeat("1", 64)
				}
				return adapter.verify(ctx, raw, account)
			}
			f := newManagedTestFixture(t, true, adapter)
			_, connection := f.sshConnection(t, "image-create", "SYNTHETIC_SSH")
			if connection.Authentication != "verified" || connection.Selection != nil || connection.Host.Image.Status != "missing" {
				t.Fatal("missing image published a selection", connection)
			}
			body := []byte(fmt.Sprintf(`{"action":"install_image","expected_revision":%d}`, connection.Revision))
			op, err := f.services.connections.Submit(ctx, f.actor, "app", connection.ID, "image-install", body)
			if err != nil || op.Status != "accepted" {
				t.Fatal(op, err)
			}
			repo := &lostImageCompletion{Repository: managedConnectionRepository{f.host.store}, lose: !beforeBuild}
			resetWorker := func() {
				t.Helper()
				var err error
				f.services.connections, err = connections.New(f.host.access, repo, f.vault, []connections.Adapter{adapter}, time.Now)
				if err != nil {
					t.Fatal(err)
				}
			}
			resetWorker()
			if beforeBuild {
				started, err := f.host.store.BeginConnectionInstall(ctx, op.ID, time.Now().UTC())
				if err != nil || !started {
					t.Fatal("could not simulate consumed permit", err)
				}
			} else {
				if _, err = f.services.connections.RunOnce(ctx); err == nil || builds != 1 {
					t.Fatal("lost completion was not preserved", err, builds)
				}
			}
			resetWorker()
			if _, err = f.services.connections.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			outcome, err := f.services.connections.Operation(ctx, f.actor, "app", op.ID)
			if err != nil || recoveries != 1 {
				t.Fatal("recovery did not inspect", outcome, err, recoveries)
			}
			if beforeBuild {
				if outcome.Status != "failed" || outcome.Problem == nil || *outcome.Problem != "image_installation_unknown" || builds != 0 {
					t.Fatal("unknown outcome retried installation", outcome, builds)
				}
			} else if outcome.Status != "succeeded" || builds != 1 {
				t.Fatal("installed image was not recovered", outcome, builds)
			}
			replay, err := f.services.connections.Submit(ctx, f.actor, "app", connection.ID, "image-install", body)
			if err != nil || replay.ID != op.ID || !replay.Replay || replay.Status != "accepted" {
				t.Fatal("replay lost the original receipt", replay, err)
			}
			if worked, err := f.services.connections.RunOnce(ctx); err != nil || worked {
				t.Fatal("terminal installation queued another call", worked, err)
			}
			current, err := f.services.connections.Get(ctx, f.actor, "app", connection.ID)
			if err != nil {
				t.Fatal(err)
			}
			if beforeBuild {
				// A later explicit check may observe a delayed installation; it cannot build.
				installed = "sha256:" + strings.Repeat("2", 64)
				_, err = f.services.connections.Submit(ctx, f.actor, "app", current.ID, "image-check", []byte(fmt.Sprintf(`{"action":"check","expected_revision":%d}`, current.Revision)))
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.services.connections.RunOnce(ctx); err != nil {
					t.Fatal(err)
				}
				current, err = f.services.connections.Get(ctx, f.actor, "app", current.ID)
				if err != nil || builds != 0 {
					t.Fatal("check built an image", err)
				}
			}
			if current.Selection == nil || current.Host.Image.ID != installed {
				t.Fatal("verified installed image has no selection", current)
			}
			frozen, err := f.host.store.ReadManagedBinding(ctx, f.binding(t, current))
			if err != nil {
				t.Fatal(err)
			}
			config, err := parseManagedSSHRuntimeConfig(frozen.RuntimeConfig)
			if err != nil || config.Version != 2 || config.Image != installed || config.Architecture != "amd64" || config.RecipeSHA256 != sshhost.ImageRecipeDigest() {
				t.Fatal("image ID or platform was not frozen", config, err)
			}
		})
	}
}

func TestManagedSSHRuntimeKeepsLegacyImagePolicyAndRejectsInvalidInstalledImage(t *testing.T) {
	legacy := []byte(fmt.Sprintf(`{"schema_version":1,"image":%q}`, sshhost.StandardImage))
	if config, err := parseManagedSSHRuntimeConfig(legacy); err != nil || config.Version != 1 {
		t.Fatal("old jobs lost recovery", err)
	}
	for _, raw := range []string{
		fmt.Sprintf(`{"schema_version":2,"image":%q,"architecture":"amd64","recipe_sha256":%q}`, sshhost.StandardImage, sshhost.ImageRecipeDigest()),
		fmt.Sprintf(`{"schema_version":2,"image":%q,"architecture":"386","recipe_sha256":%q}`, "sha256:"+strings.Repeat("1", 64), sshhost.ImageRecipeDigest()),
		fmt.Sprintf(`{"schema_version":2,"image":%q,"architecture":"amd64","recipe_sha256":%q,"cpus":"2;evil"}`, "sha256:"+strings.Repeat("1", 64), sshhost.ImageRecipeDigest()),
	} {
		if _, err := parseManagedSSHRuntimeConfig([]byte(raw)); err == nil {
			t.Fatal("accepted an unfrozen or invalid installed image", raw)
		}
	}
}
