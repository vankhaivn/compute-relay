package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
)

type managedImageAdapter struct {
	managedAdapter
	unknown   bool
	installed bool
	installs  int
}

func (a *managedImageAdapter) Verify(ctx context.Context, secret []byte) (connections.Verification, error) {
	verified, err := a.managedAdapter.Verify(ctx, secret)
	image := &connections.ImageEvidence{Status: "missing", RecipeSHA256: strings.Repeat("a", 64)}
	if a.installed {
		image.Status, image.ID = "ready", "sha256:"+strings.Repeat("b", 64)
	}
	verified.Host = &connections.HostEvidence{HostKeyFingerprint: "SHA256:" + strings.Repeat("A", 43), OS: "linux", Architecture: "amd64", Image: image}
	verified.SelectionUnavailable = !a.installed
	return verified, err
}

func (*managedImageAdapter) PlanImageInstallation(c connections.Connection) (connections.ImageInstallation, error) {
	return connections.ImageInstallation{RecipeSHA256: c.Host.Image.RecipeSHA256, Architecture: c.Host.Architecture}, nil
}

func (a *managedImageAdapter) InstallImage(ctx context.Context, secret []byte, record connections.Record, recoverOnly bool) (connections.Verification, error) {
	a.installs++
	if a.unknown {
		return connections.Verification{}, connections.ErrImageInstallationUnknown
	}
	a.installed = true
	return a.Verify(ctx, secret)
}

func TestManagedHTTPImageInstallationReceiptsAndReplay(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint("unknown-", unknown), func(t *testing.T) {
			ctx := context.Background()
			adapter := &managedImageAdapter{unknown: unknown}
			f := setupManagedHTTP(t, true, adapter)
			base := "/v1/workspaces/a"
			_, raw := f.call(t, "POST", base+"/connections", "manage", "image-create", managedBody, 202)
			created := readManagedOperation(t, raw)
			if worked, err := f.service.RunOnce(ctx); err != nil || !worked {
				t.Fatal("connection verification", worked, err)
			}
			connectionPath := base + "/connections/" + created.ConnectionID
			_, raw = f.call(t, "GET", connectionPath, "read", "", "", 200)
			wireSchema(t, "connection", raw)
			var connection connections.Connection
			if err := json.Unmarshal(raw, &connection); err != nil || connection.Authentication != "verified" || connection.Selection != nil {
				t.Fatal("missing image published a selection", err)
			}
			body := fmt.Sprintf(`{"action":"install_image","expected_revision":%d}`, connection.Revision)
			headers, raw := f.call(t, "POST", connectionPath+"/actions", "manage", "image-install", body, 202)
			first := readManagedOperation(t, raw)
			location := base + "/connection-operations/" + string(first.ID)
			if first.Action != "install_image" || first.Status != "accepted" || first.Replay || headers.Get("Location") != location || adapter.installs != 0 {
				t.Fatal("installation did not return its durable receipt before provider effects", first)
			}
			assertReplay := func() {
				t.Helper()
				_, raw := f.call(t, "POST", connectionPath+"/actions", "manage", "image-install", body, 202)
				replay := readManagedOperation(t, raw)
				if !replay.Replay || replay.ID != first.ID || replay.Status != "accepted" || replay.ConnectionRevision != first.ConnectionRevision {
					t.Fatal("installation replay changed the original receipt", replay)
				}
			}
			assertReplay()
			_, raw = f.call(t, "GET", location, "manage", "", "", 200)
			if got := readManagedOperation(t, raw); got.Status != "accepted" || got.ID != first.ID {
				t.Fatal("accepted installation is not readable", got)
			}
			if _, found, err := f.store.NextConnectionOperation(ctx); err != nil || !found {
				t.Fatal("could not claim accepted installation", found, err)
			}
			_, raw = f.call(t, "GET", location, "manage", "", "", 200)
			if got := readManagedOperation(t, raw); got.Status != "running" || got.ID != first.ID || adapter.installs != 0 {
				t.Fatal("running installation is not readable", got)
			}
			if worked, err := f.service.RunOnce(ctx); err != nil || !worked || adapter.installs != 1 {
				t.Fatal("installation worker did not run exactly once", worked, err, adapter.installs)
			}
			_, raw = f.call(t, "GET", location, "manage", "", "", 200)
			completed := readManagedOperation(t, raw)
			if unknown {
				if completed.Status != "failed" || completed.Problem == nil || *completed.Problem != "image_installation_unknown" {
					t.Fatal("unknown installation outcome was not returned", completed)
				}
			} else if completed.Status != "succeeded" || completed.Problem != nil {
				t.Fatal("successful installation outcome was not returned", completed)
			}
			assertReplay()
			if worked, err := f.service.RunOnce(ctx); err != nil || worked || adapter.installs != 1 {
				t.Fatal("HTTP replay repeated image installation", worked, err, adapter.installs)
			}
			for _, who := range []string{"read", "write", "operate", "execute"} {
				f.call(t, "GET", location, who, "", "", 403)
			}
		})
	}
}
