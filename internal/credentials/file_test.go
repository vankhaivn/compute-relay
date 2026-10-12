package credentials

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func fileFixture(t *testing.T) *File {
	t.Helper()
	f, err := NewFile("rt_fixture", filepath.Join(t.TempDir(), "credentials"))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func fileSecret(t *testing.T, f *File, key string, want []byte) {
	t.Helper()
	var borrowed []byte
	err := f.WithSecret(context.Background(), key, func(value []byte) error {
		borrowed = value
		if !bytes.Equal(value, want) {
			t.Fatal("credential contents changed")
		}
		return nil
	})
	if err != nil || !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
		t.Fatal("read failed or owned buffer was retained", err)
	}
}

func TestFileVaultImmutableReplayRestartAndExactDeletion(t *testing.T) {
	f := fileFixture(t)
	ctx := context.Background()
	secret := bytes.Repeat([]byte{0, 0xff, '\n', '$', '"'}, MaxVaultBytes/5)
	if err := f.Create(ctx, "op_fixture", secret); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(f.directory, fileVaultName))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Create(ctx, "op_fixture", secret); err != nil {
		t.Fatal("identical creation did not replay", err)
	}
	if err := f.Create(ctx, "op_fixture", []byte("different")); !errors.Is(err, ErrConflict) {
		t.Fatal("immutable entry was replaced", err)
	}
	after, err := os.ReadFile(filepath.Join(f.directory, fileVaultName))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("replay/conflict rewrote the store")
	}
	reopened, err := NewFile("rt_fixture", f.directory)
	if err != nil {
		t.Fatal(err)
	}
	fileSecret(t, reopened, "op_fixture", secret)
	if err := reopened.Create(ctx, "keep", []byte("SYNTHETIC_KEEP")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := reopened.Delete(ctx, "op_fixture"); err != nil {
			t.Fatal("exact deletion did not replay", err)
		}
	}
	if err := reopened.WithSecret(ctx, "op_fixture", func([]byte) error { t.Fatal("deleted value read"); return nil }); !errors.Is(err, ErrUnconfigured) {
		t.Fatal(err)
	}
	fileSecret(t, reopened, "keep", []byte("SYNTHETIC_KEEP"))
	if statefs.CheckFile(filepath.Join(f.directory, fileVaultName)) != nil || statefs.CheckDir(f.directory) != nil {
		t.Fatal("credential permissions are not private")
	}
}

func TestFileVaultConcurrentChangesDoNotLoseEntries(t *testing.T) {
	f := fileFixture(t)
	var workers sync.WaitGroup
	for i := range 24 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := f.Create(context.Background(), fmt.Sprintf("entry_%d", i), []byte{byte(i)}); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	for i := range 24 {
		fileSecret(t, f, fmt.Sprintf("entry_%d", i), []byte{byte(i)})
	}
}

func TestFileVaultFailedAndUnacknowledgedPublication(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			f := fileFixture(t)
			ctx := context.Background()
			if err := f.Create(ctx, "original", []byte("SYNTHETIC_ORIGINAL")); err != nil {
				t.Fatal(err)
			}
			f.publish = func(from, to string) error {
				if committed {
					if err := os.Rename(from, to); err != nil {
						t.Fatal(err)
					}
				}
				return errors.New("SYNTHETIC_PRIVATE_FAILURE")
			}
			if err := f.Create(ctx, "new", []byte("SYNTHETIC_NEW")); err != ErrUnavailable {
				t.Fatal("publication error leaked or was acknowledged", err)
			}
			f.publish = os.Rename
			fileSecret(t, f, "original", []byte("SYNTHETIC_ORIGINAL"))
			if committed {
				fileSecret(t, f, "new", []byte("SYNTHETIC_NEW"))
			} else if err := f.WithSecret(ctx, "new", func([]byte) error { t.Fatal("uncommitted entry read"); return nil }); err != ErrUnconfigured {
				t.Fatal(err)
			}
			if err := f.Create(ctx, "new", []byte("SYNTHETIC_NEW")); err != nil {
				t.Fatal("original intent could not replay", err)
			}
			items, err := os.ReadDir(f.directory)
			if err != nil || len(items) != 1 || items[0].Name() != fileVaultName {
				t.Fatal("temporary credential file retained")
			}
		})
	}
}

func TestFileVaultRejectsCorruptionAndUnsafeFiles(t *testing.T) {
	for _, data := range []string{
		"", "COMPUTE_RELAY_VAULT=2\nINSTALLATION_ID=rt_fixture\n", "COMPUTE_RELAY_VAULT=1\nINSTALLATION_ID=rt_other\n",
		"COMPUTE_RELAY_VAULT=1\nINSTALLATION_ID=rt_fixture\nSECRET_61=YQ==\nSECRET_61=Yg==\n",
		"COMPUTE_RELAY_VAULT=1\nINSTALLATION_ID=rt_fixture\nSECRET_61=$(SYNTHETIC_COMMAND)\n",
		"COMPUTE_RELAY_VAULT=1\nINSTALLATION_ID=rt_fixture\nSECRET_61=YQ==", // truncated publication
		"COMPUTE_RELAY_VAULT=1\nINSTALLATION_ID=rt_fixture\nSECRET_=YQ==\n",
		"COMPUTE_RELAY_VAULT=1\nINSTALLATION_ID=rt_fixture\nSECRET_zz=YQ==\n",
		strings.Repeat("x", maxFileVaultBytes+1),
	} {
		f := fileFixture(t)
		path := filepath.Join(f.directory, fileVaultName)
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := f.Create(context.Background(), "new", []byte("SYNTHETIC_SECRET")); err != ErrUnavailable {
			t.Fatal("corrupt file was accepted or overwritten", err)
		}
		actual, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(actual, []byte(data)) {
			t.Fatal("corrupt evidence was replaced")
		}
	}
	for _, kind := range []string{"symlink", "hardlink", "directory", "permissions"} {
		t.Run(kind, func(t *testing.T) {
			f := fileFixture(t)
			path := filepath.Join(f.directory, fileVaultName)
			target := filepath.Join(f.directory, "outside")
			if err := os.WriteFile(target, append(f.header(), []byte("SECRET_61=YQ==\n")...), 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(target, path)
				if err != nil && runtime.GOOS == "windows" {
					t.Skip("Windows symlink privilege unavailable")
				}
			case "hardlink":
				err = os.Link(target, path)
			case "directory":
				err = os.Mkdir(path, 0700)
			case "permissions":
				if runtime.GOOS == "windows" {
					t.Skip("Windows uses DACLs, covered by statefs")
				}
				err = os.WriteFile(path, f.header(), 0644)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := f.WithSecret(context.Background(), "a", func([]byte) error { t.Fatal("unsafe read"); return nil }); err != ErrUnavailable {
				t.Fatal("unsafe file was read", err)
			}
			if err := f.Create(context.Background(), "new", []byte("SYNTHETIC_SECRET")); err != ErrUnavailable {
				t.Fatal("unsafe file was overwritten", err)
			}
		})
	}
}

func TestFileVaultClearsBorrowedBytesAfterErrorAndPanic(t *testing.T) {
	f := fileFixture(t)
	ctx := context.Background()
	if err := f.Create(ctx, "entry", []byte("SYNTHETIC_SECRET")); err != nil {
		t.Fatal(err)
	}
	for _, panics := range []bool{false, true} {
		var borrowed []byte
		func() {
			defer func() { _ = recover() }()
			_ = f.WithSecret(ctx, "entry", func(raw []byte) error {
				borrowed = raw
				if panics {
					panic("synthetic")
				}
				return errors.New("synthetic")
			})
		}()
		if len(borrowed) == 0 || !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
			t.Fatal("callback retained owned secret bytes")
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := f.Create(cancelled, "cancelled", []byte("secret")); err != context.Canceled {
		t.Fatal("cancelled creation proceeded", err)
	}
}

func TestFileVaultExecutableChild(t *testing.T) {
	mode := os.Getenv("COMPUTE_RELAY_FILE_VAULT_CHILD")
	if mode == "" {
		return
	}
	root, err := statefs.Open(os.Getenv("COMPUTE_RELAY_FILE_VAULT_ROOT"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	f, err := NewFile("rt_process_fixture", filepath.Join(root.Path, "credentials"))
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("SYNTHETIC_PROCESS_SECRET")
	if mode == "create" {
		if err := f.Create(context.Background(), "entry", secret); err != nil {
			t.Fatal(err)
		}
	} else {
		fileSecret(t, f, "entry", secret)
	}
}

func TestFileVaultReopensFromAnotherExecutable(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "runtime")
	for _, mode := range []string{"create", "read"} {
		binary := filepath.Join(t.TempDir(), "relay-"+mode+".exe")
		if err := os.WriteFile(binary, raw, 0700); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary, "-test.run=^TestFileVaultExecutableChild$")
		cmd.Env = append(os.Environ(), "COMPUTE_RELAY_FILE_VAULT_CHILD="+mode, "COMPUTE_RELAY_FILE_VAULT_ROOT="+root)
		output, err := cmd.CombinedOutput()
		if err != nil || bytes.Contains(output, []byte("SYNTHETIC_PROCESS_SECRET")) {
			t.Fatal("cross-executable credential operation failed; output withheld", err)
		}
	}
}
