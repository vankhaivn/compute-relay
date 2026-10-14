package runtimehost

import (
	"context"
	"runtime"
	"testing"
)

func TestManagedCompositionNeedsNoPythonOrAccountRead(t *testing.T) {
	f := newManagedTestFixture(t, true)
	t.Setenv("PATH", t.TempDir())
	config := f.config
	config.MachineShape = "cpu"
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	s, err := f.host.managedWorkers(context.Background(), config)
	if err != nil {
		t.Fatal("native startup failed", err)
	}
	descriptors, err := s.connections.Descriptors(context.Background(), f.actor, "app")
	if err != nil || len(descriptors) != 2 || descriptors[0].Type != "kaggle" || descriptors[1].Type != "ssh" {
		t.Fatal("native adapters missing", descriptors, err)
	}
	if s.dispatcher == nil || f.vault.calls != 0 || f.adapter.checks != 0 {
		t.Fatal("startup touched credentials or misrepresented vault support")
	}
	f.connection(t, "saved-kaggle-key", "SYNTHETIC_FIRST")
	if _, err := f.host.managedWorkers(context.Background(), config); err != nil {
		t.Fatal("native startup rejected a saved Kaggle connection", err)
	}
}

func TestManagedCredentialChoicesNeedNoPython(t *testing.T) {
	for _, backend := range []string{"file", "keychain"} {
		t.Run(backend, func(t *testing.T) {
			f := newManagedTestFixture(t, true)
			config := f.config
			config.MachineShape, config.CredentialStore = "cpu", backend
			s, err := f.host.managedWorkers(context.Background(), config)
			if err != nil {
				t.Fatal("native credential choice failed", err)
			}
			if (s.dispatcher != nil) != (backend == "file" || runtime.GOOS == "darwin") {
				t.Fatal("credential backend availability changed")
			}
			selected, err := f.host.managedCredentialStore(context.Background(), "")
			if err != nil || selected != backend || f.vault.calls != 0 || f.adapter.checks != 0 {
				t.Fatal("startup changed the backend or accessed an account", err)
			}
		})
	}
}
