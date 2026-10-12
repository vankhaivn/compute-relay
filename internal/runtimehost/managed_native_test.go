package runtimehost

import (
	"context"
	"runtime"
	"testing"
)

func TestManagedSSHOnlyCompositionNeedsNoPythonOrAccountRead(t *testing.T) {
	f := newManagedTestFixture(t, true)
	config := f.config
	config.PythonExecutable, config.MachineShape = "", "cpu"
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
	s, err := f.host.managedWorkers(context.Background(), config)
	if err != nil {
		t.Fatal("SSH-only startup failed", err)
	}
	descriptors, err := s.connections.Descriptors(context.Background(), f.actor, "app")
	if err != nil || len(descriptors) != 1 || descriptors[0].Type != "ssh" {
		t.Fatal("SSH-only mode advertised Kaggle", descriptors, err)
	}
	if (s.dispatcher != nil) != (runtime.GOOS == "darwin") || f.vault.calls != 0 || f.adapter.checks != 0 {
		t.Fatal("startup touched credentials or misrepresented vault support")
	}
	f.connection(t, "saved-kaggle-key", "SYNTHETIC_FIRST")
	if _, err := f.host.managedWorkers(context.Background(), config); err != ErrRequest {
		t.Fatal("SSH-only startup stranded a saved Kaggle connection", err)
	}
}
