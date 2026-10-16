package kaggle

import "testing"

func TestKaggleKeepsItsOneDayWallMaximum(t *testing.T) {
	if MaxWallSeconds != 86400 || RuntimeCapabilities("cpu").MaxWallSeconds != MaxWallSeconds || RuntimeCapabilities("NvidiaTeslaT4").MaxWallSeconds != MaxWallSeconds {
		t.Fatal("Kaggle advertises a longer wall maximum")
	}
	policy := DefaultExecutionPolicy()
	for wall, valid := range map[int64]bool{MaxWallSeconds: true, MaxWallSeconds + 1: false, 604800: false} {
		policy.MaxWallSeconds = wall
		if policy.valid() != valid {
			t.Fatalf("Kaggle execution policy wall %d", wall)
		}
	}
}
