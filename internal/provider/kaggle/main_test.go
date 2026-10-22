package kaggle

import (
	"os"
	"testing"
	"time"
)

var defaultRecheckGrace, defaultRecheckPoll = stagingRecheckGrace, stagingRecheckPoll

// Offline fixtures answer at once, so the pre-submit recheck window is shortened.
func TestMain(m *testing.M) {
	stagingRecheckGrace, stagingRecheckPoll = 50*time.Millisecond, time.Millisecond
	os.Exit(m.Run())
}

func TestStagingRecheckWindowOutlastsLiveCooldowns(t *testing.T) {
	if defaultRecheckGrace < 5*time.Minute || defaultRecheckPoll < 5*time.Second || defaultRecheckPoll > stagingRecheckMaxPoll {
		t.Fatal("pre-submit recheck window is too short for Kaggle cooldowns or polls too often")
	}
}
