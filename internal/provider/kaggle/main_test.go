package kaggle

import (
	"os"
	"testing"
	"time"
)

var defaultRecheckGrace, defaultRecheckPoll, defaultPrivacyPoll = stagingRecheckGrace, stagingRecheckPoll, stagingPrivacyPoll

// Offline fixtures answer at once, so the pre-submit recheck window and privacy rereads are shortened.
func TestMain(m *testing.M) {
	stagingRecheckGrace, stagingRecheckPoll, stagingPrivacyPoll = 50*time.Millisecond, time.Millisecond, time.Millisecond
	os.Exit(m.Run())
}

func TestStagingRecheckWindowOutlastsLiveCooldowns(t *testing.T) {
	if defaultRecheckGrace < 5*time.Minute || defaultRecheckPoll < 5*time.Second || defaultRecheckPoll > stagingRecheckMaxPoll ||
		defaultPrivacyPoll < time.Second || stagingPrivacyRereads < 2 {
		t.Fatal("pre-submit recheck window is too short for Kaggle cooldowns or polls too often")
	}
}
