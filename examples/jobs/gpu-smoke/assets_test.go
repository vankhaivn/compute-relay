package gpusmoke

import (
	"strings"
	"testing"
)

func TestSmokeAssetsAreFixedIndependentAndNeverInstallDependencies(t *testing.T) {
	source := Sources()
	if len(source) != 2 || !strings.Contains(source["calculation.py"], "torch.mm(") || !strings.Contains(source["main.py"], "CC_INPUT_DIR") {
		t.Fatal("smoke workload is incomplete")
	}
	for _, text := range source {
		for _, forbidden := range []string{"pip install", "KAGGLE_API_TOKEN", "requests.", "http://", "https://", "time.sleep("} {
			if strings.Contains(text, forbidden) {
				t.Fatal("unexpected network, secret or idle work in smoke asset")
			}
		}
	}
	source["main.py"] = "caller replacement"
	if Sources()["main.py"] == source["main.py"] {
		t.Fatal("caller changed the fixed asset")
	}
}
