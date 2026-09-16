package kaggle

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

const executionSpec = `{"api_version":"compute-connector/v1alpha1","name":"private name","profile":"profile","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","main.py"]},"inputs":[{"name":"input","target":"data.txt","source":{"kind":"object","object_id":"input"}}],"outputs":[{"path":"answer.txt","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`

func executionFixture(t *testing.T) (Config, provider.Plan, provider.Prepared) {
	t.Helper()
	c, plan := stagingFixture(t)
	plan.Job.Specification = []byte(executionSpec)
	plan.Job.SpecificationSHA256 = provider.Digest(plan.Job.Specification)
	stage, err := buildStagingPlan(c, DefaultStagingPolicy(), plan, "prep")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(stagingReference{"123", c.AccountName + "/" + stage.request.Slug, 1, stage.request.MarkerSHA256})
	return c, plan, provider.Prepared{Identity: plan.Job.Identity, PreparationID: "prep", PlanSHA256: plan.Digest(), Resource: string(raw), Ready: true, Private: true}
}

func TestExecutionPlanBindsSourceWithoutRenamingAnExistingIntent(t *testing.T) {
	c, plan, prepared := executionFixture(t)
	one, err := buildExecutionRequest(c, DefaultStagingPolicy(), DefaultExecutionPolicy(), plan, prepared)
	if err != nil {
		t.Fatal(err)
	}
	two, err := buildExecutionRequest(c, DefaultStagingPolicy(), DefaultExecutionPolicy(), plan.Clone(), prepared)
	if err != nil || one != two || one.SourceSHA256 != provider.Digest([]byte(one.Source)) || !strings.HasPrefix(one.Slug, "cre-") {
		t.Fatal("unstable source identity", err)
	}
	for _, text := range []string{c.PythonExecutable, string(c.CredentialRef), "private name"} {
		if strings.Contains(one.Source, text) {
			t.Fatal("local/private metadata entered kernel source")
		}
	}
	changed := DefaultExecutionPolicy()
	changed.AllowInternet = true
	plan.Job.Specification = []byte(strings.Replace(executionSpec, `"disabled"`, `"required"`, 1))
	plan.Job.SpecificationSHA256 = provider.Digest(plan.Job.Specification)
	stage, _ := buildStagingPlan(c, DefaultStagingPolicy(), plan, "prep")
	prepared.PlanSHA256 = plan.Digest()
	raw, _ := json.Marshal(stagingReference{"123", c.AccountName + "/" + stage.request.Slug, 1, stage.request.MarkerSHA256})
	prepared.Resource = string(raw)
	other, err := buildExecutionRequest(c, DefaultStagingPolicy(), changed, plan, prepared)
	if err != nil || other.Slug != one.Slug || other.SourceSHA256 == one.SourceSHA256 {
		t.Fatal("changed content selected a new identity", err)
	}
}

func TestExecutionPlanRejectsUnreadyRemappedOrInconsistentSnapshots(t *testing.T) {
	for _, mode := range []string{"unready", "public", "digest", "resource", "revision", "wall", "gpu", "internet", "input", "spec"} {
		t.Run(mode, func(t *testing.T) {
			c, plan, prepared := executionFixture(t)
			policy := DefaultExecutionPolicy()
			switch mode {
			case "unready":
				prepared.Ready = false
			case "public":
				prepared.Private = false
			case "digest":
				prepared.PlanSHA256 = provider.Digest(nil)
			case "resource":
				prepared.Resource = strings.Replace(prepared.Resource, `"version":1`, `"version":2`, 1)
			case "revision":
				c.Revision = "changed"
			case "wall":
				policy.MaxWallSeconds = 9
			case "gpu":
				policy.MachineShape = "NvidiaTeslaT4" // no silent upgrade of a CPU job
			case "internet":
				plan.Job.Specification = []byte(strings.Replace(executionSpec, `"disabled"`, `"required"`, 1))
			case "input":
				plan.Job.Specification = []byte(strings.Replace(executionSpec, `"object_id":"input"`, `"object_id":"another"`, 1))
			case "spec":
				plan.Job.Specification = []byte(`{"invalid":true}`)
			}
			if _, err := buildExecutionRequest(c, DefaultStagingPolicy(), policy, plan, prepared); err == nil {
				t.Fatal("unsafe execution plan accepted")
			}
		})
	}
}
