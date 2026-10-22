package kaggle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestApplicationImageIsRejectedBeforeKaggleMutation(t *testing.T) {
	p, plan, _, _, remote := nativeRuntime(t, false)
	image := domain.ObjectMetadata{WorkspaceID: plan.Job.Identity.WorkspaceID, ID: "image", Bytes: 77,
		SHA256: domain.SHA256Digest(strings.Repeat("e", 64))}
	plan.Job.Inputs.ApplicationImage = &image
	plan.Job.Specification = []byte(strings.Replace(executionSpec, `"execution":`,
		fmt.Sprintf(`"application_image":{"object_id":"image","sha256":%q,"bytes":77},"execution":`, image.SHA256), 1))
	plan.Job.SpecificationSHA256 = provider.Digest(plan.Job.Specification)
	if err := plan.Job.Validate(); err != nil {
		t.Fatal(err)
	}
	_, err := p.Validate(context.Background(), plan.Job)
	assertStepReason(t, err, "application_image_unsupported")
	var problem domain.Problem
	if !errors.As(err, &problem) || problem.Code != domain.CodeUnsupportedCapability ||
		remote.count("CreateDataset") != 0 || remote.count("SaveKernel") != 0 {
		t.Fatalf("application image was not rejected before mutation: problem=%+v err=%v", problem, err)
	}
}
