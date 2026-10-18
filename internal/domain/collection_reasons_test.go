package domain

import (
	"strings"
	"testing"
)

func TestCollectionReasonsAreClosed(t *testing.T) {
	reasons := strings.Fields(`artifact_request_invalid artifact_account_read_failed artifact_account_mismatch artifact_remote_read_failed artifact_remote_identity_mismatch artifact_status_read_failed artifact_not_terminal artifact_manifest_download_failed artifact_manifest_invalid artifact_file_limit artifact_byte_limit artifact_path_invalid artifact_output_undeclared artifact_output_missing artifact_stdout_download_failed artifact_stderr_download_failed artifact_environment_download_failed artifact_target_mismatch artifact_write_failed artifact_download_failed artifact_digest_mismatch artifact_catalog_invalid artifact_credential_unavailable artifact_call_timeout artifact_runtime_stopped artifact_binding_unavailable artifact_read_failed`)
	for _, reason := range reasons {
		if !ValidCollectionReason(reason) {
			t.Errorf("rejected collection reason %q", reason)
		}
	}
	for _, reason := range []string{"", "artifact_unknown", "private_provider_canary", "ARTIFACT_READ_FAILED", "artifact_read_failed\n", "https://provider.invalid"} {
		if ValidCollectionReason(reason) {
			t.Errorf("accepted arbitrary reason %q", reason)
		}
	}
}
