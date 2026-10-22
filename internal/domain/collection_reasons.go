package domain

// ValidCollectionReason reports whether reason is a provider-neutral collection diagnostic.
func ValidCollectionReason(reason string) bool {
	switch reason {
	case "artifact_request_invalid", "artifact_account_read_failed",
		"artifact_account_mismatch", "artifact_remote_read_failed",
		"artifact_remote_identity_mismatch", "artifact_status_read_failed",
		"artifact_not_terminal", "artifact_manifest_download_failed",
		"artifact_manifest_invalid", "artifact_manifest_missing", "artifact_file_limit",
		"artifact_byte_limit", "artifact_path_invalid",
		"artifact_output_undeclared", "artifact_output_missing",
		"artifact_stdout_download_failed", "artifact_stderr_download_failed",
		"artifact_environment_download_failed", "artifact_target_mismatch",
		"artifact_write_failed", "artifact_download_failed",
		"artifact_digest_mismatch", "artifact_catalog_invalid",
		"artifact_credential_unavailable", "artifact_call_timeout",
		"artifact_runtime_stopped", "artifact_binding_unavailable",
		"artifact_read_failed", "artifact_collection_failed", "artifact_resolver_panic",
		"artifact_discovery_panic", "artifact_verification_panic", "artifact_transfer_panic",
		"artifact_catalog_failed", "artifact_blob_read_failed", "artifact_blob_write_failed":
		return true
	default:
		return false
	}
}
