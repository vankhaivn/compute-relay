package domain

// These errors describe LOCAL HTTP validation/backpressure, never provider quota or
// remote execution. They are additive to the M2-02 public error taxonomy.
const (
	CodeRuntimeUnavailable   ErrorCode = "RUNTIME_UNAVAILABLE"
	CodeRequestTimeout       ErrorCode = "REQUEST_TIMEOUT"
	CodeInvalidRequest       ErrorCode = "INVALID_REQUEST"
	CodeRequestLimitExceeded ErrorCode = "REQUEST_LIMIT_EXCEEDED"
)

func init() {
	errorCategories[CodeRuntimeUnavailable] = ErrorCategoryLocalRuntime
	errorCategories[CodeRequestTimeout] = ErrorCategoryLocalRuntime
	errorCategories[CodeInvalidRequest] = ErrorCategoryValidation
	errorCategories[CodeRequestLimitExceeded] = ErrorCategoryLocalRuntime
}
