package domain

import "fmt"

// ValidateAttemptStateTransition enforces monotonic state movement in every dimension.
func ValidateAttemptStateTransition(current, next AttemptState) error {
	if err := current.Validate(); err != nil {
		return fmt.Errorf("current attempt state: %w", err)
	}
	if err := next.Validate(); err != nil {
		return fmt.Errorf("next attempt state: %w", err)
	}
	if err := validateTransition("orchestration", current.Orchestration, next.Orchestration, orchestrationTransitions); err != nil {
		return err
	}
	if err := validateTransition("execution", current.Execution, next.Execution, executionTransitions); err != nil {
		return err
	}
	if err := validateTransition("result", current.Result, next.Result, resultTransitions); err != nil {
		return err
	}
	if err := validateTransition("cancellation", current.Cancellation, next.Cancellation, cancellationTransitions); err != nil {
		return err
	}
	if err := validateTransition("remote activity", current.RemoteActivity, next.RemoteActivity, remoteActivityTransitions); err != nil {
		return err
	}
	if err := validateTransition("release evidence", current.ReleaseEvidence, next.ReleaseEvidence, releaseEvidenceTransitions); err != nil {
		return err
	}
	if current.DeadlineExceeded && !next.DeadlineExceeded {
		return TransitionError{Dimension: "deadline", From: "exceeded", To: "not_exceeded"}
	}
	return nil
}

// TransitionError describes a rejected state movement.
type TransitionError struct {
	Dimension string
	From      string
	To        string
}

func (e TransitionError) Error() string {
	return fmt.Sprintf("illegal %s transition from %q to %q", e.Dimension, e.From, e.To)
}

// Code allows application layers to map this error to the stable taxonomy.
func (e TransitionError) Code() ErrorCode {
	return CodeIllegalStateTransition
}

func validateTransition[T comparable](dimension string, current, next T, graph map[T]map[T]struct{}) error {
	if current == next {
		return nil
	}
	allowed, ok := graph[current]
	if !ok {
		return TransitionError{Dimension: dimension, From: fmt.Sprint(current), To: fmt.Sprint(next)}
	}
	if _, ok := allowed[next]; !ok {
		return TransitionError{Dimension: dimension, From: fmt.Sprint(current), To: fmt.Sprint(next)}
	}
	return nil
}

func setOf[T comparable](values ...T) map[T]struct{} {
	set := make(map[T]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}
