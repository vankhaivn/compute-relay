package domain

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// ProviderBinding freezes the provider-neutral profile resolution for a job.
type ProviderBinding struct {
	Profile               string
	ProviderInstanceID    ProviderInstanceID
	ConfigurationRevision string
}

func (binding ProviderBinding) Validate() error {
	if strings.TrimSpace(binding.Profile) == "" {
		return errors.New("profile must not be empty")
	}
	if !binding.ProviderInstanceID.Valid() {
		return errors.New("provider instance ID is invalid")
	}
	if strings.TrimSpace(binding.ConfigurationRevision) == "" {
		return errors.New("provider configuration revision must not be empty")
	}
	return nil
}

// Job is one immutable requested unit of business work.
type Job struct {
	ID                   JobID
	WorkspaceID          WorkspaceID
	Name                 string
	SpecificationVersion string
	SpecificationDigest  SHA256Digest
	Binding              ProviderBinding
	ActiveAttemptID      AttemptID
	CreatedAt            time.Time
}

// NewJob validates the immutable identity and resolution snapshot.
func NewJob(job Job) (Job, error) {
	if !job.ID.Valid() {
		return Job{}, errors.New("job ID is invalid")
	}
	if !job.WorkspaceID.Valid() {
		return Job{}, errors.New("workspace ID is invalid")
	}
	if strings.TrimSpace(job.Name) == "" {
		return Job{}, errors.New("job name must not be empty")
	}
	if strings.TrimSpace(job.SpecificationVersion) == "" {
		return Job{}, errors.New("job specification version must not be empty")
	}
	if !job.SpecificationDigest.Valid() {
		return Job{}, errors.New("job specification digest is invalid")
	}
	if err := job.Binding.Validate(); err != nil {
		return Job{}, fmt.Errorf("provider binding: %w", err)
	}
	if !job.ActiveAttemptID.Valid() {
		return Job{}, errors.New("active attempt ID is invalid")
	}
	if job.CreatedAt.IsZero() {
		return Job{}, errors.New("job creation time must not be zero")
	}
	return job, nil
}

// Attempt is one explicit compute execution and its independent state dimensions.
type Attempt struct {
	ID        AttemptID
	JobID     JobID
	Number    uint32
	State     AttemptState
	Revision  uint64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewAttempt creates a first-class attempt in the initial non-remote state.
func NewAttempt(id AttemptID, jobID JobID, number uint32, createdAt time.Time) (Attempt, error) {
	attempt := Attempt{
		ID:        id,
		JobID:     jobID,
		Number:    number,
		State:     InitialAttemptState(),
		Revision:  1,
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}
	if err := attempt.Validate(); err != nil {
		return Attempt{}, err
	}
	return attempt, nil
}

// Validate checks entity identity, timestamps, revision, and state consistency.
func (attempt Attempt) Validate() error {
	if !attempt.ID.Valid() {
		return errors.New("attempt ID is invalid")
	}
	if !attempt.JobID.Valid() {
		return errors.New("job ID is invalid")
	}
	if attempt.Number == 0 {
		return errors.New("attempt number must be positive")
	}
	if attempt.Revision == 0 {
		return errors.New("attempt revision must be positive")
	}
	if attempt.CreatedAt.IsZero() || attempt.UpdatedAt.IsZero() {
		return errors.New("attempt timestamps must not be zero")
	}
	if attempt.UpdatedAt.Before(attempt.CreatedAt) {
		return errors.New("attempt update time precedes creation")
	}
	if err := attempt.State.Validate(); err != nil {
		return fmt.Errorf("attempt state: %w", err)
	}
	return nil
}

// Transition returns an updated copy. No-op transitions keep the same revision and time.
func (attempt Attempt) Transition(next AttemptState, updatedAt time.Time) (Attempt, error) {
	if err := attempt.Validate(); err != nil {
		return Attempt{}, err
	}
	if err := ValidateAttemptStateTransition(attempt.State, next); err != nil {
		return Attempt{}, err
	}
	if attempt.State == next {
		return attempt, nil
	}
	if updatedAt.IsZero() {
		return Attempt{}, errors.New("attempt transition time must not be zero")
	}
	if updatedAt.Before(attempt.UpdatedAt) {
		return Attempt{}, errors.New("attempt transition time precedes current update time")
	}
	if attempt.Revision == math.MaxUint64 {
		return Attempt{}, errors.New("attempt revision overflow")
	}
	attempt.State = next
	attempt.Revision++
	attempt.UpdatedAt = updatedAt
	return attempt, nil
}
