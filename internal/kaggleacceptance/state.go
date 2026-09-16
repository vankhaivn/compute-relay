package kaggleacceptance

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/statefs"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type session struct {
	root    *statefs.Root
	store   *sqlite.Store
	inputs  *blobfs.Store
	results *blobfs.Store
	record  record
	access  *auth.Service
	actor   auth.Principal
	tokenID string
}

func (s *session) close() error {
	var err error
	if s.store != nil && s.tokenID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = s.store.RevokeToken(ctx, s.tokenID)
		cancel()
	}
	if s.results != nil {
		err = errors.Join(err, s.results.Close())
	}
	if s.inputs != nil {
		err = errors.Join(err, s.inputs.Close())
	}
	if s.store != nil {
		err = errors.Join(err, s.store.Close())
	}
	if s.root != nil {
		err = errors.Join(err, s.root.Close())
	}
	return err
}

func openSession(ctx context.Context, o Options, fixture bool) (result *session, err error) {
	s := &session{}
	defer func() {
		if err != nil {
			_ = s.close()
		}
	}()
	create := o.Mode == "prepare"
	if create {
		if _, err = statefs.PrivateDir(o.Root, true); err != nil {
			return nil, ErrState
		}
	} else if statefs.CheckDir(o.Root) != nil {
		return nil, ErrState
	}
	s.root, err = statefs.Open(o.Root)
	if err != nil {
		return nil, ErrState
	}
	root := s.root.Path
	if !create {
		if readRecord(filepath.Join(root, "acceptance.json"), &s.record) != nil {
			return nil, ErrState
		}
		for _, name := range []string{"state", "inputs", "results"} {
			if statefs.CheckDir(filepath.Join(root, name)) != nil {
				return nil, ErrState
			}
		}
		if s.record.Protocol != 1 || s.record.Config.Validate() != nil || s.record.ProgramSHA256 != o.ProgramSHA256 || s.record.Fixture != fixture || !domain.SHA256Digest(s.record.Challenge).Valid() || !s.record.Receipt.JobID.Valid() || !s.record.Receipt.AttemptID.Valid() || s.record.Receipt.Replay || (s.record.MachineShape != "NvidiaTeslaT4" && s.record.MachineShape != "NvidiaTeslaP100") {
			return nil, ErrState
		}
	}
	bundle, err := bundleBytes()
	if err != nil {
		return nil, ErrState
	}
	request, err := admission.Parse([]byte(jobSpecification))
	if err != nil {
		return nil, ErrState
	}
	if create {
		challenge, err := randomNonce()
		if err != nil {
			return nil, err
		}
		s.record = record{Protocol: 1, Config: o.Config, MachineShape: o.MachineShape, ProgramSHA256: o.ProgramSHA256, Challenge: challenge, BundleSHA256: provider.Digest(bundle), SpecificationSHA256: provider.Digest(request.Canonical()), Fixture: fixture}
	}
	input, err := inputBytes(s.record.Challenge)
	if err != nil {
		return nil, ErrState
	}
	if create {
		s.record.InputSHA256 = provider.Digest(input)
	}
	if s.record.BundleSHA256 != provider.Digest(bundle) || s.record.InputSHA256 != provider.Digest(input) || s.record.SpecificationSHA256 != provider.Digest(request.Canonical()) {
		return nil, ErrState
	}
	s.store, err = sqlite.Open(ctx, filepath.Join(root, "state"), sqlite.DefaultOptions())
	if err != nil {
		return nil, err
	}
	s.inputs, err = blobfs.New(filepath.Join(root, "inputs"), blobfs.Limits{MaxObjectBytes: 2 << 20, MaxTotalBytes: 4 << 20})
	if err != nil {
		return nil, err
	}
	s.results, err = blobfs.New(filepath.Join(root, "results"), blobfs.Limits{MaxObjectBytes: 24 << 20, MaxTotalBytes: 64 << 20})
	if err != nil {
		return nil, err
	}
	if create {
		if err := s.store.PutWorkspace(ctx, auth.Workspace{ID: Workspace, Enabled: true, AllowedProfiles: []string{ProfileName}}); err != nil {
			return nil, err
		}
		binding := s.record.binding()
		profile := admission.DefaultProfile(binding.Binding, binding.AccountScope)
		profile.CredentialRef, profile.MaxRemoteWallSeconds = binding.CredentialRef, 120
		if err := s.store.PutProfile(ctx, profile, true); err != nil {
			return nil, err
		}
		if err := s.store.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
			return nil, err
		}
		for id, data := range map[domain.ObjectID][]byte{"acceptance_code": bundle, "acceptance_input": input} {
			metadata, err := s.inputs.Put(ctx, domain.ObjectMetadata{ID: id, WorkspaceID: Workspace, Bytes: -1}, bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
			if err := s.store.CommitObject(ctx, metadata); err != nil {
				return nil, err
			}
		}
	}
	// Ephemeral local application authority, never a provider token or a file secret.
	s.access, err = auth.New(s.store, s.store, nil)
	if err != nil {
		return nil, err
	}
	secret, _, err := s.access.Issue(ctx, Workspace, []auth.Scope{auth.Read, auth.Write, auth.Operate}, time.Now().Add(time.Hour))
	if err != nil {
		return nil, err
	}
	s.actor, err = s.access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		return nil, err
	}
	s.tokenID = s.actor.TokenID()
	if create {
		service, err := admission.New(s.access, s.store, admission.DefaultLimits())
		if err != nil {
			return nil, err
		}
		s.record.Receipt, err = service.Submit(ctx, s.actor, Workspace, "fixed-gpu-acceptance-v1", request.Canonical())
		if err != nil {
			return nil, err
		}
		if err := writeRecord(filepath.Join(root, "acceptance.json"), s.record); err != nil {
			return nil, err
		}
	}
	original, err := s.store.ReadJob(ctx, Workspace, s.tokenID, s.record.Receipt.JobID)
	if err != nil || original.Attempt.ID != s.record.Receipt.AttemptID || original.Attempt.Number != 1 || original.Job.ActiveAttemptID != s.record.Receipt.AttemptID || original.Profile.Binding != s.record.binding().Binding || original.Profile.AccountScope != s.record.Config.AccountName || original.Profile.CredentialRef != string(s.record.Config.CredentialRef) || provider.Digest(original.Request.Canonical()) != s.record.SpecificationSHA256 || len(original.Objects) != 2 {
		return nil, ErrState
	}
	for _, object := range original.Objects {
		expected := s.record.BundleSHA256
		if object.Role != "bundle" {
			expected = s.record.InputSHA256
		}
		if object.Object.SHA256 != expected {
			return nil, ErrState
		}
	}
	return s, nil
}

func (s *session) journal(ctx context.Context) (dispatch.Journal, error) {
	return s.store.InspectDispatch(ctx, Workspace, s.tokenID, s.record.Receipt.JobID, s.record.Receipt.AttemptID)
}
func (s *session) original(ctx context.Context) (provider.Plan, provider.Prepared, error) {
	j, err := s.journal(ctx)
	if err != nil || j.Plan == nil || j.Prepared == nil {
		return provider.Plan{}, provider.Prepared{}, ErrState
	}
	return j.Plan.Clone(), *j.Prepared, nil
}

// The marker is written AFTER a successful new M3 submission-intent commit and
// BEFORE that commit's caller receives its permit. If marker I/O fails, no helper
// may run; recovery retains the journaled uncertainty and never rearms submission.
type markedRepository struct {
	dispatch.Repository
	s       *session
	allow   bool
	process string
}

func (r *markedRepository) CommitDispatch(ctx context.Context, handle dispatch.Handle, action dispatch.Action, now time.Time) (dispatch.Work, error) {
	if handle.Claim.WorkspaceID != Workspace || handle.Claim.JobID != r.s.record.Receipt.JobID || handle.Claim.AttemptID != r.s.record.Receipt.AttemptID {
		return dispatch.Work{}, ErrState
	}
	if !r.allow && (action.Kind == dispatch.BeginPreparation || action.Kind == dispatch.BeginSubmission) {
		return dispatch.Work{}, ErrPermission
	}
	work, err := r.Repository.CommitDispatch(ctx, handle, action, now)
	if err != nil {
		return work, err
	}
	if action.Kind == dispatch.BeginSubmission {
		if work.Journal.Plan == nil || !work.Journal.SubmitStarted {
			return dispatch.Work{}, ErrState
		}
		mark := submissionMark{1, r.process, work.Journal.Plan.Digest(), work.Journal.Plan.Job.Identity.IntentID, now.UTC()}
		if err := writeRecord(filepath.Join(r.s.root.Path, "submission-process.json"), mark); err != nil {
			return dispatch.Work{}, err
		}
	}
	return work, nil
}
