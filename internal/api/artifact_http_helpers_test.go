package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"
)

func artifactTestRequest(ctx context.Context, endpoint string, token []byte, target artifactTarget, suffix, cursor string, limit int) (*http.Response, error) {
	query := url.Values{"attempt_id": {target.AttemptID}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	path := "/v1/workspaces/" + url.PathEscape(target.WorkspaceID) + "/jobs/" + url.PathEscape(target.JobID) + "/artifacts" + suffix
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+path+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+string(token))
	return http.DefaultClient.Do(req)
}

func listTestArtifacts(ctx context.Context, endpoint string, token []byte, target artifactTarget, cursor string, limit int) (artifactPage, error) {
	var page artifactPage
	resp, err := artifactTestRequest(ctx, endpoint, token, target, "", cursor, limit)
	if err != nil {
		return page, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return page, errors.New("artifact list failed")
	}
	err = json.NewDecoder(resp.Body).Decode(&page)
	return page, err
}

func testArtifactMetadata(ctx context.Context, endpoint string, token []byte, target artifactTarget, id string) (artifactMetadata, error) {
	var meta artifactMetadata
	resp, err := artifactTestRequest(ctx, endpoint, token, target, "/"+url.PathEscape(id), "", 0)
	if err != nil {
		return meta, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return meta, errors.New("artifact metadata failed")
	}
	err = json.NewDecoder(resp.Body).Decode(&meta)
	return meta, err
}

func downloadTestArtifact(ctx context.Context, endpoint string, token []byte, meta artifactMetadata, dst io.Writer) error {
	resp, err := artifactTestRequest(ctx, endpoint, token, meta.artifactTarget, "/"+url.PathEscape(meta.Artifact.ID)+"/content", "", 0)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("artifact download failed")
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, hash), resp.Body)
	if err != nil {
		return err
	}
	if n != meta.Artifact.Bytes || hex.EncodeToString(hash.Sum(nil)) != meta.Artifact.SHA256 {
		return errors.New("artifact bytes differ from metadata")
	}
	if resp.Trailer.Get(verifiedArtifactTrailer) != "true" {
		return errors.New("artifact acknowledgement missing")
	}
	return nil
}

func requireNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
