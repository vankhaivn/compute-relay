package kaggleapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"testing"
)

func TestDatasetFilePagingScalesWithinFixedCallBound(t *testing.T) {
	for _, count := range []int{1, 200, MaxStagingFiles} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			calls := 0
			c := newFixtureClient(t, func(r *http.Request) (*http.Response, error) {
				calls++
				var query struct {
					Owner   string `json:"ownerSlug"`
					Slug    string `json:"datasetSlug"`
					Version int    `json:"datasetVersionNumber"`
					Size    int    `json:"pageSize"`
					Cursor  string `json:"pageToken"`
				}
				if json.NewDecoder(r.Body).Decode(&query) != nil || query.Owner != fixtureDataset.Owner || query.Slug != fixtureDataset.Slug || query.Version != 1 || query.Size != 200 {
					t.Fatal("dataset page lost frozen binding or batch size", query)
				}
				offset := 0
				if query.Cursor != "" {
					var err error
					offset, err = strconv.Atoi(query.Cursor)
					if err != nil {
						t.Fatal(err)
					}
				}
				end := min(offset+query.Size, count)
				files := make([]map[string]any, 0, end-offset)
				for i := offset; i < end; i++ {
					files = append(files, map[string]any{"name": fmt.Sprintf("file-%03d", i), "totalBytes": 1})
				}
				next := ""
				if end < count {
					next = strconv.Itoa(end)
				}
				body, _ := json.Marshal(map[string]any{"datasetFiles": files, "nextPageToken": next})
				return response(200, "application/json", string(body)), nil
			}, nil)
			files, err := c.ListDatasetFiles(context.Background(), fixtureDataset, count, int64(count))
			if err != nil || len(files) != count || calls != (count+199)/200 {
				t.Fatal("scaled list did not complete within page bound", len(files), calls, err)
			}
		})
	}
}

func TestDatasetFilePagingRejectsNonprogressAndOverlongInventory(t *testing.T) {
	for _, empty := range []bool{false, true} {
		calls := 0
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
			calls++
			files := []map[string]any{}
			if !empty {
				files = append(files, map[string]any{"name": fmt.Sprintf("file-%03d", calls)})
			}
			body, _ := json.Marshal(map[string]any{"datasetFiles": files, "nextPageToken": strconv.Itoa(calls)})
			return response(200, "application/json", string(body)), nil
		}, nil)
		files, err := c.ListDatasetFiles(context.Background(), fixtureDataset, MaxStagingFiles, MaxStagingFiles)
		wantCalls := 16
		if empty {
			wantCalls = 1
		}
		if err == nil || files != nil || calls != wantCalls {
			t.Fatal("non-complete inventory escaped fixed bound", calls, err)
		}
	}
}

func TestOwnedKernelInventoryRequiresCompleteExplicitPages(t *testing.T) {
	for _, mode := range []string{"found-last", "absent", "missing-array", "null-array", "later-refusal", "cycle", "empty-next", "page-cap"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			c := newFixtureClient(t, func(r *http.Request) (*http.Response, error) {
				calls++
				var q struct {
					Group, User, Language, KernelType, OutputType, Search string
					PageSize                                              int
					PageToken                                             string
				}
				if json.NewDecoder(r.Body).Decode(&q) != nil || q.Group != "PROFILE" || q.User != fixtureKernel.Owner || q.Language != "all" || q.KernelType != "all" || q.OutputType != "all" || q.Search != "" || q.PageSize != 100 {
					t.Fatal("owned inventory became filtered or unbounded", q)
				}
				if calls == 2 {
					switch mode {
					case "missing-array":
						return response(200, "application/json", `{}`), nil
					case "null-array":
						return response(200, "application/json", `{"kernels":null}`), nil
					case "later-refusal":
						return response(403, "application/json", `{"private":"SYNTHETIC_TOKEN"}`), nil
					case "cycle":
						return response(200, "application/json", `{"kernels":[{"ref":"fixture_user/other"}],"nextPageToken":"page2"}`), nil
					case "empty-next":
						return response(200, "application/json", `{"kernels":[],"nextPageToken":"page3"}`), nil
					}
				}
				kernels := make([]map[string]any, 100)
				for i := range kernels {
					kernels[i] = map[string]any{"ref": fixtureKernel.Owner + fmt.Sprintf("/other-%03d", (calls-1)*100+i)}
				}
				if mode == "found-last" && calls == 3 {
					kernels[99] = map[string]any{"ref": fixtureKernel.reference()}
				}
				next := ""
				if calls < 3 || mode == "page-cap" {
					next = "page" + strconv.Itoa(calls+1)
				}
				raw, _ := json.Marshal(map[string]any{"kernels": kernels, "nextPageToken": next})
				return response(200, "application/json", string(raw)), nil
			}, nil)
			found, err := c.OwnedKernelPresent(context.Background(), fixtureKernel)
			if mode == "found-last" || mode == "absent" {
				if err != nil || found != (mode == "found-last") || calls != 3 {
					t.Fatal("owned inventory proof incomplete", found, calls, err)
				}
			} else if mode == "page-cap" {
				if !errors.Is(err, ErrLimit) || found || calls != 100 {
					t.Fatal("capped inventory became absence", found, calls, err)
				}
			} else if err == nil || found || calls != 2 {
				t.Fatal("malformed/partial inventory became absence", found, calls, err)
			}
			if mode == "missing-array" || mode == "null-array" {
				if !errors.Is(err, ErrResponse) {
					t.Fatal("malformed inventory lost reason category", err)
				}
			}
		})
	}
}
