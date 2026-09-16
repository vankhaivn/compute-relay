package staging

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func fixture(t testing.TB) (Identity, Object, []Input) {
	t.Helper()
	id := Identity{InstallationID: "installation", WorkspaceID: "workspace", JobID: "job", AttemptID: "attempt", InstanceID: "instance",
		ConfigurationRevision: "revision", PreparationID: "preparation", Nonce: strings.Repeat("a", 64), ParentPlanSHA256: strings.Repeat("b", 64),
		InputManifestSHA256: "3b6506ac37b0c2c25ccd45e50f7f3b049a123a31ffaf1226cf678bcff40432b3"}
	bundle := Object{"workspace", "code", 4, sum([]byte("code"))}
	inputs := []Input{{"data", "data.txt", Object{"workspace", "data", 5, sum([]byte("hello"))}}}
	return id, bundle, inputs
}
func planFixture(t testing.TB) Plan {
	t.Helper()
	id, b, inputs := fixture(t)
	p, err := NewPlan("fixture_user", id, b, inputs)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func inputHash(t testing.TB, inputs []Input) string {
	t.Helper()
	// Test-side implementation uses a sorted map-shaped JSON recipe, rather
	// than the production digestEntry type's declaration order.
	entries := make([]map[string]any, 0, len(inputs))
	for _, in := range inputs {
		entries = append(entries, map[string]any{"bytes": in.Object.Bytes, "name": in.Name, "sha256": in.Object.SHA256, "target": in.Target})
	}
	// Each test supplies target-sorted fixtures; reorder tests permute afterwards.
	raw, err := canonicalJSON(entries)
	if err != nil {
		t.Fatal(err)
	}
	return sum(raw)
}
func TestPlanDeterminismIsolationAndMetadata(t *testing.T) {
	id, b, inputs := fixture(t)
	p, err := NewPlan("fixture_user", id, b, inputs)
	if err != nil {
		t.Fatal(err)
	}
	q, err := NewPlan("fixture_user", id, b, inputs)
	if err != nil || !reflect.DeepEqual(p, q) {
		t.Fatal("non-deterministic plan", err)
	}
	if len(strings.Split(p.Reference(), "/")[1]) != 44 {
		t.Fatal("slug outside reviewed metadata bounds")
	}
	inputs[0].Object.SHA256 = strings.Repeat("c", 64)
	manifestBefore := p.Manifest()
	filesBefore := p.Files()
	p.Manifest()[0] = '!'
	p.Metadata()[0] = '!'
	p.Files()[0].Name = "../escape"
	if !reflect.DeepEqual(p.Manifest(), manifestBefore) || !reflect.DeepEqual(p.Files(), filesBefore) {
		t.Fatal("plan is externally mutable")
	}
	var m map[string]any
	if json.Unmarshal(p.Metadata(), &m) != nil {
		t.Fatal("invalid metadata")
	}
	if m["id"] != p.Reference() || m["title"] == "job" {
		t.Fatal("metadata leaks job title or changes target")
	}
	licenses := m["licenses"].([]any)
	if licenses[0].(map[string]any)["name"] != "copyright-authors" {
		t.Fatal("unexpected relicensing")
	}
	if _, ok := m["isPrivate"]; ok {
		t.Fatal("invented metadata privacy field")
	}
	if !strings.Contains(m["description"].(string), "does not relicense") {
		t.Fatal("rights notice missing")
	}
	if len(p.Files()) != 3 || p.Files()[2].Name != ManifestName || p.Files()[2].SHA256 != p.Digest() {
		t.Fatal("missing manifest identity")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if string(p.Manifest()) != string(manifestBefore) {
				t.Error("concurrent mutation")
			}
		}()
	}
	wg.Wait()
}
func TestPlanChangesIdentityForEveryFrozenBoundary(t *testing.T) {
	id, b, inputs := fixture(t)
	original, _ := NewPlan("fixture_user", id, b, inputs)
	for _, field := range []string{"InstallationID", "WorkspaceID", "JobID", "AttemptID", "InstanceID", "ConfigurationRevision", "PreparationID", "Nonce", "ParentPlanSHA256"} {
		t.Run(field, func(t *testing.T) {
			changed := id
			v := reflect.ValueOf(&changed).Elem().FieldByName(field)
			if field == "Nonce" || field == "ParentPlanSHA256" {
				v.SetString(strings.Repeat("c", 64))
			} else {
				v.SetString(v.String() + "-different")
			}
			bb := b
			in := append([]Input(nil), inputs...)
			if field == "WorkspaceID" {
				bb.WorkspaceID = changed.WorkspaceID
				in[0].Object.WorkspaceID = changed.WorkspaceID
			}
			p, err := NewPlan("fixture_user", changed, bb, in)
			if err != nil || p.Reference() == original.Reference() || p.Digest() == original.Digest() {
				t.Fatal("boundary omitted", err)
			}
		})
	}
	p, err := NewPlan("other_user", id, b, inputs)
	if err != nil || strings.Split(p.Reference(), "/")[1] == strings.Split(original.Reference(), "/")[1] {
		t.Fatal("account omitted", err)
	}
}
func TestPlanInputOrderAndPythonDigestRecipe(t *testing.T) {
	id, b, inputs := fixture(t)
	inputs = append(inputs, Input{"more", "z&<.txt", Object{"workspace", "extra", 0, sum(nil)}})
	id.InputManifestSHA256 = inputHash(t, inputs)
	p, err := NewPlan("fixture_user", id, b, inputs)
	if err != nil {
		t.Fatal("HTML-sensitive target recipe mismatch", err)
	}
	inputs[0], inputs[1] = inputs[1], inputs[0]
	q, err := NewPlan("fixture_user", id, b, inputs)
	if err != nil || !reflect.DeepEqual(p, q) {
		t.Fatal("input order changed identity", err)
	}
	id.InputManifestSHA256 = "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
	if p, err := NewPlan("fixture_user", id, b, nil); err != nil || len(p.Files()) != 2 {
		t.Fatal("empty-input recipe", err)
	}
}
func TestPlanRejectsInvalidAndCrossWorkspaceInputs(t *testing.T) {
	for _, mode := range []string{"account", "operation", "nonce", "digest", "foreign-bundle", "foreign-input", "negative", "large-bundle", "large-input", "too-many", "duplicate-name", "overlap", "traversal", "absolute", "backslash", "unicode", "object-conflict", "aggregate"} {
		t.Run(mode, func(t *testing.T) {
			id, b, inputs := fixture(t)
			account := "fixture_user"
			switch mode {
			case "account":
				account = "../other"
			case "operation":
				id.PreparationID = ""
			case "nonce":
				id.Nonce = "not-a-nonce"
			case "digest":
				id.InputManifestSHA256 = strings.Repeat("d", 64)
			case "foreign-bundle":
				b.WorkspaceID = "other"
			case "foreign-input":
				inputs[0].Object.WorkspaceID = "other"
			case "negative":
				inputs[0].Object.Bytes = -1
			case "large-bundle":
				b.Bytes = MaxBundleBytes + 1
			case "large-input":
				inputs[0].Object.Bytes = MaxInputBytes + 1
			case "too-many":
				for len(inputs) <= MaxInputs {
					inputs = append(inputs, inputs[0])
				}
			case "duplicate-name":
				inputs = append(inputs, Input{"DATA", "z.txt", Object{"workspace", "extra", 0, sum(nil)}})
			case "overlap":
				inputs = append(inputs, Input{"extra", "data.txt/nested", Object{"workspace", "extra", 0, sum(nil)}})
			case "traversal":
				inputs[0].Target = "../escape"
			case "absolute":
				inputs[0].Target = "/escape"
			case "backslash":
				inputs[0].Target = `C:\escape`
			case "unicode":
				inputs[0].Target = "dữ-liệu"
			case "object-conflict":
				inputs[0].Object.ID = b.ID
			case "aggregate":
				inputs[0].Object.Bytes = MaxInputBytes
				inputs = append(inputs, Input{"second", "y.txt", Object{"workspace", "second", MaxInputBytes, sum(nil)}}, Input{"third", "z.txt", Object{"workspace", "third", 1, sum(nil)}})
			}
			if mode != "digest" {
				id.InputManifestSHA256 = inputHash(t, inputs)
			}
			if p, err := NewPlan(account, id, b, inputs); err == nil || p.Reference() != "" {
				t.Fatal("invalid plan returned identity")
			}
		})
	}
}
func FuzzPlanTargetValidation(f *testing.F) {
	f.Add("data.txt")
	f.Add("../escape")
	f.Add("a//b")
	f.Add("z&<.txt")
	f.Fuzz(func(t *testing.T, target string) {
		if len(target) > 2048 {
			return
		}
		id, b, inputs := fixture(t)
		inputs[0].Target = target
		id.InputManifestSHA256 = inputHash(t, inputs)
		p, err := NewPlan("fixture_user", id, b, inputs)
		if err == nil {
			if !safeTarget(target) || p.Reference() == "" {
				t.Fatal("unsafe plan")
			}
			if len(p.Manifest()) > MaxManifestBytes {
				t.Fatal("unbounded manifest")
			}
		}
	})
}
