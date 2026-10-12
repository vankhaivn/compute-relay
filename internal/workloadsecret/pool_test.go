package workloadsecret

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func sampleSupply() (Key, Supply, time.Time) {
	now := time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC)
	return Key{"workspace", "job", "attempt"}, Supply{AttemptID: "attempt", ReadinessRevision: domain.SHA256Digest(strings.Repeat("a", 64)), ExpiresAt: now.Add(time.Minute).Format(time.RFC3339), Values: map[string][]byte{"ingest": []byte("synthetic-secret")}}, now
}
func TestStrictSupplyRejectsAmbiguousAndOversizedValues(t *testing.T) {
	_, request, _ := sampleSupply()
	raw, _ := json.Marshal(request)
	valid, err := ParseSupply(raw)
	if err != nil {
		t.Fatal(err)
	}
	valid.Clear()
	for _, raw := range []string{
		strings.Replace(string(raw), `"values":{`, `"unknown":true,"values":{`, 1),
		strings.Replace(string(raw), `"values":{`, `"values":null,"values":{`, 1),
		strings.Replace(string(raw), `"values":{`, `"values":{"ingest":"YQ==",`, 1),
		strings.Replace(string(raw), `"ingest":`, `"INGEST":`, 1),
		strings.Replace(string(raw), `"values":{`, `"values":[] ,"discard":{`, 1),
	} {
		if _, err := ParseSupply([]byte(raw)); !errors.Is(err, ErrRequest) {
			t.Fatal("ambiguous supply accepted", err)
		}
	}
	request.Values["ingest"] = bytes.Repeat([]byte("x"), MaxSlotBytes+1)
	raw, _ = json.Marshal(request)
	if _, err := ParseSupply(raw); !errors.Is(err, ErrRequest) {
		t.Fatal("unbounded slot", err)
	}
}
func TestPoolClaimsOnceAndClearsOriginalAndBorrowedCopies(t *testing.T) {
	key, req, now := sampleSupply()
	pool, _ := NewPool(bytes.Repeat([]byte{1}, 32))
	defer pool.Close()
	var fingerprint string
	receipt, err := pool.Supply(key, req, now, func(hash string, m Metadata) (Receipt, bool, error) {
		fingerprint = hash
		return Receipt{AuthorizationID: "secret_one", WorkspaceID: key.WorkspaceID, JobID: key.JobID, Metadata: m, Status: "granted", CreatedAt: now}, true, nil
	})
	if err != nil || len(fingerprint) != 64 {
		t.Fatal(err)
	}
	original := pool.entries[key].Values["ingest"]
	barrierErr := errors.New("transaction aborted")
	if _, err := pool.Claim(key, receipt.AuthorizationID, now, func() error { return barrierErr }); !errors.Is(err, barrierErr) || !pool.Available(key, receipt.AuthorizationID, now) {
		t.Fatal("failed barrier consumed material", err)
	}
	var wg sync.WaitGroup
	claims := make(chan Material, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, e := pool.Claim(key, receipt.AuthorizationID, now, func() error { return nil })
			if e == nil {
				claims <- m
			}
		}()
	}
	wg.Wait()
	close(claims)
	count := 0
	for material := range claims {
		count++
		borrowed := material.Values["ingest"]
		if !bytes.Equal(borrowed, req.Values["ingest"]) {
			t.Fatal("missing borrowed material")
		}
		material.Clear()
		if !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
			t.Fatal("borrowed bytes retained")
		}
	}
	if count != 1 || pool.Available(key, receipt.AuthorizationID, now) || !bytes.Equal(original, make([]byte, len(original))) {
		t.Fatal("claim repeated or retained original")
	}
}
func TestPoolFingerprintIsKeyedAndReplayCannotRestoreConsumedMaterial(t *testing.T) {
	key, req, now := sampleSupply()
	var hashes []string
	for _, keyByte := range []byte{1, 2} {
		pool, _ := NewPool(bytes.Repeat([]byte{keyByte}, 32))
		persist := func(hash string, m Metadata) (Receipt, bool, error) {
			hashes = append(hashes, hash)
			return Receipt{AuthorizationID: "secret_one", WorkspaceID: key.WorkspaceID, JobID: key.JobID, Metadata: m, Status: "granted", CreatedAt: now, Replay: true}, false, nil
		}
		if _, err := pool.Supply(key, req, now, persist); err != nil {
			t.Fatal(err)
		}
		if pool.Available(key, "secret_one", now) {
			t.Fatal("consumed replay restored plaintext")
		}
		pool.Close()
	}
	if hashes[0] == hashes[1] {
		t.Fatal("fingerprint was not keyed")
	}
}
func TestPoolExpiryAndRevocationEraseMaterial(t *testing.T) {
	for _, kind := range []string{"expiry", "revoke", "sweep", "close"} {
		t.Run(kind, func(t *testing.T) {
			key, req, now := sampleSupply()
			pool, _ := NewPool(bytes.Repeat([]byte{1}, 32))
			defer pool.Close()
			receipt, err := pool.Supply(key, req, now, func(_ string, m Metadata) (Receipt, bool, error) {
				return Receipt{AuthorizationID: "secret_one", WorkspaceID: key.WorkspaceID, JobID: key.JobID, Metadata: m, Status: "granted", CreatedAt: now}, true, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			retained := pool.entries[key].Values["ingest"]
			switch kind {
			case "expiry":
				pool.Available(key, receipt.AuthorizationID, now.Add(time.Minute))
			case "revoke":
				_, _ = pool.Revoke(key, receipt.AuthorizationID, func() (Receipt, error) { return receipt, nil })
			case "sweep":
				pool.Sweep(now, func(Key, domain.OperationID) bool { return false })
			case "close":
				pool.Close()
			}
			if pool.Available(key, receipt.AuthorizationID, now) || !bytes.Equal(retained, make([]byte, len(retained))) {
				t.Fatal("plaintext retained after", kind)
			}
		})
	}
}

func TestSupplyRejectsNoncanonicalBase64(t *testing.T) {
	for _, value := range []string{`YQ==\n`, `YR==`} {
		raw := []byte(`{"attempt_id":"attempt_1","readiness_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expires_at":"2099-10-12T01:00:00Z","values":{"ingest":"` + value + `"}}`)
		if _, err := ParseSupply(raw); err == nil {
			t.Fatal("noncanonical secret encoding accepted")
		}
	}
}
