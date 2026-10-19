package kaggleapi

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var accountName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{1,49}$`)

type Account struct {
	Active   bool
	Username string
}

// IntrospectToken checks only the client's explicit token. It does not bind the
// returned account to an attempt; the caller must compare Username with its owner.
func (c *Client) IntrospectToken(ctx context.Context) (Account, error) {
	token := c.credential()
	defer clear(token)
	if len(token) == 0 {
		return Account{}, failure(authRPC, ErrConfig, 0, false)
	}
	object, err := c.rpc(ctx, authRPC, struct {
		Token string `json:"token"`
	}{string(token)}, maxAccountBytes)
	if err != nil {
		return Account{}, err
	}
	var result struct {
		Active   bool   `json:"active"`
		Username string `json:"username"`
	}
	if decodeObject(object, &result) != nil || bytes.Equal(object["active"], []byte("null")) ||
		(result.Active && (!accountName.MatchString(result.Username) || strings.Contains(result.Username, string(token)))) {
		return Account{}, failure(authRPC, ErrResponse, 0, true)
	}
	if !result.Active {
		return Account{}, nil
	}
	c.state.mu.Lock()
	if c.state.accountScope == "" {
		c.state.accountScope = result.Username
	}
	c.state.mu.Unlock()
	return Account{Active: result.Active, Username: result.Username}, nil
}

// Quota durations retain nanosecond precision. Unknown quota never inherits a
// zero allowance. FreeOnly describes the billing flag independently of whether
// all duration messages are present; it does not authorize accelerator use.
type Quota struct {
	Known    bool
	FreeOnly bool
	Reason   string
	Limit    time.Duration
	Used     time.Duration
	Reserved time.Duration
}

func (c *Client) AcceleratorQuota(ctx context.Context) (Quota, error) {
	object, err := c.rpc(ctx, quotaRPC, struct{}{}, maxAccountBytes)
	if err != nil {
		return Quota{}, err
	}
	raw := object["gpuQuota"]
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return Quota{Reason: "missing_quota"}, nil
	}
	quota, err := strictObject(raw)
	if err != nil {
		return Quota{}, failure(quotaRPC, ErrResponse, 0, true)
	}
	var fields struct {
		Billing  json.RawMessage `json:"isPayToScaleEnabled"`
		Limit    json.RawMessage `json:"totalTimeAllowed"`
		Used     json.RawMessage `json:"timeUsed"`
		Reserved json.RawMessage `json:"timeReserved"`
	}
	if decodeObject(quota, &fields) != nil {
		return Quota{}, failure(quotaRPC, ErrResponse, 0, true)
	}
	// The non-optional protobuf bool defaults only when absent. Explicit null,
	// strings, numbers or true preserve the helper's paid-or-unknown outcome.
	if len(fields.Billing) != 0 && !bytes.Equal(fields.Billing, []byte("false")) {
		return Quota{Reason: "paid_or_unknown"}, nil
	}
	result := Quota{FreeOnly: true, Reason: "missing_quota"}
	names := []string{"totalTimeAllowed", "timeUsed", "timeReserved"}
	for _, name := range names {
		raw := quota[name]
		if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
			return result, nil
		}
	}
	values := make([]time.Duration, 3)
	for i, name := range names {
		raw := quota[name]
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return Quota{}, failure(quotaRPC, ErrResponse, 0, true)
		}
		value, ok := duration(text)
		if !ok {
			return Quota{}, failure(quotaRPC, ErrResponse, 0, true)
		}
		values[i] = value
	}
	result.Known, result.Reason = true, "none"
	result.Limit, result.Used, result.Reserved = values[0], values[1], values[2]
	return result, nil
}

var protobufDuration = regexp.MustCompile(`^(0|[1-9][0-9]{0,7})(?:\.([0-9]{1,9}))?s$`)

func duration(text string) (time.Duration, bool) {
	parts := protobufDuration.FindStringSubmatch(text)
	if parts == nil {
		return 0, false
	}
	seconds, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || seconds > 366*24*60*60 {
		return 0, false
	}
	fraction := parts[2] + strings.Repeat("0", 9-len(parts[2]))
	nanos, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil || seconds == 366*24*60*60 && nanos != 0 {
		return 0, false
	}
	return time.Duration(seconds)*time.Second + time.Duration(nanos), true
}
