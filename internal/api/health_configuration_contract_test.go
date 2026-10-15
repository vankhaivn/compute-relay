package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
)

type healthConfigurationAdapter struct{ watchdogHTTPAdapter }

func (*healthConfigurationAdapter) ValidateConfiguration(c connections.Configuration) error {
	return c.Validate()
}

func TestConfiguredHostHealthConnectionResponseSchema(t *testing.T) {
	f := setupManagedHTTP(t, true, &healthConfigurationAdapter{})
	base := "/v1/workspaces/a/connections"
	_, raw := f.call(t, "POST", base, "manage", "health-config-create", strings.Replace(managedBody, `"fixture"`, `"ssh"`, 1), 202)
	created := readManagedOperation(t, raw)
	_, err := f.service.RunOnce(context.Background())
	requireNoError(t, err)
	path := base + "/" + created.ConnectionID
	_, raw = f.call(t, "GET", path, "read", "", "", 200)
	connection := decodeHTTPJSON[connections.Connection](t, raw)
	for _, battery := range []int{100, 10} {
		body := fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":900,"host_health":{"low_battery_percent":%d}}}`, connection.Revision, battery)
		_, raw = f.call(t, "POST", path+"/actions", "manage", fmt.Sprintf("health-config-%d", battery), body, 202)
		readManagedOperation(t, raw)
		_, err = f.service.RunOnce(context.Background())
		requireNoError(t, err)
		_, raw = f.call(t, "GET", path, "read", "", "", 200)
		wireSchema(t, "connection", raw)
		connection = decodeHTTPJSON[connections.Connection](t, raw)
		if connection.Configuration == nil || connection.Configuration.HostHealth == nil || connection.Configuration.HostHealth.LowBatteryPercent == nil || *connection.Configuration.HostHealth.LowBatteryPercent != float64(battery) {
			t.Fatal("configured policy was not projected")
		}
		var wire map[string]any
		requireNoError(t, json.Unmarshal(raw, &wire))
		studioConnection(t, wire)
	}
}
