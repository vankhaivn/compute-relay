package hosthealth

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

const grantSamplerRule = "(root) NOPASSWD: /usr/bin/powermetrics --samplers smc -n 1 -i 1000"

func grantListing(defaults string, rules ...string) string {
	text := ""
	if defaults != "" {
		text = "Matching Defaults entries for REDACTED on REDACTED:\n    " + defaults + "\n\n"
	}
	return text + "User REDACTED may run the following commands on REDACTED:\n    " + strings.Join(rules, "\n    ") + "\n"
}

func TestRecordedGrantClassifiesOnlyPasswordlessAuthority(t *testing.T) {
	raw, err := os.ReadFile("testdata/grant-recorded.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := ClassifyGrant(Sample{Status: Available, Data: raw}); got != "narrow" {
		t.Fatal("ordinary passworded administrator access changed exact passwordless grant", got)
	}
	samples := fixtures(t)
	samples[Sudo] = Sample{Status: Available, Data: raw}
	snapshot := Parse(testResource, time.Now().UTC(), samples)
	requireValue(t, snapshot.SudoGrant, "narrow")
	if snapshot.CPUDieTemperatureCelsius.Status != Available || snapshot.GPUDieTemperatureCelsius.Status != Available || snapshot.FanRPM.Status != Available {
		t.Fatal("narrow real listing withheld typed sampler metrics")
	}
	for _, warning := range snapshot.Warnings {
		if warning == "sudo_grant_broader" {
			t.Fatal("ordinary passworded admin access became a broader grant warning")
		}
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil || snapshot.Validate() != nil || strings.Contains(string(encoded), "REDACTED") || strings.Contains(string(encoded), "env_keep") || strings.Contains(string(encoded), "sudo_lecture") {
		t.Fatal("source listing text escaped the typed snapshot", err)
	}
}

func TestGrantOrderingTagsAndContinuationsStayConservative(t *testing.T) {
	for _, test := range []struct {
		name, want string
		rules      []string
	}{
		{"exact_sampler", "narrow", []string{grantSamplerRule}},
		{"ordinary_admin_before", "narrow", []string{"(ALL) ALL", grantSamplerRule}},
		{"explicit_passworded_admin_before", "narrow", []string{"(ALL : ALL) PASSWD: ALL", grantSamplerRule}},
		{"several_passworded_admin_before", "narrow", []string{"(root) PASSWD: ALL", "(ALL) ALL", grantSamplerRule}},
		{"root_only_and_all_groups", "narrow", []string{"(ALL:ALL) NOPASSWD: /usr/bin/powermetrics"}},
		{"ordinary_admin_after", "broader", []string{grantSamplerRule, "(ALL) ALL"}},
		{"explicit_passworded_admin_after", "broader", []string{grantSamplerRule, "(root) PASSWD: ALL"}},
		{"sampler_password_override_after", "broader", []string{grantSamplerRule, "(root) PASSWD: /usr/bin/powermetrics"}},
		{"additional_passwordless_before", "broader", []string{"(root) NOPASSWD: /bin/sh", grantSamplerRule}},
		{"additional_passwordless_after", "broader", []string{grantSamplerRule, "(root) NOPASSWD: /bin/sh"}},
		{"passwordless_all_before", "broader", []string{"(ALL) NOPASSWD: ALL", grantSamplerRule}},
		{"duplicate_sampler", "broader", []string{grantSamplerRule, grantSamplerRule}},
		{"ordinary_admin_only", "absent", []string{"(ALL) ALL"}},
		{"passwordless_tag_inherited", "broader", []string{grantSamplerRule + ", /bin/sh"}},
		{"password_tag_then_passwordless", "broader", []string{"(ALL) PASSWD: ALL, NOPASSWD: /usr/bin/powermetrics"}},
		{"passwordless_then_password_tag", "broader", []string{grantSamplerRule + ", PASSWD: /bin/sh"}},
		{"extra_tag", "broader", []string{"(root) NOPASSWD: SETENV: /usr/bin/powermetrics"}},
		{"negated_command", "broader", []string{"(root) NOPASSWD: !/usr/bin/powermetrics"}},
		{"negated_runas", "broader", []string{"(ALL, !root) NOPASSWD: /usr/bin/powermetrics"}},
		{"other_runas", "broader", []string{"(nobody) NOPASSWD: /usr/bin/powermetrics"}},
		{"wildcard_arguments", "broader", []string{"(root) NOPASSWD: /usr/bin/powermetrics *"}},
		{"wrapped_arguments", "broader", []string{"(root) NOPASSWD: /usr/bin/powermetrics --samplers smc -n 1 -i\n        1000"}},
		{"wrapped_inherited_command", "broader", []string{grantSamplerRule + ",\n        /bin/sh"}},
		{"explicit_continuation", "broader", []string{grantSamplerRule + " \\\n        /bin/sh"}},
		{"trailing_unparsed_text", "broader", []string{grantSamplerRule, "PASSWD: /bin/sh"}},
		{"command_defaults_after", "broader", []string{grantSamplerRule, "Runas and Command-specific defaults for REDACTED:", "!authenticate"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyGrant(Sample{Status: Available, Data: []byte(grantListing("", test.rules...))}); got != test.want {
				t.Fatal("grant classification", got, "want", test.want)
			}
		})
	}
}

func TestGrantPasswordOnlyAndEmptyCompleteListingsAreAbsent(t *testing.T) {
	for _, test := range []struct{ name, listing, want string }{
		{"explicit_password_only", grantListing("authenticate, listpw=never", "(ALL) PASSWD: ALL"), "absent"},
		{"ordinary_password_only", grantListing("authenticate, listpw=never", "(ALL) ALL"), "absent"},
		{"default_authentication", grantListing("listpw=never", "(ALL) ALL"), "absent"},
		{"several_password_only", grantListing("authenticate", "(root) PASSWD: ALL", "(ALL : ALL) ALL"), "absent"},
		{"bare_complete_rule", "(ALL) PASSWD: ALL\n", "absent"},
		{"empty_commands_with_defaults", grantListing("authenticate, listpw=never"), "absent"},
		{"empty_commands_without_defaults", grantListing(""), "absent"},
		{"unknown_defaults_password_only", grantListing("!authenticate", "(ALL) PASSWD: ALL"), "broader"},
		{"unknown_defaults_empty", grantListing("exempt_group=admin"), "broader"},
		{"incomplete_defaults_without_commands", "Matching Defaults entries for REDACTED on REDACTED:\n    authenticate, listpw=never\n", "broader"},
		{"incomplete_commands_header", "User REDACTED may run the following commands on REDACTED\n", "broader"},
		{"unparsed_successful_whitespace", " \n\t", "broader"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyGrant(Sample{Status: Available, Data: []byte(test.listing)}); got != test.want {
				t.Fatal("passwordless authority classification", got, "want", test.want)
			}
		})
	}
}

func TestGrantDefaultsMustBeCompletelyRecognized(t *testing.T) {
	for _, test := range []struct{ defaults, want string }{
		{"authenticate, listpw=any", "narrow"},
		{"listpw=all", "narrow"},
		{"listpw=always", "narrow"},
		{"listpw=never", "narrow"},
		{`env_reset, env_keep+=LANG, env_keep+="LC_ALL LC_CTYPE", lecture_file=/etc/sudo_lecture, !log_allowed`, "narrow"},
		{"!authenticate", "broader"},
		{"!authenticate, authenticate", "broader"},
		{"exempt_group=admin", "broader"},
		{"authenticate, exempt_group=wheel", "broader"},
		{"rootpw", "broader"},
		{"unknown_option", "broader"},
		{"timestamp_timeout=0", "broader"},
		{"listpw=something_else", "broader"},
		{"env_reset,", "broader"},
		{"env_reset,\n        env_keep+=LANG", "broader"},
		{"env_keep+=LANG LC_ALL", "broader"},
		{`env_keep+="LANG LC_ALL`, "broader"},
		{`env_keep+="LANG, !authenticate"`, "broader"},
		{`env_keep+="LANG \\\"LC_ALL\\\""`, "broader"},
		{`env_keep+=""`, "broader"},
		{"Defaults !authenticate", "broader"},
	} {
		t.Run(test.defaults, func(t *testing.T) {
			if got := ClassifyGrant(Sample{Status: Available, Data: []byte(grantListing(test.defaults, "(ALL) ALL", grantSamplerRule))}); got != test.want {
				t.Fatal("Defaults classification", got, "want", test.want)
			}
		})
	}
}

func TestGrantListingRejectsPartialHeadersAndUnavailableSamples(t *testing.T) {
	valid := grantListing("env_reset", "(ALL) ALL", grantSamplerRule)
	for _, text := range []string{
		"unexpected policy preamble\n" + valid,
		"Matching Defaults entries for REDACTED on REDACTED:\n" + grantListing("", grantSamplerRule),
		strings.Replace(valid, "User REDACTED", "User OTHER", 1),
		strings.ReplaceAll(valid, "for REDACTED", "for root"),
		strings.ReplaceAll(grantListing("", "(ALL) ALL", grantSamplerRule), "User REDACTED", "User root"),
		strings.Replace(valid, "commands on REDACTED:", "commands on OTHER:", 1),
		strings.Replace(valid, "commands on REDACTED:", "commands:", 1),
		valid + "User REDACTED may run the following commands on REDACTED:\n",
		valid + "Matching Defaults entries for REDACTED on REDACTED:\n    !authenticate\n",
		strings.Replace(valid, "env_reset", "env_reset\r", 1),
	} {
		if got := ClassifyGrant(Sample{Status: Available, Data: []byte(text)}); got != "broader" {
			t.Fatal("incomplete or contradictory successful listing accepted", got)
		}
	}
	for _, sample := range []Sample{
		{Status: Unavailable, Data: []byte(valid)},
		{Status: Available, Reason: "output_limit", Data: []byte(valid)},
		{Status: Available, Data: []byte(strings.Repeat(" ", MaxSourceBytes+1) + valid)},
		{Status: Available, Data: []byte(valid + "\x00")},
	} {
		if got := ClassifyGrant(sample); got != "absent" {
			t.Fatal("failed or incomplete source granted narrow authority", got)
		}
	}
}
