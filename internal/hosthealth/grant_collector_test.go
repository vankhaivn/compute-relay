package hosthealth

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestCollectorSkipsPrivilegedSamplerForCompletePasswordOnlyListing(t *testing.T) {
	for _, rules := range [][]string{{"(ALL) PASSWD: ALL"}, {"(ALL) ALL"}, nil} {
		listing := grantListing("authenticate, listpw=never", rules...)
		t.Run(listing, func(t *testing.T) {
			samples := fixtures(t)
			samples[Sudo] = Sample{Status: Available, Data: []byte(listing)}
			fixed := commands(Config{})
			privileged, listingRead := 0, false
			executor := executorFunc(func(_ context.Context, argv []string, out, _ io.Writer) error {
				for _, command := range fixed {
					if !reflect.DeepEqual(argv, command.argv) {
						continue
					}
					if command.source == Sudo {
						listingRead = true
					}
					_, err := out.Write(samples[command.source].Data)
					return err
				}
				privileged++
				return errors.New("unexpected privileged sampler invocation")
			})
			snapshot := Collect(t.Context(), executor, testResource, Config{})
			if !listingRead || privileged != 0 {
				t.Fatal("password-only listing invoked privileged metrics", listingRead, privileged)
			}
			requireValue(t, snapshot.SudoGrant, "absent")
			requireValue(t, snapshot.CPUCount, 16)
			for _, metric := range []Metric[float64]{snapshot.CPUDieTemperatureCelsius, snapshot.GPUDieTemperatureCelsius, snapshot.FanRPM} {
				if metric.Status != Unavailable || metric.Reason != "sudo_grant_absent" {
					t.Fatal("password-only authority became available privileged metrics", metric)
				}
			}
			for _, warning := range snapshot.Warnings {
				if warning == "sudo_grant_broader" {
					t.Fatal("zero passwordless authority became a broader warning")
				}
			}
		})
	}
}
