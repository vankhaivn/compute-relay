package runtimehost

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestLoopbackListenIsExplicitAndCanonical(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "127.0.0.2:7331", "[::1]:7331"} {
		if !ValidListen(address) {
			t.Fatal("valid loopback rejected")
		}
	}
	for _, address := range []string{"localhost:7331", "0.0.0.0:7331", "[::]:7331", "192.168.1.2:7331", "127.0.0.1:01", "127.0.0.1:+1", "127.0.0.1:65536", "[::1%zone]:7331", "[::ffff:127.0.0.1]:7331", "127.0.0.1"} {
		if ValidListen(address) {
			t.Fatal("unsafe listener", address)
		}
	}
}
func testListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	return listener
}
func boundedClient() *http.Client {
	return &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, Timeout: 5 * time.Second}
}
func TestHTTPShutdownJoinsActiveHandlerBeforeReturning(t *testing.T) {
	listener := testListener(t)
	active, release := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(active); <-release; w.Write([]byte("finished")) })}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, done := make(chan struct{}), make(chan error, 1)
	go func() {
		done <- serveHTTP(ctx, server, listener, time.Second, func() error { close(ready); return nil })
	}()
	<-ready
	response := make(chan error, 1)
	go func() {
		r, e := boundedClient().Get("http://" + listener.Addr().String())
		if e == nil {
			defer r.Body.Close()
			_, e = io.ReadAll(r.Body)
		}
		response <- e
	}()
	<-active
	cancel()
	select {
	case err := <-done:
		t.Fatal("released ownership with active handler", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-response; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown failed to join")
	}
}
func TestHTTPForcedCloseCancelsThenJoinsHandler(t *testing.T) {
	listener := testListener(t)
	active, exited := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(active); <-r.Context().Done(); close(exited) })}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready, done := make(chan struct{}), make(chan error, 1)
	go func() {
		done <- serveHTTP(ctx, server, listener, 50*time.Millisecond, func() error { close(ready); return nil })
	}()
	<-ready
	response := make(chan struct{})
	go func() {
		r, _ := boundedClient().Get("http://" + listener.Addr().String())
		if r != nil {
			r.Body.Close()
		}
		close(response)
	}()
	<-active
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrState) {
			t.Fatal("forced failure not reported", err)
		}
		select {
		case <-exited:
		default:
			t.Fatal("handler not joined")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("forced close did not join")
	}
	<-response
}
func TestHTTPAnnouncementFailureClosesListener(t *testing.T) {
	listener := testListener(t)
	address := listener.Addr().String()
	server := &http.Server{Handler: http.NotFoundHandler()}
	err := serveHTTP(context.Background(), server, listener, time.Second, func() error { return errors.New("SYNTHETIC_PRIVATE_OUTPUT") })
	if !errors.Is(err, ErrState) {
		t.Fatal("unacknowledged startup succeeded", err)
	}
	c, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
	if err == nil {
		c.Close()
		t.Fatal("listener leaked after announcement failure")
	}
}
