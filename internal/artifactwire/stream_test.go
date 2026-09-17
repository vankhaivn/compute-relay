package artifactwire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

type finalReader struct {
	payload []byte
	err     error
}
func (r *finalReader) Read(p []byte) (int, error) {
	if len(r.payload) == 0 {
		return 0, r.err
	}
	n := copy(p, r.payload)
	r.payload = r.payload[n:]
	if len(r.payload) == 0 {
		return n, r.err
	}
	return n, nil
}

type readerFunc func([]byte) (int, error)
func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
type writerFunc func([]byte) (int, error)
func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestCopyRetainsFinalReadErrorAndExactEOF(t *testing.T) {
	payload := []byte("verified")
	h := sha256.Sum256(payload)
	file := File{"art_one", "outputs/answer", "output", int64(len(payload)), hex.EncodeToString(h[:])}
	for _, test := range []struct {
		name string
		r    io.Reader
		ok   bool
	}{
		{"ordinary", bytes.NewReader(payload), true},
		{"EOF-with-bytes", &finalReader{append([]byte{}, payload...), io.EOF}, true},
		{"error-with-final-bytes", &finalReader{append([]byte{}, payload...), errors.New("private cause")}, false},
		{"short", bytes.NewReader(payload[:3]), false},
		{"extra", bytes.NewReader(append(append([]byte{}, payload...), 'x')), false},
		{"wrong", bytes.NewReader([]byte("wrongxxx")), false},
		{"zero-progress", readerFunc(func([]byte) (int, error) { return 0, nil }), false},
		{"invalid-count", readerFunc(func(p []byte) (int, error) { return len(p) + 1, nil }), false},
		{"panic", readerFunc(func([]byte) (int, error) { panic("private cause") }), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var dst bytes.Buffer
			err := Copy(context.Background(), &dst, test.r, file)
			if (err == nil) != test.ok || int64(dst.Len()) > file.Bytes {
				t.Fatal("unqualified stream", err, dst.Len())
			}
		})
	}
	for _, dst := range []io.Writer{
		writerFunc(func(p []byte) (int, error) { return len(p) - 1, nil }),
		writerFunc(func(p []byte) (int, error) { return len(p), io.ErrClosedPipe }),
		writerFunc(func(p []byte) (int, error) { panic("private cause") }),
	} {
		if Copy(context.Background(), dst, bytes.NewReader(payload), file) == nil {
			t.Fatal("failed destination qualified")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Copy(ctx, io.Discard, bytes.NewReader(payload), file) == nil {
		t.Fatal("cancelled copy qualified")
	}
	empty := sha256.Sum256(nil)
	file.Bytes, file.SHA256 = 0, hex.EncodeToString(empty[:])
	if err := Copy(context.Background(), io.Discard, bytes.NewReader(nil), file); err != nil {
		t.Fatal("explicitly empty artifact rejected", err)
	}
}
