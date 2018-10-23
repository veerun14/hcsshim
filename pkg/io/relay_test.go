package npipeio

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

var _ = (io.ReadWriteCloser)(&bufCloser{})

type bufCloser struct {
	b bytes.Buffer
}

func (bc *bufCloser) Read(p []byte) (int, error) {
	return bc.b.Read(p)
}

func (bc *bufCloser) Write(p []byte) (int, error) {
	return bc.b.Write(p)
}

func (bc *bufCloser) Close() error {
	return nil
}

var _ = (io.ReadWriteCloser)(&blockCloser{})

type blockCloser struct {
	wg sync.WaitGroup

	b bytes.Buffer
}

func newBlockCloser() *blockCloser {
	bc := &blockCloser{}
	bc.wg.Add(1)
	return bc
}

func (bc *blockCloser) Read(p []byte) (int, error) {
	bc.wg.Wait()
	return bc.b.Read(p)
}

func (bc *blockCloser) Write(p []byte) (int, error) {
	bc.wg.Wait()
	return bc.b.Write(p)
}

func (bc *blockCloser) Close() error {
	return nil
}

var _ = (UpstreamIO)(&testUpIO{})

type testUpIO struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
}

func (tuio *testUpIO) Stdin() io.Reader {
	return tuio.stdin
}

func (tuio *testUpIO) Stdout() io.Writer {
	return tuio.stdout
}

func (tuio *testUpIO) Stderr() io.Writer {
	return tuio.stderr
}

var _ = (DownstreamIO)(&testDownIO{})

type testDownIO struct {
	stdin  io.Writer
	stdout io.Reader
	stderr io.Reader
}

func (tdio *testDownIO) Stdin() io.Writer {
	return tdio.stdin
}

func (tdio *testDownIO) Stdout() io.Reader {
	return tdio.stdout
}

func (tdio *testDownIO) Stderr() io.Reader {
	return tdio.stderr
}

func TestIORelaySuccess(t *testing.T) {
	inUp, outUp, errUp := new(bytes.Buffer), new(bytes.Buffer), new(bytes.Buffer)
	up := &testUpIO{
		stdin:  inUp,
		stdout: outUp,
		stderr: errUp,
	}
	inDown, outDown, errDown := new(bytes.Buffer), new(bytes.Buffer), new(bytes.Buffer)
	down := &testDownIO{
		stdin:  inDown,
		stdout: outDown,
		stderr: errDown,
	}
	ctx := context.TODO()

	inUp.Write([]byte(t.Name()))
	outDown.Write([]byte(t.Name()))
	errDown.Write([]byte(t.Name()))
	r := NewIORelay(ctx, up, down)
	err := r.Wait()
	if err != nil {
		t.Fatalf("failed to relay with error: %v", err)
	}
	val := inDown.String()
	if val != t.Name() {
		t.Fatalf("stdin expected: '%s', got: '%s'", t.Name(), val)
	}
	val = outUp.String()
	if val != t.Name() {
		t.Fatalf("stdout expected: '%s', got: '%s'", t.Name(), val)
	}
	val = errUp.String()
	if val != t.Name() {
		t.Fatalf("stderr expected: '%s', got: '%s'", t.Name(), val)
	}
	// Verify we can call Wait multiple times once done.
	err = r.Wait()
	if err != nil {
		t.Fatalf("failed to relay with error: %v", err)
	}
}

func TestIORelayWaitBeforeComplete(t *testing.T) {
	inUp, outUp, errUp := newBlockCloser(), new(bytes.Buffer), new(bytes.Buffer)
	up := &testUpIO{
		stdin:  inUp,
		stdout: outUp,
		stderr: errUp,
	}
	inDown, outDown, errDown := new(bytes.Buffer), newBlockCloser(), newBlockCloser()
	down := &testDownIO{
		stdin:  inDown,
		stdout: outDown,
		stderr: errDown,
	}
	ctx := context.TODO()

	inUp.b.Write([]byte(t.Name()))
	outDown.b.Write([]byte(t.Name()))
	errDown.b.Write([]byte(t.Name()))
	r := NewIORelay(ctx, up, down)

	done := make(chan error, 1)
	go func() {
		done <- r.Wait()
	}()

	select {
	case err := <-done:
		t.Fatalf("done finished before block closer with error: '%v'", err)
	case <-time.After(10 * time.Millisecond):
		inUp.wg.Done()
		outDown.wg.Done()
		errDown.wg.Done()
	}

	err := <-done
	if err != nil {
		t.Fatalf("failed to relay with error: %v", err)
	}
	val := inDown.String()
	if val != t.Name() {
		t.Fatalf("stdin expected: '%s', got: '%s'", t.Name(), val)
	}
	val = outUp.String()
	if val != t.Name() {
		t.Fatalf("stdout expected: '%s', got: '%s'", t.Name(), val)
	}
	val = errUp.String()
	if val != t.Name() {
		t.Fatalf("stderr expected: '%s', got: '%s'", t.Name(), val)
	}
}
