package npipeio

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Microsoft/go-winio"
)

type testServerSet struct {
	l    *NamedPipeDelayedConnection
	path string
}

type downStreamPipePair struct {
	r *io.PipeReader
	w *io.PipeWriter
}

func (dspp *downStreamPipePair) Close() {
	dspp.w.Close()
	dspp.r.Close()
}

func testCreateListener2(ctx context.Context, stdVal string, t *testing.T) *testServerSet {
	pipePath := fmt.Sprintf(`\\.\pipe\%s-%s`, t.Name(), stdVal)
	l, err := winio.ListenPipe(pipePath, nil)
	if err != nil {
		t.Fatalf("failed to listen pipe: %s, with error: %v", pipePath, err)
	}
	npdc := NewNamedPipeDelayedConnection(ctx, l)
	return &testServerSet{l: npdc, path: pipePath}
}

func testCreatePipePair(t *testing.T) *downStreamPipePair {
	r, w := io.Pipe()
	return &downStreamPipePair{r: r, w: w}
}

func testConnectPipe(t *testing.T, path string) net.Conn {
	c, err := winio.DialPipe(path, nil)
	if err != nil {
		t.Fatalf("failed to dial pipe: %s, with error: %v", path, err)
	}
	return c
}

// TestNamedPipeDelayedConnectionRelayE2E setups a complete E2E test the exact
// way the runtime shim interfaces with upstream clients. The shim acts as the
// Server serving NamedPipe connections and allows a client to
// disconnect/reconnect any number of times without and will simply continue to
// stream the contents. Its not until the `CloseIO` call comes that `Stdin` is
// closed which causes the complete relay to finish.
func TestNamedPipeDelayedConnectionRelayE2E(t *testing.T) {
	serverSet := [3]*testServerSet{}
	downStreamSet := [3]*downStreamPipePair{}

	ctx := context.TODO()

	// Create the Listeners. This represents the shim upstream IO connections.
	serverSet[0] = testCreateListener2(ctx, "stdin", t)
	defer serverSet[0].l.Close()
	serverSet[1] = testCreateListener2(ctx, "stdout", t)
	defer serverSet[1].l.Close()
	serverSet[2] = testCreateListener2(ctx, "stderr", t)
	defer serverSet[2].l.Close()

	tuio := &testUpIO{
		stdin:  serverSet[0].l,
		stdout: serverSet[1].l,
		stderr: serverSet[2].l,
	}

	// Create the Pipe IO. This represents the downstream IO the shim is
	// relaying to/from.
	downStreamSet[0] = testCreatePipePair(t)
	defer downStreamSet[0].Close()
	downStreamSet[1] = testCreatePipePair(t)
	defer downStreamSet[1].Close()
	downStreamSet[2] = testCreatePipePair(t)
	defer downStreamSet[2].Close()

	tdio := &testDownIO{
		stdin:  downStreamSet[0].w,
		stdout: downStreamSet[1].r,
		stderr: downStreamSet[2].r,
	}

	// For simulation purposes just relay all stdin to stdout/stderr
	processDone := make(chan error, 1)
	go func() {
		mr := io.MultiWriter(downStreamSet[1].w, downStreamSet[2].w)
		_, err := io.Copy(mr, downStreamSet[0].r)

		// A real process would close its side of the handles
		downStreamSet[0].r.Close()
		downStreamSet[1].w.Close()
		downStreamSet[2].w.Close()
		processDone <- err
	}()

	// Start the relay
	relay := NewIORelay(ctx, tuio, tdio)
	relayDone := make(chan error, 1)
	go func() {
		relayDone <- relay.Wait()
	}()

	// Connect the client and relay data. We do this 10 times to verify the
	// reconnect paths and that the relay does not stop or lose data.
	for i := 0; i < 10; i++ {
		cIn, cOut, cErr := testConnectPipe(t, serverSet[0].path), testConnectPipe(t, serverSet[1].path), testConnectPipe(t, serverSet[2].path)

		expected := []byte(t.Name())
		actualOut := make([]byte, len(expected))
		actualErr := make([]byte, len(expected))
		outDone := make(chan testAsyncReadWriteResult, 1)
		go func() {
			n, err := cOut.Read(actualOut)
			outDone <- testAsyncReadWriteResult{n: n, err: err}
		}()
		errDone := make(chan testAsyncReadWriteResult, 1)
		go func() {
			n, err := cErr.Read(actualErr)
			errDone <- testAsyncReadWriteResult{n: n, err: err}
		}()
		// Write cIn and verify results.
		n, err := cIn.Write(expected)
		if err != nil {
			t.Errorf("iteration: %d, failed to write stdin with error: %v", i, err)
		}
		if n != len(expected) {
			t.Errorf("iteration: %d, failed to write all expected: %d bytes, actual: %d", i, len(expected), n)
		}
		result := <-outDone
		if result.err != nil {
			t.Errorf("iteration: %d, failed to read stdout with error: %v", i, result.err)
		}
		if result.n != len(expected) {
			t.Errorf("iteration: %d, failed to read all stdout expected: %d bytes, actual: %d", i, len(expected), n)
		}
		result = <-errDone
		if result.err != nil {
			t.Errorf("iteration: %d, failed to read stderr with error: %v", i, result.err)
		}
		if result.n != len(expected) {
			t.Errorf("iteration: %d, failed to read all stderr expected: %d bytes, actual: %d", i, len(expected), n)
		}
		err = cIn.Close()
		if err != nil {
			t.Logf("iteration: %d, failed to close stdin with error: %v", i, err)
		}
		err = cOut.Close()
		if err != nil {
			t.Logf("iteration: %d, failed to close stdout with error: %v", i, err)
		}
		err = cErr.Close()
		if err != nil {
			t.Logf("iteration: %d, failed to close stderr with error: %v", i, err)
		}

		// Verify the relay's stay alive
		select {
		case err = <-processDone:
			t.Fatalf("iteration: %d, process realy closed with error: %v", i, err)
		case err = <-relayDone:
			t.Fatalf("iteration: %d, io realy closed with error: %v", i, err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Verify that the relay dies after simulating the `CloseIO` on stdin
	serverSet[0].l.Close()
	relay.WaitStdin()
	downStreamSet[0].w.Close()

	err := <-processDone
	if err != nil {
		t.Fatalf("process realy closed with error: %v", err)
	}

	err = <-relayDone
	if err != nil {
		t.Fatalf("io realy closed with error: %v", err)
	}
}
