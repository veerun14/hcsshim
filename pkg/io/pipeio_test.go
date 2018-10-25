package npipeio

import (
	"bytes"
	"context"
	"testing"
	"time"

	winio "github.com/Microsoft/go-winio"
)

type testAsyncReadWriteResult struct {
	n   int
	err error
}

func testCreateListener(t *testing.T) (string, *NamedPipeDelayedConnection) {
	pipePath := `\\.\pipe\` + t.Name()
	l, err := winio.ListenPipe(pipePath, nil)
	if err != nil {
		t.Fatalf("failed to create listener at path: %s, err: %v", pipePath, err)
	}
	ctx := context.TODO()
	return pipePath, NewNamedPipeDelayedConnection(ctx, l)
}

func testBlockedWrite(t *testing.T, npdc *NamedPipeDelayedConnection) ([]byte, []byte, chan testAsyncReadWriteResult) {
	expected := []byte(t.Name())
	resp := make([]byte, len(expected))

	writeDone := make(chan testAsyncReadWriteResult, 1)
	go func() {
		n, err := npdc.Write(expected)
		writeDone <- testAsyncReadWriteResult{n, err}
	}()

	select {
	case err := <-writeDone:
		t.Fatalf("write should not have completed before connection; err: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	return expected, resp, writeDone
}

func testBlockedRead(t *testing.T, npdc *NamedPipeDelayedConnection) ([]byte, []byte, chan testAsyncReadWriteResult) {
	expected := []byte(t.Name())
	resp := make([]byte, len(expected))

	readDone := make(chan testAsyncReadWriteResult, 1)
	go func() {
		n, err := npdc.Read(resp)
		readDone <- testAsyncReadWriteResult{n, err}
	}()

	select {
	case err := <-readDone:
		t.Fatalf("read should not have completed before connection; err: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	return expected, resp, readDone
}

func testVerifyResults(t *testing.T, expected, actual []byte, op string, opFn func() (int, error)) {
	n, err := opFn()
	if err != nil {
		t.Fatalf("failed to %s bytes with error: %v", op, err)
	}
	if n != len(expected) {
		t.Fatalf("%s bytes expected: '%d' bytes, actual: '%d' bytes", op, len(expected), n)
	}
	if !bytes.Equal(expected, actual) {
		t.Fatalf("%s expected: '%s', actual: '%s'", op, string(expected), string(actual))
	}
}

func testVerifyOpCompletes(t *testing.T, expected []byte, done <-chan testAsyncReadWriteResult, op string) {
	d := <-done
	if d.err != nil {
		t.Fatalf("%s response failed with error: %v", op, d.err)
	}
	if d.n != len(expected) {
		t.Fatalf("%s response failed with invalid length, expected: %d, actual %d", op, len(expected), d.n)
	}
}

func TestNamedPipeDelayedConnectionBlockedWrite(t *testing.T) {
	pipePath, npdc := testCreateListener(t)
	defer npdc.Close()

	expected, resp, writeDone := testBlockedWrite(t, npdc)

	// Connect to unblock the Write
	c, err := winio.DialPipe(pipePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Read the data
	testVerifyResults(t, expected, resp, "read", func() (int, error) { return c.Read(resp) })

	// Verify the Write response
	testVerifyOpCompletes(t, expected, writeDone, "write")
}

func TestNamedPipeDelayedConnectionWriteReconnect(t *testing.T) {
	pipePath, npdc := testCreateListener(t)
	defer npdc.Close()

	// Connect and disconnect to put the listener into an
	// 'unknown' disconnected state. IE: the client is gone
	// but there have been no reads/writes for the listener to
	// notice this yet.
	c, err := winio.DialPipe(pipePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()

	// Issue the Write
	expected, resp, writeDone := testBlockedWrite(t, npdc)

	// Reconnect to unblock the Write
	c, err = winio.DialPipe(pipePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Read the data
	testVerifyResults(t, expected, resp, "read", func() (int, error) { return c.Read(resp) })

	// Verify the Write response
	testVerifyOpCompletes(t, expected, writeDone, "write")
}

func TestNamedPipeDelayedConnectionCloseUnblocksWrite(t *testing.T) {
	_, npdc := testCreateListener(t)

	_, _, writeDone := testBlockedWrite(t, npdc)
	err := npdc.Close()
	if err != nil {
		t.Fatalf("close expected to succeed, got: %v", err)
	}

	d := <-writeDone
	if d.err != winio.ErrFileClosed {
		t.Fatalf("write response expected error: %v, got error: %v", winio.ErrFileClosed, d.err)
	}
	if d.n != 0 {
		t.Fatalf("write response failed with invalid length, expected: 0, actual %d", d.n)
	}
}

func TestNamedPipeDelayedConnectionBlockedRead(t *testing.T) {
	pipePath, npdc := testCreateListener(t)
	defer npdc.Close()

	expected, resp, readDone := testBlockedRead(t, npdc)

	// Connect to unblock the Write
	c, err := winio.DialPipe(pipePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Write the data
	testVerifyResults(t, expected, resp, "write", func() (int, error) { return c.Write(expected) })

	// Verify the Read response
	testVerifyOpCompletes(t, expected, readDone, "read")
}

func TestNamedPipeDelayedConnectionReadReconnect(t *testing.T) {
	pipePath, npdc := testCreateListener(t)
	defer npdc.Close()

	// Connect and disconnect to put the listener into an
	// 'unknown' disconnected state. IE: the client is gone
	// but there have been no reads/writes for the listener to
	// notice this yet.
	c, err := winio.DialPipe(pipePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()

	// Issue the Read
	expected, resp, readDone := testBlockedRead(t, npdc)

	// Reconnect to unblock the Read
	c, err = winio.DialPipe(pipePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Write the data
	testVerifyResults(t, expected, resp, "write", func() (int, error) { return c.Write(expected) })

	// Verify the Read response
	testVerifyOpCompletes(t, expected, readDone, "read")
}

func TestNamedPipeDelayedConnectionCloseUnblocksRead(t *testing.T) {
	_, npdc := testCreateListener(t)

	_, _, readDone := testBlockedRead(t, npdc)
	err := npdc.Close()
	if err != nil {
		t.Fatalf("close expected to succeed, got: %v", err)
	}

	d := <-readDone
	if d.err != winio.ErrFileClosed {
		t.Fatalf("read response expected error: %v, got error: %v", winio.ErrFileClosed, d.err)
	}
	if d.n != 0 {
		t.Fatalf("read response failed with invalid length, expected: 0, actual %d", d.n)
	}
}
