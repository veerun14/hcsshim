package npipeio

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// UpstreamIO defines an interface for all IO handles that should be relayed:
//
// stdin  ->
// stdout <-
// stderr <-
type UpstreamIO interface {
	Stdin() io.Reader
	Stdout() io.Writer
	Stderr() io.Writer
}

// DownstreamIO defines an interface for all IO handles that should be relayed:
//
// -> stdin
// <- stdout
// <- stderr
type DownstreamIO interface {
	Stdin() io.Writer
	Stdout() io.Reader
	Stderr() io.Reader
}

// IORelay relays data from `u` to `d` in a single group.
type IORelay struct {
	ctx context.Context

	u UpstreamIO
	d DownstreamIO

	g *errgroup.Group
}

// NewIORelay creates an `IORelay` and begins relaying data from `upstream` to
// `downstream`.
//
// Note: This relay specifically handles the special case when a call to `Close`
// happens on `upstream.Stdin()` to stop the relay as a non-error.
func NewIORelay(ctx context.Context, upstream UpstreamIO, downstream DownstreamIO) *IORelay {
	ir := &IORelay{
		u: upstream,
		d: downstream,
	}
	ir.g, ir.ctx = errgroup.WithContext(ctx)

	if upstream.Stdin() != nil {
		ir.g.Go(func() error {
			if _, err := io.Copy(downstream.Stdin(), upstream.Stdin()); err != nil {
				return errors.Wrap(err, "error relaying stdin")
			}
			return nil
		})
	}
	if upstream.Stdout() != nil {
		ir.g.Go(func() error {
			if _, err := io.Copy(upstream.Stdout(), downstream.Stdout()); err != nil {
				return errors.Wrap(err, "error relaying stdout")
			}
			return nil
		})
	}
	if upstream.Stderr() != nil {
		ir.g.Go(func() error {
			if _, err := io.Copy(upstream.Stderr(), downstream.Stderr()); err != nil {
				return errors.Wrap(err, "error relaying stderr")
			}
			return nil
		})
	}
	return ir
}

// Wait waits for all active relays to finish before returning any errors
// associated with the relay operation.
func (ir *IORelay) Wait() error {
	return ir.g.Wait()
}
