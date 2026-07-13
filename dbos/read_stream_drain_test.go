package dbos

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/stretchr/testify/require"
)

// fakeStreamDB implements only the two systemDatabase methods that readStream
// uses. It embeds the interface so it satisfies the rest (none of which are
// called here) without a pile of stub methods.
type fakeStreamDB struct {
	sysdb.SystemDatabase
	reads int
}

// StreamWakeChannel mirrors the pre-refactor behavior for fakes: no wake
// signal (readStream falls back to its bounded wait).
func (f *fakeStreamDB) StreamWakeChannel(_, _ string) (chan struct{}, func()) {
	return nil, func() {}
}

func (f *fakeStreamDB) ReadStream(_ context.Context, _ sysdb.ReadStreamDBInput) ([]sysdb.StreamEntry, bool, error) {
	f.reads++
	if f.reads == 1 {
		// First read: the producer's final value is not visible yet — it commits
		// in the window between this read and the status check below.
		return nil, false, nil
	}
	// The post-inactive final read drains the value the producer committed just
	// before it completed.
	return []sysdb.StreamEntry{{Value: "final", Offset: 0}}, false, nil
}

func (f *fakeStreamDB) ListWorkflows(_ context.Context, _ sysdb.ListWorkflowsDBInput) ([]WorkflowStatus, error) {
	// The producer is terminal, but it committed "final" after the reader's first
	// stream read above — so its writes are all committed by now.
	return []WorkflowStatus{{Status: WorkflowStatusSuccess}}, nil
}

// A reader must not drop a value the producer commits between the reader's
// stream read and its status check. When the reader observes the producer is
// inactive it must make one more read pass to drain to the end of the stream,
// because all of the producer's writes are committed once it is terminal.
//
// Without that final read, this interleaving — first read empty, producer
// commits "final" and completes, status check sees terminal — drops "final".
func TestReadStreamDrainsValueCommittedBeforeProducerInactive(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	c := &dbosContext{
		ctx:      ctx,
		systemDB: &fakeStreamDB{},
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	values, closed, err := c.ReadStream(c, "wf", "stream")
	require.NoError(t, err)
	require.True(t, closed, "stream should be reported closed once the producer is terminal")
	require.Len(t, values, 1, "reader must drain the value committed just before the producer went inactive")
}
