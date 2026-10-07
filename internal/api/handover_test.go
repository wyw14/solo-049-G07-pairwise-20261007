package api

import (
	"context"
	"testing"

	"orbitlink/internal/handover"
	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
)

func startTracking(t *testing.T, runtime *Runtime) (passsvc.Pass, receiver.Attempt, recorder.Segment) {
	t.Helper()
	value := runtime.Passes.List()[0]
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	decision, err := runtime.Passes.AcceptDiversity(value.ID, results)
	if err != nil || !decision.Ready {
		t.Fatalf("diversity did not converge: %#v %v", decision, err)
	}
	segment, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("tracking did not start a recorder")
	}
	value, _ = runtime.Passes.Get(value.ID)
	return value, attempt, segment
}

func bufferFrames(t *testing.T, runtime *Runtime, passID, attemptID, segmentID string, symbols int) {
	t.Helper()
	session, err := runtime.NewDecoderSession(passID, segmentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.OnLock(attemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Feed(attemptID, make([]byte, symbols)); err != nil {
		t.Fatal(err)
	}
}

func TestHandoverFreezeConfirmAbortAndRecovery(t *testing.T) {
	root := t.TempDir()
	runtime, err := NewRuntime(root)
	if err != nil {
		t.Fatal(err)
	}
	value, attempt, segment := startTracking(t, runtime)
	bufferFrames(t, runtime, value.ID, attempt.ID, segment.ID, 128) // two 64-byte frames

	// Prepare freezes source writes and pins attempt + decoder epoch.
	prepared, err := runtime.Handovers.Prepare(value.ID, "south")
	if err != nil {
		t.Fatal(err)
	}
	if prepared.State != handover.Prepared || prepared.AttemptID != attempt.ID || prepared.DecoderEpoch != 7 {
		t.Fatalf("handover is not pinned to the receive chain: %#v", prepared)
	}
	if prepared.FrozenFrames != 2 {
		t.Fatalf("expected 2 frozen frames, got %d", prepared.FrozenFrames)
	}
	if err := runtime.Recorder.Buffer(segment.ID, recorder.Frame{Sequence: 99, Payload: []byte("late")}); err == nil {
		t.Fatal("frozen segment accepted new frames")
	}
	if _, err := runtime.Recorder.Drain(context.Background(), segment.ID); err == nil {
		t.Fatal("frozen segment drained frames")
	}
	if _, err := runtime.Handovers.Prepare(value.ID, "east"); err == nil {
		t.Fatal("overlapping handover unexpectedly prepared")
	}

	// Only the destination station may confirm.
	if _, err := runtime.Handovers.Confirm(prepared.ID, "east"); err == nil {
		t.Fatal("foreign station confirmed the handover")
	}

	// Abort keeps source ownership and every unwritten frame.
	aborted, err := runtime.Handovers.Abort(prepared.ID, "destination unreachable")
	if err != nil {
		t.Fatal(err)
	}
	if aborted.State != handover.Aborted {
		t.Fatalf("handover did not abort: %#v", aborted)
	}
	current, _ := runtime.Recorder.ActiveForPass(value.ID)
	if current.OwnerStation != "north" || current.OwnerGeneration != 1 || current.Status != recorder.Open || current.FramesBuffered != 2 {
		t.Fatalf("abort did not restore source ownership and frames: %#v", current)
	}

	// A fresh handover confirms and the destination takes over.
	prepared, err = runtime.Handovers.Prepare(value.ID, "south")
	if err != nil {
		t.Fatal(err)
	}
	transferred, err := runtime.Handovers.Confirm(prepared.ID, "south")
	if err != nil {
		t.Fatal(err)
	}
	if err := transferred.ValidateTransferred(); err != nil {
		t.Fatal(err)
	}
	current, _ = runtime.Recorder.ActiveForPass(value.ID)
	if current.OwnerStation != "south" || current.OwnerGeneration != 2 || current.Status != recorder.Open || current.FramesBuffered != 2 {
		t.Fatalf("destination did not take over the frozen frames: %#v", current)
	}
	updated, _ := runtime.Passes.Get(value.ID)
	if updated.OwnerStation != "south" || updated.OwnerGeneration != 2 {
		t.Fatalf("pass ownership did not follow the handover: %#v", updated)
	}
	written, err := runtime.Recorder.Drain(context.Background(), segment.ID)
	if err != nil || written != 2 {
		t.Fatalf("destination could not drain preserved frames: %d %v", written, err)
	}
	if err := runtime.Handovers.Cleanup(prepared.ID); err != nil {
		t.Fatal(err)
	}

	// Sessions survive a restart so an interrupted flow stays recoverable.
	recovered, err := NewRuntime(root)
	if err != nil {
		t.Fatal(err)
	}
	sessions := recovered.Handovers.List(value.ID)
	states := map[handover.State]int{}
	for _, session := range sessions {
		states[session.State]++
	}
	if len(sessions) != 2 || states[handover.Aborted] != 1 || states[handover.Transferred] != 1 {
		t.Fatalf("handover sessions did not recover: %#v", sessions)
	}
}
