package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
)

// trackPass drives the seeded pass into Tracking with an active recording,
// returning the pass ID, attempt ID and active segment.
func trackPass(t *testing.T, runtime *Runtime) (string, string, recorder.Segment) {
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
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 3},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 3},
	}
	if _, err := runtime.Passes.AcceptDiversity(value.ID, results); err != nil {
		t.Fatal(err)
	}
	segment, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found {
		t.Fatal("tracking pass has no active recording")
	}
	return value.ID, attempt.ID, segment
}

func bufferFrame(t *testing.T, runtime *Runtime, segmentID string, sequence uint64) {
	t.Helper()
	if err := runtime.Recorder.Buffer(segmentID, recorder.Frame{Sequence: sequence, Epoch: 3, Payload: []byte("frame-payload")}); err != nil {
		t.Fatal(err)
	}
}

func TestHandoverPrepareConfirmCommit(t *testing.T) {
	runtime := newTestRuntime(t)
	passID, attemptID, segment := trackPass(t, runtime)
	bufferFrame(t, runtime, segment.ID, 1)

	session, err := runtime.Handovers.Prepare(passID, "south")
	if err != nil {
		t.Fatal(err)
	}
	if session.State != "prepared" || session.AttemptID != attemptID || session.Epoch != 3 || session.BufferedFrames != 1 {
		t.Fatalf("unexpected prepared session: %#v", session)
	}

	// Old writes are frozen: neither buffering nor draining is allowed.
	if err := runtime.Recorder.Buffer(segment.ID, recorder.Frame{Sequence: 2, Epoch: 3, Payload: []byte("blocked")}); err == nil {
		t.Fatal("buffering unexpectedly succeeded while frozen")
	}
	if _, err := runtime.Recorder.Drain(context.Background(), segment.ID); err == nil {
		t.Fatal("drain unexpectedly succeeded while frozen")
	}

	// Commit before the destination confirms is rejected.
	if _, err := runtime.Handovers.Commit(session.ID); err == nil {
		t.Fatal("commit unexpectedly succeeded before confirm")
	}
	// Only the destination station may confirm.
	if _, err := runtime.Handovers.Confirm(session.ID, "north"); err == nil {
		t.Fatal("confirm by source station unexpectedly succeeded")
	}
	if _, err := runtime.Handovers.Confirm(session.ID, "south"); err != nil {
		t.Fatal(err)
	}

	committed, err := runtime.Handovers.Commit(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != "committed" || committed.DestinationGeneration != session.SourceGeneration+1 {
		t.Fatalf("unexpected committed session: %#v", committed)
	}
	value, _ := runtime.Passes.Get(passID)
	if value.OwnerStation != "south" || value.OwnerGeneration != session.SourceGeneration+1 {
		t.Fatalf("pass ownership did not advance: %#v", value)
	}

	// The new owner can write again, and the frame buffered before the
	// handover is still there.
	bufferFrame(t, runtime, segment.ID, 2)
	drained, err := runtime.Recorder.Drain(context.Background(), segment.ID)
	if err != nil || drained != 2 {
		t.Fatalf("drained %d frames after commit: %v", drained, err)
	}
}

func TestHandoverAbortKeepsSourceOwnershipAndFrames(t *testing.T) {
	runtime := newTestRuntime(t)
	passID, _, segment := trackPass(t, runtime)
	bufferFrame(t, runtime, segment.ID, 1)
	bufferFrame(t, runtime, segment.ID, 2)

	session, err := runtime.Handovers.Prepare(passID, "south")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Handovers.Confirm(session.ID, "south"); err != nil {
		t.Fatal(err)
	}
	aborted, err := runtime.Handovers.Abort(session.ID, "destination link degraded")
	if err != nil {
		t.Fatal(err)
	}
	if aborted.State != "aborted" || aborted.AbortReason == "" {
		t.Fatalf("unexpected aborted session: %#v", aborted)
	}

	// Source keeps ownership and the unwritten frames survive the rollback.
	value, _ := runtime.Passes.Get(passID)
	if value.OwnerStation != "north" || value.OwnerGeneration != session.SourceGeneration {
		t.Fatalf("source ownership changed on abort: %#v", value)
	}
	current, found := runtime.Recorder.ActiveForPass(passID)
	if !found || current.FramesBuffered != 2 {
		t.Fatalf("buffered frames lost on abort: %#v", current)
	}

	// Writes resume for the source owner.
	bufferFrame(t, runtime, segment.ID, 3)
	drained, err := runtime.Recorder.Drain(context.Background(), segment.ID)
	if err != nil || drained != 3 {
		t.Fatalf("drained %d frames after abort: %v", drained, err)
	}

	// A terminal session cannot transition again.
	if _, err := runtime.Handovers.Abort(session.ID, "again"); err == nil {
		t.Fatal("abort of terminal session unexpectedly succeeded")
	}
}

func TestHandoverCommitRejectsMovedAttempt(t *testing.T) {
	runtime := newTestRuntime(t)
	passID, _, _ := trackPass(t, runtime)

	session, err := runtime.Handovers.Prepare(passID, "south")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Handovers.Confirm(session.ID, "south"); err != nil {
		t.Fatal(err)
	}

	// The decoder epoch moves after the freeze (a re-sync on the same
	// attempt): the commit must refuse to hand over against a stale
	// attempt/epoch snapshot.
	if err := runtime.Attempts.Deliver(receiver.AcquisitionResult{PassID: passID, AttemptID: session.AttemptID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 9}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Handovers.Commit(session.ID); err == nil {
		t.Fatal("commit unexpectedly succeeded after the receive attempt moved")
	}

	// The handover stays resumable: abort restores the source cleanly.
	if _, err := runtime.Handovers.Abort(session.ID, "attempt moved"); err != nil {
		t.Fatal(err)
	}
	value, _ := runtime.Passes.Get(passID)
	if value.OwnerStation != "north" {
		t.Fatalf("source ownership changed: %#v", value)
	}
}

func TestHandoverSurvivesRestart(t *testing.T) {
	runtime := newTestRuntime(t)
	passID, attemptID, segment := trackPass(t, runtime)
	bufferFrame(t, runtime, segment.ID, 1)

	session, err := runtime.Handovers.Prepare(passID, "south")
	if err != nil {
		t.Fatal(err)
	}

	// Rebuild the whole runtime from the same state root: the prepared
	// handover and the frozen segment must still be there.
	restarted, err := NewRuntime(runtime.StateRoot)
	if err != nil {
		t.Fatal(err)
	}
	restored, found := restarted.Handovers.Get(session.ID)
	if !found || restored.State != "prepared" || restored.AttemptID != attemptID || restored.Epoch != 3 {
		t.Fatalf("handover session did not survive restart: %#v", restored)
	}
	if err := restarted.Recorder.Buffer(segment.ID, recorder.Frame{Sequence: 2, Epoch: 3, Payload: []byte("blocked")}); err == nil {
		t.Fatal("buffering unexpectedly succeeded while frozen after restart")
	}

	if _, err := restarted.Handovers.Confirm(session.ID, "south"); err != nil {
		t.Fatal(err)
	}
	committed, err := restarted.Handovers.Commit(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != "committed" {
		t.Fatalf("unexpected state after restart commit: %#v", committed)
	}
	value, _ := restarted.Passes.Get(passID)
	if value.OwnerStation != "south" {
		t.Fatalf("ownership did not transfer after restart: %#v", value)
	}
}

func TestHandoverEndpoints(t *testing.T) {
	runtime := newTestRuntime(t)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	passID, _, _ := trackPass(t, runtime)

	server := httptest.NewServer(router)
	defer server.Close()
	base := server.URL + "/api/passes/" + passID + "/handover"

	response := requestJSON(t, server.Client(), http.MethodPost, base+"/prepare", `{"destination":"south"}`)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("prepare returned %d", response.StatusCode)
	}
	var session struct {
		ID    string `json:"id"`
		State string `json:"state"`
	}
	decodeBody(t, response, &session)
	if session.ID == "" || session.State != "prepared" {
		t.Fatalf("unexpected prepare response: %#v", session)
	}

	response = requestJSON(t, server.Client(), http.MethodPost, base+"/"+session.ID+"/confirm", `{"station":"south"}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("confirm returned %d", response.StatusCode)
	}
	_ = response.Body.Close()

	response = requestJSON(t, server.Client(), http.MethodPost, base+"/"+session.ID+"/commit", `{}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("commit returned %d", response.StatusCode)
	}
	_ = response.Body.Close()

	response, err = server.Client().Get(base)
	if err != nil {
		t.Fatal(err)
	}
	var listed struct {
		Items []struct {
			ID    string `json:"id"`
			State string `json:"state"`
		} `json:"items"`
	}
	decodeBody(t, response, &listed)
	if len(listed.Items) != 1 || listed.Items[0].State != "committed" {
		t.Fatalf("unexpected handover list: %#v", listed.Items)
	}
}
