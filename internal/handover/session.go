package handover

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orbitlink/internal/audit"
	"orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/recorder"
	"orbitlink/internal/store"
)

// State is the lifecycle of a recording handover. A session is resumable
// while it sits in Prepared or Confirmed; Committed and Aborted are terminal.
type State string

const (
	Prepared  State = "prepared"
	Confirmed State = "confirmed"
	Committed State = "committed"
	Aborted   State = "aborted"
)

func (s State) terminal() bool {
	return s == Committed || s == Aborted
}

// Session ties the receive attempt, decoder epoch and segment ownership of a
// pass to the source and destination stations of one recording handover.
type Session struct {
	ID                    string    `json:"id"`
	PassID                string    `json:"pass_id"`
	SegmentID             string    `json:"segment_id"`
	Source                string    `json:"source"`
	Destination           string    `json:"destination"`
	SourceGeneration      int64     `json:"source_generation"`
	DestinationGeneration int64     `json:"destination_generation,omitempty"`
	AttemptID             string    `json:"attempt_id"`
	Epoch                 int64     `json:"epoch"`
	BufferedFrames        int       `json:"buffered_frames"`
	State                 State     `json:"state"`
	AbortReason           string    `json:"abort_reason,omitempty"`
	PreparedAt            time.Time `json:"prepared_at"`
	ConfirmedAt           time.Time `json:"confirmed_at,omitempty"`
	CommittedAt           time.Time `json:"committed_at,omitempty"`
	AbortedAt             time.Time `json:"aborted_at,omitempty"`
}

type Coordinator struct {
	mu         sync.Mutex
	path       string
	sessions   map[string]Session
	passes     *pass.Service
	recordings *pass.RecordingState
	recorder   *recorder.Manager
	attempts   *receiver.AttemptDispatcher
	audit      *audit.Trail
}

func NewCoordinator(paths store.Paths, passes *pass.Service, recordings *pass.RecordingState, manager *recorder.Manager, attempts *receiver.AttemptDispatcher, trail *audit.Trail) (*Coordinator, error) {
	coordinator := &Coordinator{
		path: paths.Snapshot("recording-handovers"), sessions: make(map[string]Session),
		passes: passes, recordings: recordings, recorder: manager, attempts: attempts, audit: trail,
	}
	var sessions []Session
	found, err := store.ReadJSON(coordinator.path, &sessions)
	if err != nil {
		return nil, err
	}
	if found {
		for _, session := range sessions {
			coordinator.sessions[session.ID] = session
		}
	}
	return coordinator, nil
}

// Prepare freezes the source owner's writes and snapshots the current receive
// attempt, decoder epoch and buffered frame count so the handover can resume
// or roll back from a known point.
func (c *Coordinator) Prepare(passID, destination string) (Session, error) {
	if destination == "" {
		return Session{}, fmt.Errorf("handover destination station is required")
	}
	value, found := c.passes.Get(passID)
	if !found {
		return Session{}, fmt.Errorf("pass %s not found", passID)
	}
	if value.Phase != pass.Tracking {
		return Session{}, fmt.Errorf("pass cannot hand over recording from phase %s", value.Phase)
	}
	if value.RecordingSegmentID == "" {
		return Session{}, fmt.Errorf("pass %s has no active recording", passID)
	}
	if destination == value.OwnerStation {
		return Session{}, fmt.Errorf("handover destination must differ from current owner %s", value.OwnerStation)
	}
	attempt, found := c.attempts.Current(passID)
	if !found {
		return Session{}, fmt.Errorf("pass %s has no current receive attempt", passID)
	}
	segment, found := c.recorder.ActiveForPass(passID)
	if !found || segment.ID != value.RecordingSegmentID {
		return Session{}, fmt.Errorf("pass %s has no active recording", passID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, session := range c.sessions {
		if session.SegmentID == segment.ID && !session.State.terminal() {
			return Session{}, fmt.Errorf("recording segment %s already has handover %s in %s", segment.ID, session.ID, session.State)
		}
	}
	frozen, err := c.recorder.Freeze(segment.ID, value.OwnerStation, value.OwnerGeneration)
	if err != nil {
		return Session{}, err
	}
	session := Session{
		ID: uuid.NewString(), PassID: passID, SegmentID: segment.ID,
		Source: value.OwnerStation, Destination: destination,
		SourceGeneration: value.OwnerGeneration,
		AttemptID:        attempt.ID, Epoch: value.DecoderEpoch, BufferedFrames: frozen.FramesBuffered,
		State: Prepared, PreparedAt: time.Now().UTC(),
	}
	c.sessions[session.ID] = session
	if err := c.flushLocked(); err != nil {
		delete(c.sessions, session.ID)
		_, _ = c.recorder.Unfreeze(segment.ID, value.OwnerStation, value.OwnerGeneration)
		return Session{}, err
	}
	_, _ = c.audit.Record(audit.Event{Component: "handover", Action: "prepared", Subject: session.ID, Fields: map[string]any{"pass_id": passID, "source": session.Source, "destination": destination, "attempt_id": session.AttemptID, "epoch": session.Epoch, "buffered_frames": session.BufferedFrames}})
	return session, nil
}

// Confirm records the destination station's acknowledgement. Only the
// destination of a prepared session may confirm.
func (c *Coordinator) Confirm(sessionID, station string) (Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, found := c.sessions[sessionID]
	if !found {
		return Session{}, fmt.Errorf("handover session %s not found", sessionID)
	}
	if session.State != Prepared {
		return Session{}, fmt.Errorf("handover session %s cannot confirm from %s", sessionID, session.State)
	}
	if station == "" || station != session.Destination {
		return Session{}, fmt.Errorf("handover must be confirmed by destination station %s", session.Destination)
	}
	session.State = Confirmed
	session.ConfirmedAt = time.Now().UTC()
	c.sessions[sessionID] = session
	if err := c.flushLocked(); err != nil {
		return Session{}, err
	}
	_, _ = c.audit.Record(audit.Event{Component: "handover", Action: "confirmed", Subject: session.ID, Fields: map[string]any{"pass_id": session.PassID, "station": station}})
	return session, nil
}

// Commit hands ownership to the confirmed destination. The receive attempt
// and decoder epoch must not have moved since the freeze; if they have, the
// commit is rejected and the caller should abort instead.
func (c *Coordinator) Commit(sessionID string) (Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, found := c.sessions[sessionID]
	if !found {
		return Session{}, fmt.Errorf("handover session %s not found", sessionID)
	}
	if session.State != Confirmed {
		return Session{}, fmt.Errorf("handover session %s cannot commit from %s", sessionID, session.State)
	}
	value, found := c.passes.Get(session.PassID)
	if !found {
		return Session{}, fmt.Errorf("pass %s not found", session.PassID)
	}
	if value.CurrentAttemptID != session.AttemptID || value.DecoderEpoch != session.Epoch {
		return Session{}, fmt.Errorf("receive attempt or decoder epoch changed since freeze; abort handover %s", sessionID)
	}
	segment, err := c.recordings.TransferOwner(session.PassID, session.SegmentID, session.Source, session.Destination, session.SourceGeneration)
	if err != nil {
		return Session{}, err
	}
	session.State = Committed
	session.DestinationGeneration = segment.OwnerGeneration
	session.CommittedAt = time.Now().UTC()
	c.sessions[sessionID] = session
	if err := c.flushLocked(); err != nil {
		return Session{}, err
	}
	_, _ = c.audit.Record(audit.Event{Component: "handover", Action: "committed", Subject: session.ID, Fields: map[string]any{"pass_id": session.PassID, "destination": session.Destination, "destination_generation": session.DestinationGeneration, "epoch": session.Epoch}})
	return session, nil
}

// Abort rolls a prepared or confirmed handover back: the source station keeps
// ownership and every frame that was still buffered when the freeze began.
func (c *Coordinator) Abort(sessionID, reason string) (Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, found := c.sessions[sessionID]
	if !found {
		return Session{}, fmt.Errorf("handover session %s not found", sessionID)
	}
	if session.State != Prepared && session.State != Confirmed {
		return Session{}, fmt.Errorf("handover session %s cannot abort from %s", sessionID, session.State)
	}
	if _, err := c.recorder.Unfreeze(session.SegmentID, session.Source, session.SourceGeneration); err != nil {
		return Session{}, err
	}
	session.State = Aborted
	session.AbortReason = reason
	session.AbortedAt = time.Now().UTC()
	c.sessions[sessionID] = session
	if err := c.flushLocked(); err != nil {
		return Session{}, err
	}
	_, _ = c.audit.Record(audit.Event{Component: "handover", Action: "aborted", Subject: session.ID, Severity: audit.Warning, Fields: map[string]any{"pass_id": session.PassID, "reason": reason, "owner": session.Source, "buffered_frames": session.BufferedFrames}})
	return session, nil
}

func (c *Coordinator) flushLocked() error {
	values := make([]Session, 0, len(c.sessions))
	for _, session := range c.sessions {
		values = append(values, session)
	}
	return store.WriteJSON(c.path, values)
}
