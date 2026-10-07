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

// State is the recoverable stage of a recording handover.
type State string

const (
	// Prepared: source writes are frozen, ownership has not moved yet.
	Prepared State = "prepared"
	// Confirmed: destination acknowledged and ownership moved; writer resume is pending.
	Confirmed State = "confirmed"
	// Transferred: destination owns the segment and writes have resumed.
	Transferred State = "transferred"
	// Aborted: handover failed; source keeps ownership and all unwritten frames.
	Aborted State = "aborted"
)

type Session struct {
	ID                    string    `json:"id"`
	PassID                string    `json:"pass_id"`
	SegmentID             string    `json:"segment_id"`
	Source                string    `json:"source"`
	Destination           string    `json:"destination"`
	SourceGeneration      int64     `json:"source_generation"`
	DestinationGeneration int64     `json:"destination_generation"`
	AttemptID             string    `json:"attempt_id"`
	DecoderEpoch          int64     `json:"decoder_epoch"`
	FrozenFrames          int       `json:"frozen_frames"`
	State                 State     `json:"state"`
	FailureReason         string    `json:"failure_reason,omitempty"`
	PreparedAt            time.Time `json:"prepared_at"`
	TransferredAt         time.Time `json:"transferred_at,omitempty"`
	AbortedAt             time.Time `json:"aborted_at,omitempty"`
	CleanedAt             time.Time `json:"cleaned_at,omitempty"`
}

type Coordinator struct {
	mu         sync.Mutex
	path       string
	sessions   map[string]Session
	recordings *pass.RecordingState
	recorder   *recorder.Manager
	attempts   *receiver.AttemptDispatcher
	audit      *audit.Trail
}

func NewCoordinator(paths store.Paths, recordings *pass.RecordingState, manager *recorder.Manager, attempts *receiver.AttemptDispatcher, trail *audit.Trail) (*Coordinator, error) {
	coordinator := &Coordinator{
		path: paths.Snapshot("handover-sessions"), sessions: make(map[string]Session),
		recordings: recordings, recorder: manager, attempts: attempts, audit: trail,
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
	// A crash between ownership transfer and writer resume leaves a session in
	// confirmed: finish the resume so the destination can write again. Sessions
	// still in prepared stay frozen for an operator to confirm or abort.
	recovered := false
	for _, session := range coordinator.sessions {
		if session.State != Confirmed {
			continue
		}
		if _, err := manager.Resume(session.SegmentID, session.Destination, session.DestinationGeneration); err != nil {
			return nil, fmt.Errorf("recover handover %s: %w", session.ID, err)
		}
		session.State = Transferred
		session.TransferredAt = time.Now().UTC()
		coordinator.sessions[session.ID] = session
		recovered = true
		_, _ = trail.Record(audit.Event{Component: "handover", Action: "recovered", Subject: session.ID, Fields: map[string]any{"pass_id": session.PassID, "destination": session.Destination, "destination_generation": session.DestinationGeneration}})
	}
	if recovered {
		if err := coordinator.flushLocked(); err != nil {
			return nil, err
		}
	}
	return coordinator, nil
}

// Prepare freezes source writes and records the receive attempt and decoder
// epoch the handover is pinned to. Ownership stays with the source until the
// destination confirms.
func (c *Coordinator) Prepare(passID, destination string) (Session, error) {
	if destination == "" {
		return Session{}, fmt.Errorf("handover destination is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	value, segment, err := c.recordings.ActiveSegment(passID)
	if err != nil {
		return Session{}, err
	}
	if destination == value.OwnerStation {
		return Session{}, fmt.Errorf("handover destination must differ from current owner")
	}
	for _, existing := range c.sessions {
		if existing.SegmentID == segment.ID && (existing.State == Prepared || existing.State == Confirmed) {
			return Session{}, fmt.Errorf("handover %s is already in flight for this segment", existing.ID)
		}
	}
	frozen, err := c.recorder.Freeze(segment.ID, value.OwnerStation, value.OwnerGeneration)
	if err != nil {
		return Session{}, err
	}
	attempt, _ := c.attempts.Current(passID)
	session := Session{
		ID: uuid.NewString(), PassID: passID, SegmentID: segment.ID,
		Source: value.OwnerStation, Destination: destination,
		SourceGeneration: value.OwnerGeneration,
		AttemptID: attempt.ID, DecoderEpoch: value.DecoderEpoch,
		FrozenFrames: frozen.FramesBuffered,
		State:        Prepared, PreparedAt: time.Now().UTC(),
	}
	c.sessions[session.ID] = session
	if err := c.flushLocked(); err != nil {
		delete(c.sessions, session.ID)
		_, _ = c.recorder.Resume(segment.ID, value.OwnerStation, value.OwnerGeneration)
		return Session{}, err
	}
	_, _ = c.audit.Record(audit.Event{Component: "handover", Action: "prepared", Subject: session.ID, Fields: map[string]any{"pass_id": passID, "segment_id": segment.ID, "source": session.Source, "destination": destination, "attempt_id": session.AttemptID, "decoder_epoch": session.DecoderEpoch, "frozen_frames": session.FrozenFrames}})
	return session, nil
}

// Confirm lets the destination station take over: ownership advances first,
// then writes resume under the new owner. If the transfer fails the session
// stays prepared with the source still owning every unwritten frame.
func (c *Coordinator) Confirm(sessionID, station string) (Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, found := c.sessions[sessionID]
	if !found {
		return Session{}, fmt.Errorf("handover session %s not found", sessionID)
	}
	if station != session.Destination {
		return Session{}, fmt.Errorf("station %s is not the handover destination", station)
	}
	if session.State == Transferred {
		return session, nil
	}
	if session.State != Prepared && session.State != Confirmed {
		return Session{}, fmt.Errorf("handover session %s cannot confirm from %s", sessionID, session.State)
	}
	if session.State == Prepared {
		segment, err := c.recordings.TransferOwner(session.PassID, session.SegmentID, session.Source, session.Destination, session.SourceGeneration)
		if err != nil {
			_, _ = c.audit.Record(audit.Event{Component: "handover", Action: "confirm.failed", Subject: session.ID, Severity: audit.Warning, Fields: map[string]any{"error": err.Error()}})
			return Session{}, err
		}
		session.DestinationGeneration = segment.OwnerGeneration
		session.State = Confirmed
		c.sessions[sessionID] = session
		if err := c.flushLocked(); err != nil {
			return Session{}, err
		}
	}
	if _, err := c.recorder.Resume(session.SegmentID, session.Destination, session.DestinationGeneration); err != nil {
		return Session{}, err
	}
	session.State = Transferred
	session.TransferredAt = time.Now().UTC()
	c.sessions[sessionID] = session
	if err := c.flushLocked(); err != nil {
		return Session{}, err
	}
	_, _ = c.audit.Record(audit.Event{Component: "handover", Action: "transferred", Subject: session.ID, Fields: map[string]any{"pass_id": session.PassID, "destination": session.Destination, "destination_generation": session.DestinationGeneration, "decoder_epoch": session.DecoderEpoch}})
	return session, nil
}

// Abort cancels a prepared handover: the source keeps ownership and every
// buffered frame, and writes resume under the source.
func (c *Coordinator) Abort(sessionID, reason string) (Session, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, found := c.sessions[sessionID]
	if !found {
		return Session{}, fmt.Errorf("handover session %s not found", sessionID)
	}
	if session.State != Prepared {
		return Session{}, fmt.Errorf("handover session %s cannot abort from %s", sessionID, session.State)
	}
	if _, err := c.recorder.Resume(session.SegmentID, session.Source, session.SourceGeneration); err != nil {
		return Session{}, err
	}
	session.State = Aborted
	session.FailureReason = reason
	session.AbortedAt = time.Now().UTC()
	c.sessions[sessionID] = session
	if err := c.flushLocked(); err != nil {
		return Session{}, err
	}
	_, _ = c.audit.Record(audit.Event{Component: "handover", Action: "aborted", Subject: session.ID, Severity: audit.Warning, Fields: map[string]any{"pass_id": session.PassID, "reason": reason, "frames_preserved": session.FrozenFrames}})
	return session, nil
}

func (c *Coordinator) Cleanup(sessionID string) error {
	c.mu.Lock()
	session, found := c.sessions[sessionID]
	if !found {
		c.mu.Unlock()
		return fmt.Errorf("handover session %s not found", sessionID)
	}
	if session.State != Transferred {
		c.mu.Unlock()
		return fmt.Errorf("handover session %s has not transferred", sessionID)
	}
	session.CleanedAt = time.Now().UTC()
	c.sessions[sessionID] = session
	if err := c.flushLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()
	err := c.recorder.CloseForOwner(session.SegmentID, session.Source, session.SourceGeneration, "handover source cleanup")
	_, _ = c.audit.Record(audit.Event{Component: "handover", Action: "source.cleaned", Subject: session.ID, Fields: map[string]any{"close_error": errorText(err)}})
	return err
}

func (c *Coordinator) flushLocked() error {
	values := make([]Session, 0, len(c.sessions))
	for _, session := range c.sessions {
		values = append(values, session)
	}
	return store.WriteJSON(c.path, values)
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
