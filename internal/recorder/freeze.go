package recorder

import (
	"fmt"

	"orbitlink/internal/audit"
)

// Freeze stops all writes to the segment (buffering and draining) while a
// recording handover is prepared. Only the current owner generation may
// freeze, and the buffered frames are left untouched so a later abort keeps
// them for the source owner.
func (m *Manager) Freeze(segmentID, station string, expectedGeneration int64) (Segment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, found := m.sessions[segmentID]
	if !found {
		return Segment{}, fmt.Errorf("recording segment %s not found", segmentID)
	}
	if current.segment.OwnerStation != station || current.segment.OwnerGeneration != expectedGeneration {
		return Segment{}, fmt.Errorf("recording owner generation changed")
	}
	if current.segment.Status != Open {
		return Segment{}, fmt.Errorf("recording segment %s cannot freeze from %s", segmentID, current.segment.Status)
	}
	if current.segment.Frozen {
		return Segment{}, fmt.Errorf("recording segment %s is already frozen", segmentID)
	}
	current.segment.Frozen = true
	if err := m.flushLocked(); err != nil {
		return Segment{}, err
	}
	_, _ = m.audit.Record(audit.Event{Component: "recorder", Action: "frozen", Subject: segmentID, Fields: map[string]any{"station": station, "generation": expectedGeneration, "frames_buffered": current.segment.FramesBuffered}})
	return current.segment, nil
}

// Unfreeze releases a frozen segment back to its owner, keeping every
// buffered frame so the source station resumes exactly where it stopped.
func (m *Manager) Unfreeze(segmentID, station string, expectedGeneration int64) (Segment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, found := m.sessions[segmentID]
	if !found {
		return Segment{}, fmt.Errorf("recording segment %s not found", segmentID)
	}
	if current.segment.OwnerStation != station || current.segment.OwnerGeneration != expectedGeneration {
		return Segment{}, fmt.Errorf("recording owner generation changed")
	}
	if !current.segment.Frozen {
		return Segment{}, fmt.Errorf("recording segment %s is not frozen", segmentID)
	}
	current.segment.Frozen = false
	if err := m.flushLocked(); err != nil {
		return Segment{}, err
	}
	_, _ = m.audit.Record(audit.Event{Component: "recorder", Action: "unfrozen", Subject: segmentID, Fields: map[string]any{"station": station, "generation": expectedGeneration, "frames_buffered": current.segment.FramesBuffered}})
	return current.segment, nil
}
