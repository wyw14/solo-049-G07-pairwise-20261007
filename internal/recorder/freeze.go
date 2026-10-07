package recorder

import (
	"fmt"

	"orbitlink/internal/audit"
)

// Freeze stops a segment from accepting or draining frames while ownership
// moves to another station. Buffered frames stay in memory so a failed
// handover loses nothing.
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
	if current.segment.Status == Frozen {
		return current.segment, nil
	}
	if current.segment.Status != Open {
		return Segment{}, fmt.Errorf("recording segment %s cannot freeze from %s", segmentID, current.segment.Status)
	}
	current.segment.Status = Frozen
	if err := m.flushLocked(); err != nil {
		return Segment{}, err
	}
	_, _ = m.audit.Record(audit.Event{Component: "recorder", Action: "frozen", Subject: segmentID, Fields: map[string]any{"station": station, "generation": expectedGeneration, "buffered_frames": len(current.buffer)}})
	return current.segment, nil
}

// Resume returns a frozen segment to open once ownership is settled, under
// whichever station and generation the handover resolved to.
func (m *Manager) Resume(segmentID, station string, expectedGeneration int64) (Segment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, found := m.sessions[segmentID]
	if !found {
		return Segment{}, fmt.Errorf("recording segment %s not found", segmentID)
	}
	if current.segment.OwnerStation != station || current.segment.OwnerGeneration != expectedGeneration {
		return Segment{}, fmt.Errorf("recording owner generation changed")
	}
	if current.segment.Status != Frozen {
		return Segment{}, fmt.Errorf("recording segment %s is not frozen", segmentID)
	}
	current.segment.Status = Open
	if err := m.flushLocked(); err != nil {
		return Segment{}, err
	}
	_, _ = m.audit.Record(audit.Event{Component: "recorder", Action: "resumed", Subject: segmentID, Fields: map[string]any{"station": station, "generation": expectedGeneration, "buffered_frames": len(current.buffer)}})
	return current.segment, nil
}
