package recorder

import (
	"context"
	"fmt"
	"time"

	"orbitlink/internal/audit"
)

func (m *Manager) Drain(ctx context.Context, segmentID string) (int, error) {
	m.mu.Lock()
	current, found := m.sessions[segmentID]
	if !found {
		m.mu.Unlock()
		return 0, fmt.Errorf("recording segment %s not found", segmentID)
	}
	if current.segment.Status != Open && current.segment.Status != Draining {
		m.mu.Unlock()
		return 0, fmt.Errorf("recording segment %s cannot drain from %s", segmentID, current.segment.Status)
	}
	if current.segment.Frozen {
		m.mu.Unlock()
		return 0, fmt.Errorf("recording segment %s is frozen for handover", segmentID)
	}
	current.segment.Status = Draining
	frames := append([]Frame(nil), current.buffer...)
	m.mu.Unlock()

	written := 0
	for _, frame := range frames {
		select {
		case <-ctx.Done():
			return written, fmt.Errorf("recorder drain canceled after %d frames: %w", written, ctx.Err())
		default:
		}
		if err := m.writer.Append(segmentID, frame); err != nil {
			return written, err
		}
		written++
	}

	m.mu.Lock()
	current = m.sessions[segmentID]
	current.buffer = current.buffer[written:]
	current.segment.FramesWritten += written
	current.segment.FramesBuffered = len(current.buffer)
	err := m.flushLocked()
	m.mu.Unlock()
	if err == nil {
		_, _ = m.audit.Record(audit.Event{Component: "recorder", Action: "drained", Subject: segmentID, Fields: map[string]any{"frames": written}})
	}
	return written, err
}

func (m *Manager) Seal(segmentID, reason string) (SealResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, found := m.sessions[segmentID]
	if !found {
		return SealResult{}, fmt.Errorf("recording segment %s not found", segmentID)
	}
	if len(current.buffer) != 0 {
		return SealResult{Segment: current.segment, Complete: false, Error: "frames remain buffered"}, fmt.Errorf("recording still has %d buffered frames", len(current.buffer))
	}
	current.segment.Status = Sealed
	current.segment.SealReason = reason
	current.segment.SealedAt = time.Now().UTC()
	if err := m.flushLocked(); err != nil {
		return SealResult{}, err
	}
	_, _ = m.audit.Record(audit.Event{Component: "recorder", Action: "sealed", Subject: segmentID, Fields: map[string]any{"frames": current.segment.FramesWritten, "reason": reason}})
	return SealResult{Segment: current.segment, Complete: true}, nil
}
