package handover

import "fmt"

func (c *Coordinator) Get(sessionID string) (Session, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, found := c.sessions[sessionID]
	return session, found
}

func (c *Coordinator) List(passID string) []Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	values := make([]Session, 0, len(c.sessions))
	for _, session := range c.sessions {
		if passID == "" || session.PassID == passID {
			values = append(values, session)
		}
	}
	return values
}

func (s Session) ValidateTransferred() error {
	if s.State != Transferred || s.TransferredAt.IsZero() || s.DestinationGeneration <= s.SourceGeneration {
		return fmt.Errorf("handover ownership has not advanced")
	}
	return nil
}
