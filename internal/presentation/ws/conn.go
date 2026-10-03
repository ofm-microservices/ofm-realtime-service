package ws

import "time"

// Send writes one frame to the websocket client.
func (c *ClientConn) Send(payload []byte) error {
	if c.Done == nil {
		return c.Close()
	}
	select {
	case <-c.Done:
		return c.Close()
	case c.SendQueue <- payload:
	default:
		return c.Close()
	}
	return nil
}

// ConnectionID returns the assigned connection identifier.
func (c *ClientConn) ConnectionID() string { return c.ConnectionIDValue }

// UserID returns the authenticated user identifier.
func (c *ClientConn) UserID() string { return c.UserIDValue }

// Close closes the websocket connection.
func (c *ClientConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		if c.Done != nil {
			close(c.Done)
		}
		if c.Conn != nil {
			err = c.Conn.Close()
		}
	})
	return err
}

// ConnectedAt returns the local connection start time.
func (c *ClientConn) ConnectedAt() time.Time { return c.ConnectedAtValue }
