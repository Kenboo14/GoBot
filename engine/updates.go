package engine

import (
	"github.com/gotd/td/tg"
)

func (c *Conn) enqueueUpdate(u tg.UpdatesClass) {
	if u == nil || c.onUpdate == nil {
		return
	}
	c.updMu.Lock()
	if c.updClosed {
		c.updMu.Unlock()
		return
	}
	c.updQueue = append(c.updQueue, u)
	needSubmit := !c.updRunning
	if needSubmit {
		c.updRunning = true
	}
	c.updMu.Unlock()

	if needSubmit {
		c.sched.submitBlocking(c, taskUpdates)
	}
}

func (c *Conn) drainUpdates() {
	for {
		c.updMu.Lock()
		if len(c.updQueue) == 0 || c.updClosed {
			c.updQueue = nil
			c.updRunning = false
			c.updMu.Unlock()
			return
		}
		batch := c.updQueue
		c.updQueue = nil
		c.updMu.Unlock()

		for _, u := range batch {
			if c.runCtx.Err() != nil {
				break
			}
			_ = c.onUpdate(c.runCtx, u)
		}
	}
}

func normalizeUpdates(updates tg.UpdatesClass) tg.UpdatesClass {
	switch u := updates.(type) {
	case *tg.UpdateShortMessage:
		return shortMessage(u)
	case *tg.UpdateShortChatMessage:
		return shortChatMessage(u)
	case *tg.UpdateShortSentMessage:
		return shortSentMessage(u)
	default:
		return updates
	}
}

func copyOptionalFields(msg *tg.Message, i tg.UpdatesClass) {
	if u, ok := i.(interface {
		GetFwdFrom() (tg.MessageFwdHeader, bool)
	}); ok {
		if v, ok := u.GetFwdFrom(); ok {
			msg.SetFwdFrom(v)
		}
	}
	if u, ok := i.(interface{ GetViaBotID() (int64, bool) }); ok {
		if v, ok := u.GetViaBotID(); ok {
			msg.SetViaBotID(v)
		}
	}
	if u, ok := i.(interface {
		GetReplyTo() (tg.MessageReplyHeaderClass, bool)
	}); ok {
		if v, ok := u.GetReplyTo(); ok {
			msg.SetReplyTo(v)
		}
	}
	if u, ok := i.(interface {
		GetEntities() ([]tg.MessageEntityClass, bool)
	}); ok {
		if v, ok := u.GetEntities(); ok {
			msg.SetEntities(v)
		}
	}
	if u, ok := i.(interface {
		GetMedia() (tg.MessageMediaClass, bool)
	}); ok {
		if v, ok := u.GetMedia(); ok {
			msg.SetMedia(v)
		}
	}
}

func shortMessage(u *tg.UpdateShortMessage) *tg.UpdateShort {
	msg := &tg.Message{
		ID:      u.ID,
		PeerID:  &tg.PeerUser{UserID: u.UserID},
		Message: u.Message,
		Date:    u.Date,
	}
	msg.SetOut(u.Out)
	msg.SetMentioned(u.Mentioned)
	msg.SetMediaUnread(u.MediaUnread)
	msg.SetSilent(u.Silent)
	if !u.Out {
		msg.SetFromID(&tg.PeerUser{UserID: u.UserID})
	}
	copyOptionalFields(msg, u)
	return &tg.UpdateShort{
		Update: &tg.UpdateNewMessage{Message: msg, Pts: u.Pts, PtsCount: u.PtsCount},
		Date:   u.Date,
	}
}

func shortChatMessage(u *tg.UpdateShortChatMessage) *tg.UpdateShort {
	msg := &tg.Message{
		ID:      u.ID,
		PeerID:  &tg.PeerChat{ChatID: u.ChatID},
		Message: u.Message,
		Date:    u.Date,
	}
	msg.SetFromID(&tg.PeerUser{UserID: u.FromID})
	msg.SetOut(u.Out)
	msg.SetMentioned(u.Mentioned)
	msg.SetMediaUnread(u.MediaUnread)
	msg.SetSilent(u.Silent)
	copyOptionalFields(msg, u)
	return &tg.UpdateShort{
		Update: &tg.UpdateNewMessage{Message: msg, Pts: u.Pts, PtsCount: u.PtsCount},
		Date:   u.Date,
	}
}

func shortSentMessage(u *tg.UpdateShortSentMessage) *tg.UpdateShort {
	msg := &tg.Message{
		ID:   u.ID,
		Date: u.Date,
	}
	msg.SetOut(u.Out)
	copyOptionalFields(msg, u)
	return &tg.UpdateShort{
		Update: &tg.UpdateNewMessage{Message: msg, Pts: u.Pts, PtsCount: u.PtsCount},
		Date:   u.Date,
	}
}
