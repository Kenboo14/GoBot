package peers

import (
	"context"
	"fmt"

	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/tg"
)

// ResolvePeer resolves a tg.PeerClass into a tg.InputPeerClass using (in order):
//  1. incoming update entities
//  2. the shared bounded LRU cache
//  3. RPC fallbacks (messages.getMessages / contacts.getContacts / messages.getChats / messages.getDialogs)
func (c *Cache) ResolvePeer(
	ctx context.Context,
	api *tg.Client,
	ownerID int64,
	entities tg.Entities,
	p tg.PeerClass,
	msgID int,
) (tg.InputPeerClass, error) {
	if p == nil {
		return nil, fmt.Errorf("peer is nil")
	}

	c.CacheEntities(entities)

	switch peerObj := p.(type) {
	case *tg.PeerUser:
		if peerObj.UserID == 0 || (ownerID > 0 && peerObj.UserID == ownerID) {
			return &tg.InputPeerSelf{}, nil
		}
		if u, ok := entities.Users[peerObj.UserID]; ok && u != nil && !u.Min && u.AccessHash != 0 {
			c.SaveUser(u.ID, u.AccessHash)
			return &tg.InputPeerUser{UserID: u.ID, AccessHash: u.AccessHash}, nil
		}
		if hash, ok := c.UserHash(peerObj.UserID); ok && hash != 0 {
			return &tg.InputPeerUser{UserID: peerObj.UserID, AccessHash: hash}, nil
		}
		if api != nil {
			if msgID > 0 {
				if msgs, err := api.MessagesGetMessages(ctx, []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}}); err == nil {
					if mod, ok := msgs.AsModified(); ok {
						c.ApplyUsersChats(mod.GetUsers(), mod.GetChats())
						if hash, ok := c.UserHash(peerObj.UserID); ok && hash != 0 {
							return &tg.InputPeerUser{UserID: peerObj.UserID, AccessHash: hash}, nil
						}
					}
				}
			}
			if contacts, err := api.ContactsGetContacts(ctx, 0); err == nil {
				if cc, ok := contacts.(*tg.ContactsContacts); ok {
					c.ApplyUsersChats(cc.Users, nil)
					if hash, ok := c.UserHash(peerObj.UserID); ok && hash != 0 {
						return &tg.InputPeerUser{UserID: peerObj.UserID, AccessHash: hash}, nil
					}
				}
			}
			if dialogs, err := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
				Limit:      100,
				OffsetPeer: &tg.InputPeerEmpty{},
			}); err == nil {
				if mod, ok := dialogs.AsModified(); ok {
					c.ApplyUsersChats(mod.GetUsers(), mod.GetChats())
					if hash, ok := c.UserHash(peerObj.UserID); ok && hash != 0 {
						return &tg.InputPeerUser{UserID: peerObj.UserID, AccessHash: hash}, nil
					}
				}
			}
		}
		return nil, fmt.Errorf("peer %v cannot be resolved: access_hash not found", p)

	case *tg.PeerChat:
		c.SaveChat(peerObj.ChatID)
		return &tg.InputPeerChat{ChatID: peerObj.ChatID}, nil

	case *tg.PeerChannel:
		if ch, ok := entities.Channels[peerObj.ChannelID]; ok && ch != nil && !ch.Min && ch.AccessHash != 0 {
			c.SaveChannel(ch.ID, ch.AccessHash)
			return &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash}, nil
		}
		if hash, ok := c.ChannelHash(peerObj.ChannelID); ok && hash != 0 {
			return &tg.InputPeerChannel{ChannelID: peerObj.ChannelID, AccessHash: hash}, nil
		}
		if api != nil {
			if chats, err := api.MessagesGetChats(ctx, []int64{peerObj.ChannelID}); err == nil {
				c.ApplyUsersChats(nil, chats.GetChats())
				if hash, ok := c.ChannelHash(peerObj.ChannelID); ok && hash != 0 {
					return &tg.InputPeerChannel{ChannelID: peerObj.ChannelID, AccessHash: hash}, nil
				}
			}
			if dialogs, err := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
				Limit:      100,
				OffsetPeer: &tg.InputPeerEmpty{},
			}); err == nil {
				if mod, ok := dialogs.AsModified(); ok {
					c.ApplyUsersChats(mod.GetUsers(), mod.GetChats())
					if hash, ok := c.ChannelHash(peerObj.ChannelID); ok && hash != 0 {
						return &tg.InputPeerChannel{ChannelID: peerObj.ChannelID, AccessHash: hash}, nil
					}
				}
			}
		}
		return nil, fmt.Errorf("peer %v cannot be resolved: access_hash not found", p)

	default:
		return peer.EntitiesFromUpdate(entities).ExtractPeer(p)
	}
}
