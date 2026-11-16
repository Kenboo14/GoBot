// Package keyboard provides ergonomic reply/inline keyboard builders and
// MTProto reply markup converters.
package keyboard

import (
	"github.com/gotd/td/tg"

	"github.com/Kenboo14/GoBot/types"
)

// Inline builds an InlineKeyboardMarkup from rows of buttons.
func Inline(rows ...[]types.InlineKeyboardButton) *types.InlineKeyboardMarkup {
	return &types.InlineKeyboardMarkup{InlineKeyboard: rows}
}

// Row constructs a keyboard button row.
func Row(buttons ...types.InlineKeyboardButton) []types.InlineKeyboardButton {
	return buttons
}

// Callback creates an inline button triggering callback_data.
func Callback(text, data string) types.InlineKeyboardButton {
	return types.InlineKeyboardButton{Text: text, CallbackData: data}
}

// URL creates an inline URL button.
func URL(text, url string) types.InlineKeyboardButton {
	return types.InlineKeyboardButton{Text: text, URL: url}
}

// SwitchInlineCurrent creates an inline button switching to inline query in the current chat.
func SwitchInlineCurrent(text, query string) types.InlineKeyboardButton {
	return types.InlineKeyboardButton{Text: text, SwitchInlineQueryCurrentChat: query}
}

// Reply builds a ReplyKeyboardMarkup from rows of keyboard buttons.
func Reply(resize, oneTime bool, rows ...[]types.KeyboardButton) *types.ReplyKeyboardMarkup {
	return &types.ReplyKeyboardMarkup{
		Keyboard:        rows,
		ResizeKeyboard:  resize,
		OneTimeKeyboard: oneTime,
	}
}

// RemoveReply returns a ReplyKeyboardRemove markup.
func RemoveReply() *types.ReplyKeyboardRemove {
	return &types.ReplyKeyboardRemove{RemoveKeyboard: true}
}

// ConvertReplyMarkup converts Bot API style markup structs (or passes through
// tg.ReplyMarkupClass) into a tg.ReplyMarkupClass suitable for MTProto calls.
func ConvertReplyMarkup(markup any) tg.ReplyMarkupClass {
	if markup == nil {
		return nil
	}
	switch m := markup.(type) {
	case tg.ReplyMarkupClass:
		return m
	case *types.InlineKeyboardMarkup:
		if m == nil {
			return nil
		}
		var rows []tg.KeyboardInlineButtonRow
		for _, row := range m.InlineKeyboard {
			var buttons []tg.KeyboardInlineButton
			for _, btn := range row {
				var btnType tg.InlineButtonTypeClass
				if btn.CallbackData != "" {
					btnType = &tg.InlineButtonTypeCallback{Data: []byte(btn.CallbackData)}
				} else if btn.URL != "" {
					btnType = &tg.InlineButtonTypeURL{URL: btn.URL}
				} else if btn.SwitchInlineQueryCurrentChat != "" {
					btnType = &tg.InlineButtonTypeSwitchInline{
						SamePeer: true,
						Query:    btn.SwitchInlineQueryCurrentChat,
					}
				}
				if btnType != nil {
					buttons = append(buttons, tg.KeyboardInlineButton{
						Text: btn.Text,
						Type: btnType,
					})
				}
			}
			if len(buttons) > 0 {
				rows = append(rows, tg.KeyboardInlineButtonRow{Buttons: buttons})
			}
		}
		return &tg.ReplyInlineMarkup{Rows: rows}
	case *types.ReplyKeyboardMarkup:
		if m == nil {
			return nil
		}
		var rows []tg.KeyboardButtonRow
		for _, row := range m.Keyboard {
			var buttons []tg.KeyboardButton
			for _, btn := range row {
				var btnType tg.ButtonTypeClass
				if btn.RequestContact {
					btnType = &tg.ButtonTypeRequestPhone{}
				} else {
					btnType = &tg.ButtonTypeDefault{}
				}
				buttons = append(buttons, tg.KeyboardButton{
					Text: btn.Text,
					Type: btnType,
				})
			}
			if len(buttons) > 0 {
				rows = append(rows, tg.KeyboardButtonRow{Buttons: buttons})
			}
		}
		return &tg.ReplyKeyboardMarkup{
			Resize:    m.ResizeKeyboard,
			SingleUse: m.OneTimeKeyboard,
			Rows:      rows,
		}
	case *types.ReplyKeyboardRemove:
		if m == nil {
			return nil
		}
		return &tg.ReplyKeyboardHide{}
	}
	return nil
}
