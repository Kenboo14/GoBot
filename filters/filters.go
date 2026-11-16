// Package filters provides composable Pyrogram-style message and callback
// update predicates.
package filters

import (
	"regexp"
	"strings"

	"github.com/Kenboo14/GoBot/types"
)

// Filter matches a Message update.
type Filter func(m *types.Message) bool

// CallbackFilter matches a CallbackQuery update.
type CallbackFilter func(cq *types.CallbackQuery) bool

// And returns a Filter that matches only when both f and other return true.
func (f Filter) And(other Filter) Filter {
	return func(m *types.Message) bool {
		return f(m) && other(m)
	}
}

// Or returns a Filter that matches when either f or other returns true.
func (f Filter) Or(other Filter) Filter {
	return func(m *types.Message) bool {
		return f(m) || other(m)
	}
}

// Not returns the logical negation of f.
func (f Filter) Not() Filter {
	return func(m *types.Message) bool {
		return !f(m)
	}
}

// All matches every non-nil message.
var All Filter = func(m *types.Message) bool { return m != nil }

// Me matches outgoing messages sent by the current account.
var Me Filter = func(m *types.Message) bool { return m != nil && m.Out }

// Incoming matches incoming messages from other users.
var Incoming Filter = func(m *types.Message) bool { return m != nil && !m.Out }

// Private matches private (1-on-1) chats.
var Private Filter = func(m *types.Message) bool {
	if m == nil {
		return false
	}
	if m.Chat.Type != "" {
		return m.Chat.Type == "private"
	}
	return m.Chat.ID > 0
}

// Group matches basic groups and supergroups.
var Group Filter = func(m *types.Message) bool {
	if m == nil {
		return false
	}
	if m.Chat.Type != "" {
		return m.Chat.Type == "group" || m.Chat.Type == "supergroup"
	}
	return m.Chat.ID < 0
}

// Channel matches broadcast channels.
var Channel Filter = func(m *types.Message) bool {
	if m == nil {
		return false
	}
	return m.Chat.Type == "channel"
}

// Text matches messages with non-empty text.
var Text Filter = func(m *types.Message) bool {
	return m != nil && m.Text != ""
}

// Prefix matches messages whose text starts with p.
func Prefix(p string) Filter {
	return func(m *types.Message) bool {
		return m != nil && strings.HasPrefix(m.Text, p)
	}
}

// Command matches "/" commands (e.g. Command("start", "help")). If cmds is
// empty, any "/" command matches. Bot mentions (/cmd@bot) are stripped before
// comparing against cmds.
func Command(cmds ...string) Filter {
	return CommandWithPrefixes([]string{"/"}, cmds...)
}

// CommandWithPrefixes matches commands starting with any of prefixes.
func CommandWithPrefixes(prefixes []string, cmds ...string) Filter {
	set := make(map[string]struct{}, len(cmds))
	for _, c := range cmds {
		set[strings.ToLower(c)] = struct{}{}
	}
	return func(m *types.Message) bool {
		if m == nil || m.Text == "" {
			return false
		}
		for _, p := range prefixes {
			name, _, ok := types.ParseCommand(m.Text, p)
			if !ok {
				continue
			}
			if p == "/" {
				if idx := strings.IndexByte(name, '@'); idx >= 0 {
					name = name[:idx]
				}
			}
			if len(set) == 0 {
				return true
			}
			if _, exists := set[name]; exists {
				return true
			}
		}
		return false
	}
}

// Regex matches messages whose Text or Caption matches re.
func Regex(re *regexp.Regexp) Filter {
	return func(m *types.Message) bool {
		if m == nil || re == nil {
			return false
		}
		if m.Text != "" && re.MatchString(m.Text) {
			return true
		}
		return m.Caption != "" && re.MatchString(m.Caption)
	}
}

// CallbackExact matches callback queries with exact data match.
func CallbackExact(data string) CallbackFilter {
	return func(cq *types.CallbackQuery) bool {
		return cq != nil && cq.Data == data
	}
}

// CallbackPrefix matches callback queries whose data starts with prefix.
func CallbackPrefix(prefix string) CallbackFilter {
	return func(cq *types.CallbackQuery) bool {
		return cq != nil && strings.HasPrefix(cq.Data, prefix)
	}
}
