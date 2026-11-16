# GoBot

`github.com/Kenboo14/GoBot` is an ultra-low-overhead, multi-session Telegram MTProto engine and Pyrogram-inspired Go library built on [`gotd/td`](https://github.com/gotd/td) primitives for [GotdBot](https://github.com/Kenboo14/GotdBot).

It replaces per-session `telegram.Client` background loops and per-session peer stores with a single shared scheduler, bounded worker pool, and sharded pointer-free LRU peer cache—cutting per-session RAM and goroutine overhead by roughly an order of magnitude at 1,000+ concurrent accounts.

## Benchmark (1,000 Concurrent Sessions vs Local `tgtest` DC)

Measured on Go 1.27.3 (`GOMAXPROCS=12`) running 1,000 concurrent MTProto sessions with `Self`, `ContactsGetContacts`, `MessagesGetDialogs`, 30s idle, and 30s load at `1 upd/session/s` (`1,000 updates/s` total):

| Metric (Settled @ 1,000 sessions) | `gotd-ubot` (baseline) | `gotd-optimized` | `gobot-engine` (`GoBot`) | Change vs `gotd-ubot` |
|---|---:|---:|---:|---:|
| Sessions ready / failed | 1000 / 0 | 1000 / 0 | **1000 / 0** | — |
| Total goroutines | 11,001 (`11.00`/sess) | 11,001 (`11.00`/sess) | **1,026** (`1.025`/sess) | **−90.7%** |
| Stack in-use | 86.2 MiB (`88.10 KiB`/sess) | 86.2 MiB (`87.97 KiB`/sess) | **8.3 MiB** (`8.16 KiB`/sess) | **−90.7%** |
| Heap in-use | 125.0 MiB (`126.95 KiB`/sess) | 43.1 MiB (`42.59 KiB`/sess) | **10.3 MiB** (`9.03 KiB`/sess) | **−92.9%** (per sess) |
| Live heap alloc | `96.15 KiB`/sess | `24.93 KiB`/sess | **`5.47 KiB`/sess** | **−94.3%** |
| Heap objects | 540.01/sess | 187.88/sess | **37.10/sess** | **−93.1%** |
| Process RSS | 306.2 MiB (`302.20 KiB`/sess) | 178.8 MiB (`169.45 KiB`/sess) | **44.2 MiB** (`31.72 KiB`/sess) | **−89.5%** (per sess) |
| Idle CPU (per 1,000 sess) | 0.0167 core | 0.0109 core | **0.0005 core** | **−96.9%** |
| Load throughput (1k upd/s) | 1,000.2 upd/s (0.089 core) | 999.6 upd/s (0.094 core) | **1,000.2 upd/s** (0.102 core) | — |
| Leaked goroutines after stop | 0 | 0 | **0** | — |

Raw benchmark JSON reports are stored in [`bench/out/`](bench/out).

## Packages

- **`gobot` (root)**: `Pool`, `Client` (`NewClient` using `engine.Conn` or `NewFromGotd` wrapping `*telegram.Client`), `OnMessage`, `OnCallbackQuery`, `OnInlineQuery`, `OnRawUpdate`, `SendMessage`, `EditMessageText`, `DeleteMessages`, `AnswerCallbackQuery`, plus `Bot` drop-in compatibility for GotdBot gateway (`NewBot`, `NewEngineBot`, `NewTestBot`, `RegisterHandler`, `DispatchUpdate`).
- **`engine`**: Lightweight `bin.Invoker` (`Conn`) running 1 goroutine per session coordinated by a single global `Scheduler` (staggered keepalive pings, batched acks, salt refreshes, and ordered per-session update dispatch on a fixed worker pool).
- **`peers`**: 16-shard bounded intrusive LRU cache (`Cache` / `PeerStorage`) using flat pointer-free slices (zero GC scan overhead), optional SQLite persistence, entity caching, and `ResolvePeer`.
- **`filters`**: Composable message/callback predicates (`All`, `Me`, `Incoming`, `Private`, `Group`, `Channel`, `Text`, `Prefix`, `Command`, `CommandWithPrefixes`, `Regex`) with `.And()`, `.Or()`, `.Not()`.
- **`parser`**: HTML and Rich-HTML parser (`ParseHTML`, `CleanHTML`, `FormatRichHTML`, `IsRichContent`, `SanitizeEntities`).
- **`types`**: Shared Bot API / MTProto models (`Message`, `User`, `Chat`, `CallbackQuery`, `Update`, parameter structs) and converters (`ExtractThreadID`, `ConvertTGEntities`, `ConvertMessage`, `ParseCommand`).
- **`keyboard`**: Ergonomic inline/reply keyboard builders (`Inline`, `Row`, `Callback`, `URL`, `Reply`, `RemoveReply`) and `ConvertReplyMarkup`.
- **`session`**: AES-GCM session blob helpers (`Encrypt`, `Decrypt`, `EncryptedStorage`, `NewEncryptedBlobStorage`) and `IsSessionRevoked`.

## Quick Example

```go
pool := gobot.NewPool(gobot.PoolOptions{})
defer pool.Close()

client, err := pool.NewClient(gobot.ClientOptions{
    AppID:          apiID,
    APIHash:        apiHash,
    SessionStorage: store,
})
if err != nil {
    log.Fatal(err)
}

client.OnMessage(filters.Me.And(filters.Prefix("!ping")), func(ctx context.Context, c *gobot.Client, m *types.Message) error {
    _, err := c.SendText(ctx, m.Chat.ID, "<b>Pong!</b>", types.ParseModeHTML)
    return err
})

err = client.Run(ctx, func(ctx context.Context) error {
    _, err := client.Self(ctx)
    return err // return after startup; connection stays alive until ctx cancels
})
```

## Verification & Benchmark Commands

```bash
go vet ./...
go test ./...

# Local multi-session benchmark (terminal 1: fake DC, terminal 2: load generator)
go run ./bench/cmd/fakedc -info bench/out/dc.json
go run ./bench/cmd/loadgen -profile gobot-engine -n 1000 -settle 15s -idle 30s -load 30s -rate 1
```
