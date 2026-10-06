# ChangeFeedSpine

**One history stream, many consumers, and an AI agent that is just another transaction author.**

Most iOS apps keep their side systems in sync with ad-hoc plumbing: a `NotificationCenter` post here, an `onChange` there, a "remember to reindex" call in the save path. It works until there are five side systems — a sync outbox, a search index, the Spotlight/App Intents entity index, widget reloads, an on-device semantic index — and each one fails differently. Then the app has five half-built delivery systems and no answer to the questions that matter: *what happens when one of them crashes mid-batch? When the server's own changes come back down? When history was pruned while a consumer was offline? And what exactly did the agent change, and can I undo it without undoing the user?*

iOS 27's SwiftData `HistoryObserver` finally gives apps a first-party change feed (persistent-history transactions with a monotonic event counter and author filtering). This package is the **change-data-capture spine** you put on top of it: a single history stream fanned out to independent lanes, each with its own durable cursor, back-pressure budget, retry policy, poison quarantine and snapshot-rebuild path — plus an agent audit that answers "what did the agent change?" from the stream itself and plans a conflict-checked undo.

> Demo app: (added after the companion repo is pushed)

## Why this matters

- **Failure isolation is the product.** One slow, crashed or poisoned consumer must never block the others. Here that is structural: lanes share nothing mutable, and the dispatcher owns no delivery state at all.
- **Echo loops are an authorship problem.** A sync outbox that re-uploads what it just downloaded loops forever. Making every write carry an `Author` turns that into a one-line subscription filter — and a test proves the filter is load-bearing.
- **Agents need an audit trail you didn't have to build.** If the agent writes as `Author.agent(session)`, "what did it change?" is a filter over history every consumer already reads, and "undo it" is a compensating transaction checked against everything that happened since.

## Architecture

```
 SwiftData persistent history (HistoryObserver)        ← production adapter, not in this package
            │  HistorySource (port)
            ▼
 ┌──────────────────────────── ChangeFeed (core) ────────────────────────────┐
 │ ChangeDispatcher ── owns no delivery state; steps lanes concurrently      │
 │   ├─ ConsumerLane "sync-outbox"   cursor · budget · backoff · dead letters│
 │   ├─ ConsumerLane "search-index"  cursor · budget · backoff · dead letters│
 │   └─ ConsumerLane "widget-reload" cursor · budget · backoff · dead letters│
 │ CursorStore (port)   AgentAudit · UndoVerifier   SaturatingMath           │
 └───────────────────────────────────────────────────────────────────────────┘
            ▲  ChangeConsumer (port)
 ChangeFeedAdapters (feature-owned): SyncOutboxConsumer · SearchIndexConsumer · WidgetReloadConsumer
 ChangeFeedUI: a SwiftUI console over all of the above
```

The dependency direction is enforced by `Package.swift`: `ChangeFeed` cannot import an adapter.

| Module | Contents |
|---|---|
| `ChangeFeed` | `Author`, `HistoryToken`, `Change` (with before/after images), `Transaction`, `HistorySource` port + `InMemoryHistoryStore` reference store (atomic author-tagged commits, bounded retention), `ChangeConsumer` + `Subscription`, `CursorStore` + `InMemoryCursorStore`, `ChangeDispatcher`, the internal `ConsumerLane` actor, `BatchBudget`, `RetryPolicy`, `AgentAudit`, `UndoVerifier`, `SaturatingMath` |
| `ChangeFeedAdapters` | `SyncOutboxConsumer` (excludes `.sync` by default; capacity back-pressure; full-resync on rebuild), `SearchIndexConsumer` (inverted index; can be poisoned), `WidgetReloadConsumer` (coalesces a batch into one reload; reload budget) |
| `ChangeFeedUI` | `ChangeFeedConsole` + `ConsoleModel` (Apple platforms only) |

## Delivery guarantees (per lane)

| Guarantee | How |
|---|---|
| **Ordered** | Transactions are delivered in strictly increasing token order; the cursor never moves backwards (the lane takes `max`, and `CursorStore` implementations must ignore regressions). |
| **At-least-once** | The cursor is persisted *after* `apply` returns. A crash in between redelivers; consumers are idempotent per token (all three adapters are; a test crashes a consumer between `apply` and the cursor save, and the adapter tests replay redelivered batches into each adapter). |
| **No silent loss** | A transaction is skipped only if the subscription filters it out, or it is recorded as a `DeadLetter`. Unclassified errors are treated as **transient**, never as poison. |
| **Isolation** | A permanently failing batch is re-delivered one transaction at a time; only the offending transaction is quarantined. Other lanes never see the failure. |
| **Bounded** | Batches are capped by `BatchBudget` (transactions *and* changes); dead letters are capped per lane; history retention is capped; every counter is saturating. |
| **Reentrancy-safe** | `ConsumerLane.step` suspends at every `await`; an `inFlight` guard returns `.busy` to a concurrent step instead of letting it re-read the same cursor and deliver a duplicate batch. |

## Design decisions and trade-offs

1. **Filtering happens in the lane, not the consumer.** A transaction a consumer doesn't want must still *advance its cursor*. If each consumer filtered inside `apply`, the first one to forget would re-read skipped transactions forever. *Rejected:* per-consumer filtering.
2. **Transient vs permanent is the consumer's call, and "unknown" means transient.** Treating an unclassified error as poison would quietly drop data on the first unexpected `URLError`. The cost is that a misclassified permanent failure stalls the lane — which `LaneHealth.stalled` surfaces to a human rather than resolving by data loss. *Rejected:* "N retries, then quarantine" (it converts an outage into data loss).
3. **Poison isolation by one-at-a-time redelivery, not bisection.** Bisection needs `O(log n)` calls instead of `n`, but it re-applies the healthy half repeatedly and complicates the at-least-once story. Batches are budget-capped, so `n` is small. *Rejected:* bisection; skipping the whole batch.
4. **An oversized transaction still goes through alone.** If one bulk import exceeds `maxChanges`, refusing it would wedge the lane permanently. Exceeding the budget once is the lesser failure.
5. **Cursor expiry rebuilds from a snapshot; it never "catches up approximately."** When history is pruned past a lane's cursor, the lane calls `rebuild(from:)` with a snapshot that is consistent with an exact token, then follows the stream from that token. The outbox's rebuild queues a *full resync* marker rather than pretending it can reconstruct individual uploads.
6. **`.fromHead` cursors are persisted immediately.** Otherwise a relaunch would re-resolve "head" and silently skip everything committed while the app was dead. A zero cursor is *not* written, because "no stored cursor" already means "replay from the start".
7. **The dispatcher holds no state worth losing.** It is the component most likely to be torn down (relaunch, BGTask expiry), so all delivery state lives in lanes and the cursor store. *Trade-off:* `pumpAll()` returns when its slowest lane returns; hosts that cannot accept that drive lanes individually with `pump(_:)`.
8. **Agent undo only touches fields that still hold the agent's value.** The audit computes the agent's *net field-level* changes (a user edit interleaved mid-session is not attributed to the agent — on another field it is simply left alone, and on the same field the agent's net change is measured from the user's value, so undo restores the user's edit rather than the pre-session value); the planner restores only fields where `current == agent's value` and reports every other case as an `UndoConflict`. `UndoVerifier` checks that safety property for *any* set of operations, independently of how they were produced. *Rejected:* restoring before-images wholesale — it destroys later user edits, and a test shows the verifier catching exactly that.
9. **A partial audit is an error, not a result.** If any of an agent session was pruned, `AgentAudit.report` throws `historyPruned` instead of returning a diff that looks complete.

### What is deliberately not here

- **The SwiftData adapter itself.** `HistoryObserver` is an iOS 27 SDK API, and this repo's CI builds with the `macos-15` runner image's default Xcode rather than an iOS 27 SDK. Rather than ship an adapter that no CI run has ever compiled, the package stops at the port: `HistorySource` mirrors the shape the adapter needs (monotonic token, one author per transaction, history pruning) and `InMemoryHistoryStore` implements the same contract. A SwiftData adapter would be a single type implementing `HistorySource`; **it is not in this repo.**
- **Cross-lane ordering.** Lanes are independent by design; nothing orders lane A's delivery relative to lane B's.

## Using it

```swift
.package(url: "https://github.com/rajatslakhina/change-feed-spine-kit.git", from: "1.0.0")
```

```swift
let store = InMemoryHistoryStore(retentionLimit: 10_000)        // or your SwiftData-backed HistorySource
let dispatcher = ChangeDispatcher(source: store, cursors: myCursorStore, clock: myClock)
try await dispatcher.register(SyncOutboxConsumer(), budget: BatchBudget(maxTransactions: 64, maxChanges: 512))
try await dispatcher.register(SearchIndexConsumer(), start: .snapshot)

await store.commit(author: .agent("assistant#42"), [.patch(noteKey, set: ["body": "…"])])
await dispatcher.drain()

let report = try await AgentAudit.report(session: "assistant#42", source: store)
let plan = AgentAudit.undoPlan(for: report, current: await store.snapshot())
if plan.isComplete { await store.commit(author: .user, plan.operations) }
```

## Tests

`swift test` runs 52 XCTest cases across five suites. Besides edge cases (empty feeds, limits ≤ 0, cursors beyond head, `UInt64.max` tokens, `Int.max` retry attempts, clamped nonsense configuration), the suite includes tests built to **fail against a broken implementation**:

- `testWithoutTheAuthorFilterTheEchoLoopNeverTerminates` — the outbox with its filter removed uploads `[1, 1, 1, 1, 1]` across five round trips; the guarded one uploads `[1, 0, 0, 0, 0]`.
- `testVerifierCatchesANaiveWholesaleRestore` — `UndoVerifier` flags a naive before-image restore that clobbers a user's post-session edit.
- `testSlowConsumerDoesNotHoldBackAnotherLane` — a consumer genuinely suspended inside `apply` (on a gate, not a sleep) while another lane completes; a concurrent step on the suspended lane returns `.busy`, and exactly one delivery happens.
- `testCrashBetweenApplyAndCursorSaveRedeliversAndIdempotentConsumerIgnoresIt` — a fault-injected cursor save, a fresh dispatcher, a redelivered batch, one application.
- `testFromHeadCursorIsPersistedSoARestartDoesNotSkipTransactions`.
- `testTransientFailureDuringIsolationIsRetriedNotQuarantined` — a transient error that arrives *while* a poisoned batch is being isolated is retried, not turned into a dead letter.

Each of these was checked by mutation: removing the reentrancy guard, the poison isolation, the `.fromHead` cursor write, the undo conflict check, the verifier's check, cursor advancement on fully-filtered batches, or the transient branch inside isolation makes at least one test fail.

## Verification

(Filled in from real CI results after the push.)

## License

MIT
