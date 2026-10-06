import XCTest
@testable import ChangeFeed
@testable import ChangeFeedAdapters

final class AdapterTests: XCTestCase {
    /// Simulates a server that acknowledges every upload by sending the same change back
    /// down, as a `.sync`-authored transaction (what a real pull does after a push).
    /// Returns how many entries were uploaded in each of `rounds` round trips.
    private func runRoundTrips(outbox: SyncOutboxConsumer, rounds: Int) async throws -> [Int] {
        let store = InMemoryHistoryStore()
        let dispatcher = ChangeDispatcher(source: store, cursors: InMemoryCursorStore(), clock: ManualFeedClock())
        try await dispatcher.register(outbox)

        await store.commit(author: .user, [.put(note("a"), Record(["title": "hello", "rev": "1"]))])
        var uploadedPerRound: [Int] = []
        for round in 0..<rounds {
            await dispatcher.drain()
            let uploads = await outbox.takeForUpload()
            uploadedPerRound.append(uploads.count)
            // Server echo: it stamps a new server revision on what it received.
            for entry in uploads {
                if case .change(let change) = entry.payload, var record = change.after {
                    record["rev"] = "server-\(round)"
                    await store.commit(author: .sync, [.put(change.key, record)])
                }
            }
        }
        return uploadedPerRound
    }

    func testAuthorFilterPreventsTheSyncEchoLoop() async throws {
        let guarded = try await runRoundTrips(outbox: SyncOutboxConsumer(), rounds: 5)
        XCTAssertEqual(guarded, [1, 0, 0, 0, 0], "the user's edit is uploaded once; the server's echo is never re-sent")
    }

    /// Falsification: the same outbox with the author filter removed. If this test ever
    /// starts producing `[1, 0, 0, 0, 0]`, the echo test above is no longer proving anything.
    func testWithoutTheAuthorFilterTheEchoLoopNeverTerminates() async throws {
        let unguarded = try await runRoundTrips(outbox: SyncOutboxConsumer(subscription: Subscription(authors: .all)), rounds: 5)
        XCTAssertEqual(unguarded, [1, 1, 1, 1, 1], "every pull re-uploads itself, forever")
    }

    func testOutboxIsIdempotentPerTokenAndRebuildQueuesAFullResync() async throws {
        let outbox = SyncOutboxConsumer()
        let transaction = Transaction(token: HistoryToken(1), author: .user, changes: [
            try XCTUnwrap(Change(key: note("a"), before: nil, after: Record(["t": "x"])))
        ])
        try await outbox.apply([transaction])
        try await outbox.apply([transaction])
        let pending = await outbox.pendingEntries()
        XCTAssertEqual(pending.count, 1)
        let duplicates = await outbox.duplicatesIgnored()
        XCTAssertEqual(duplicates, 1)

        await outbox.rebuild(from: Snapshot(token: HistoryToken(9), records: [note("a"): Record(), note("b"): Record()]))
        let afterRebuild = await outbox.pendingEntries()
        XCTAssertEqual(afterRebuild.map(\.payload), [.fullResync(HistoryToken(9), entityCount: 2)])
        // Anything at or before the snapshot token is already covered by the resync.
        try await outbox.apply([transaction])
        let stillOne = await outbox.pendingEntries()
        XCTAssertEqual(stillOne.count, 1)
    }

    func testFullOutboxAppliesBackPressureToTheCursorInsteadOfGrowing() async throws {
        let store = InMemoryHistoryStore()
        let dispatcher = ChangeDispatcher(source: store, cursors: InMemoryCursorStore(), clock: ManualFeedClock())
        let outbox = SyncOutboxConsumer(capacity: 2)
        try await dispatcher.register(outbox, budget: BatchBudget(maxTransactions: 1))
        await commitEdits(store, count: 4)

        _ = try await dispatcher.pump("sync-outbox")
        _ = try await dispatcher.pump("sync-outbox")
        let third = try await dispatcher.pump("sync-outbox")
        XCTAssertEqual(third, .failed(attempt: 1, retryAt: 1))
        let pending = await outbox.pendingEntries()
        XCTAssertEqual(pending.count, 2, "capacity holds")
        let status = try await dispatcher.status(of: "sync-outbox")
        XCTAssertEqual(status.cursor, HistoryToken(2))
        XCTAssertEqual(status.lag, 2)

        _ = await outbox.takeForUpload(limit: 2)
        let drained = await outbox.takeForUpload(limit: -1)
        XCTAssertTrue(drained.isEmpty, "negative limit takes nothing")
    }

    func testSearchIndexQuarantinesOnlyThePoisonedTransactionAndOtherLanesAreUnaffected() async throws {
        let store = InMemoryHistoryStore()
        let dispatcher = ChangeDispatcher(source: store, cursors: InMemoryCursorStore(), clock: ManualFeedClock())
        let index = SearchIndexConsumer()
        let outbox = SyncOutboxConsumer()
        try await dispatcher.register(index)
        try await dispatcher.register(outbox)

        await store.commit(author: .user, [.put(note("a"), Record(["title": "Groceries", "body": "milk eggs"]))])
        await store.commit(author: .agent("s1"), [.put(note("b"), Record(["title": "Draft", SearchIndexConsumer.malformedField: "1"]))])
        await store.commit(author: .user, [.put(note("c"), Record(["title": "Trip", "body": "eggs benedict"]))])
        await dispatcher.drain()

        let eggs = await index.search("EGGS")
        XCTAssertEqual(eggs, [note("a"), note("c")])
        let indexStatus = try await dispatcher.status(of: "search-index")
        XCTAssertEqual(indexStatus.deadLetters.map(\.token), [HistoryToken(2)])
        XCTAssertEqual(indexStatus.cursor, HistoryToken(3))
        let outboxStatus = try await dispatcher.status(of: "sync-outbox")
        XCTAssertTrue(outboxStatus.deadLetters.isEmpty)
        let pending = await outbox.pendingEntries()
        XCTAssertEqual(pending.count, 3, "the outbox has no problem with the record the indexer rejects")
    }

    func testSearchIndexUpdatesRemovesAndRebuilds() async throws {
        let index = SearchIndexConsumer()
        func transaction(_ token: UInt64, _ key: EntityKey, _ before: Record?, _ after: Record?) throws -> Transaction {
            Transaction(token: HistoryToken(token), author: .user, changes: [try XCTUnwrap(Change(key: key, before: before, after: after))])
        }
        try await index.apply([try transaction(1, note("a"), nil, Record(["title": "alpha beta"]))])
        try await index.apply([try transaction(2, note("a"), Record(["title": "alpha beta"]), Record(["title": "gamma"]))])
        let alpha = await index.search("alpha")
        XCTAssertTrue(alpha.isEmpty, "stale terms are removed on update")
        let gamma = await index.search("gamma")
        XCTAssertEqual(gamma, [note("a")])
        try await index.apply([try transaction(3, note("a"), Record(["title": "gamma"]), nil)])
        let afterDelete = await index.indexedEntities()
        XCTAssertTrue(afterDelete.isEmpty)

        await index.rebuild(from: Snapshot(token: HistoryToken(10), records: [
            note("x"): Record(["body": "delta"]),
            EntityKey(type: "Tag", id: "t"): Record(["title": "delta"]),
            note("bad"): Record(["title": "delta", SearchIndexConsumer.malformedField: "1"])
        ]))
        let delta = await index.search("delta")
        XCTAssertEqual(delta, [note("x")], "rebuild honours the entity-type filter and skips malformed records")
    }

    func testWidgetConsumerCoalescesBatchesAndDefersWhenBudgetIsSpent() async throws {
        let store = InMemoryHistoryStore()
        let clock = ManualFeedClock()
        let dispatcher = ChangeDispatcher(source: store, cursors: InMemoryCursorStore(), clock: clock)
        let widget = WidgetReloadConsumer(budgetPerWindow: 1)
        try await dispatcher.register(widget)
        await commitEdits(store, count: 5)
        _ = try await dispatcher.pump("widget-reload")
        var counters = await widget.counters()
        XCTAssertEqual(counters.reloads, 1)
        XCTAssertEqual(counters.coalescedTransactions, 5, "five edits, one reload")

        await commitEdits(store, count: 1, prefix: "more")
        let deferred = try await dispatcher.pump("widget-reload")
        XCTAssertEqual(deferred, .failed(attempt: 1, retryAt: 1))
        let status = try await dispatcher.status(of: "widget-reload")
        XCTAssertEqual(status.lag, 1, "deferred, not dropped")

        await widget.resetBudget()
        clock.advance(by: 1)
        let resumed = try await dispatcher.pump("widget-reload")
        XCTAssertEqual(resumed, .advanced(to: HistoryToken(6), applied: 1, skipped: 0, quarantined: 0))
        counters = await widget.counters()
        XCTAssertEqual(counters.reloads, 2)
    }

    /// Crash between `apply` and the cursor save, then restart: the real adapters (not a
    /// test double) receive the same batch again and must not apply it twice.
    func testAdaptersIgnoreRedeliveryAfterACrashBeforeTheCursorSave() async throws {
        let store = InMemoryHistoryStore()
        let cursors = InMemoryCursorStore()
        let index = SearchIndexConsumer()
        let widget = WidgetReloadConsumer(budgetPerWindow: 10)
        await store.commit(author: .user, [.put(note("a"), Record(["title": "alpha"]))])
        await store.commit(author: .user, [.patch(note("a"), set: ["title": "beta"])])

        let first = ChangeDispatcher(source: store, cursors: cursors, clock: ManualFeedClock())
        try await first.register(index)
        try await first.register(widget)
        await cursors.failNextSaves(2)
        _ = await first.pumpAll()
        let lost = await cursors.all()
        XCTAssertTrue(lost.isEmpty, "both cursor saves failed: on disk, nothing was handled")
        let reloadsBefore = await widget.counters().reloads
        XCTAssertEqual(reloadsBefore, 1)

        let restarted = ChangeDispatcher(source: store, cursors: cursors, clock: ManualFeedClock())
        try await restarted.register(index)
        try await restarted.register(widget)
        let outcomes = await restarted.pumpAll()
        XCTAssertEqual(outcomes["search-index"], .advanced(to: HistoryToken(2), applied: 2, skipped: 0, quarantined: 0))
        XCTAssertEqual(outcomes["widget-reload"], .advanced(to: HistoryToken(2), applied: 2, skipped: 0, quarantined: 0))

        let duplicates = await index.duplicatesIgnored()
        XCTAssertEqual(duplicates, 2, "both redelivered transactions were recognised")
        let alpha = await index.search("alpha")
        XCTAssertTrue(alpha.isEmpty, "re-applying #1 after #2 would have resurrected the stale term")
        let beta = await index.search("beta")
        XCTAssertEqual(beta, [note("a")])
        let reloadsAfter = await widget.counters().reloads
        XCTAssertEqual(reloadsAfter, 1, "a redelivered batch must not spend another reload")
    }

    func testFromHeadCursorIsPersistedSoARestartDoesNotSkipTransactions() async throws {
        let store = InMemoryHistoryStore()
        let cursors = InMemoryCursorStore()
        await commitEdits(store, count: 4)
        let consumer = RecordingConsumer("late")
        let first = ChangeDispatcher(source: store, cursors: cursors, clock: ManualFeedClock())
        try await first.register(consumer, start: .fromHead)
        _ = try await first.pump("late")

        await commitEdits(store, count: 2, prefix: "while-dead")
        let restarted = ChangeDispatcher(source: store, cursors: cursors, clock: ManualFeedClock())
        try await restarted.register(consumer, start: .fromHead)
        await restarted.drain()
        let applied = await consumer.applied
        XCTAssertEqual(applied.map(\.value), [5, 6], "re-resolving 'head' after a restart would have skipped both")
    }
}
