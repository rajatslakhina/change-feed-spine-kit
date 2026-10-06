import XCTest
@testable import ChangeFeed

final class LaneDeliveryTests: XCTestCase {
    private func makeFeed(retention: Int = 10_000) -> (InMemoryHistoryStore, InMemoryCursorStore, ManualFeedClock, ChangeDispatcher) {
        let store = InMemoryHistoryStore(retentionLimit: retention)
        let cursors = InMemoryCursorStore()
        let clock = ManualFeedClock()
        let dispatcher = ChangeDispatcher(source: store, cursors: cursors, clock: clock)
        return (store, cursors, clock, dispatcher)
    }

    // MARK: Ordering and completeness

    func testEveryAdmittedTransactionIsDeliveredOnceInTokenOrderAcrossBudgets() async throws {
        for (maxTransactions, maxChanges) in [(1, 1), (3, 2), (7, 100), (64, 5)] {
            let (store, cursors, _, dispatcher) = makeFeed()
            var expected: [HistoryToken] = []
            for index in 0..<40 {
                let author: Author = index % 3 == 0 ? .sync : .user
                // Mixed sizes: some transactions touch several entities.
                let width = index % 4 + 1
                let ops = (0..<width).map { WriteOperation.patch(note("e\(index)-\($0)"), set: ["v": "\(index)"]) }
                if let transaction = await store.commit(author: author, ops), author != .sync {
                    expected.append(transaction.token)
                }
            }
            let consumer = RecordingConsumer("r", subscription: Subscription(authors: .excluding([.sync])))
            try await dispatcher.register(consumer, budget: BatchBudget(maxTransactions: maxTransactions, maxChanges: maxChanges))
            await dispatcher.drain()

            let delivered = await consumer.batches.flatMap { $0 }
            XCTAssertEqual(delivered, expected, "budget (\(maxTransactions), \(maxChanges))")
            let status = try await dispatcher.status(of: "r")
            XCTAssertEqual(status.cursor, HistoryToken(40))
            XCTAssertEqual(status.lag, 0)
            XCTAssertEqual(status.skippedTransactions, 40 - expected.count)
            let durable = await cursors.load("r")
            XCTAssertEqual(durable, HistoryToken(40))
        }
    }

    func testBatchesRespectTheChangeBudgetButAnOversizedTransactionStillProgresses() async throws {
        let (store, _, _, dispatcher) = makeFeed()
        await store.commit(author: .user, (0..<10).map { .put(note("big\($0)"), Record(["v": "1"])) })
        await commitEdits(store, count: 4)
        let consumer = RecordingConsumer("r")
        try await dispatcher.register(consumer, budget: BatchBudget(maxTransactions: 50, maxChanges: 3))

        let first = try await dispatcher.pump("r")
        XCTAssertEqual(first, .advanced(to: HistoryToken(1), applied: 1, skipped: 0, quarantined: 0),
                       "a 10-change transaction exceeds the budget of 3 but must not wedge the lane")
        let second = try await dispatcher.pump("r")
        XCTAssertEqual(second, .advanced(to: HistoryToken(4), applied: 3, skipped: 0, quarantined: 0))
        let batches = await consumer.batches
        XCTAssertEqual(batches.map(\.count), [1, 3])
    }

    func testFilteredOutTransactionsAdvanceTheCursorWithoutCallingApply() async throws {
        let (store, _, _, dispatcher) = makeFeed()
        await commitEdits(store, count: 5, author: .sync)
        let consumer = RecordingConsumer("outbox", subscription: Subscription(authors: .excluding([.sync])))
        try await dispatcher.register(consumer)

        let outcome = try await dispatcher.pump("outbox")
        XCTAssertEqual(outcome, .advanced(to: HistoryToken(5), applied: 0, skipped: 5, quarantined: 0))
        let batches = await consumer.batches
        XCTAssertTrue(batches.isEmpty)
        let next = try await dispatcher.pump("outbox")
        XCTAssertEqual(next, .caughtUp)
    }

    func testEntityTypeFilterTrimsChangesInsideATransaction() async throws {
        let (store, _, _, dispatcher) = makeFeed()
        await store.commit(author: .user, [
            .put(note("a"), Record(["title": "A"])),
            .put(EntityKey(type: "Tag", id: "t"), Record(["name": "x"]))
        ])
        let consumer = RecordingConsumer("notes", subscription: Subscription(entityTypes: ["Note"]))
        try await dispatcher.register(consumer, budget: BatchBudget(maxTransactions: 10, maxChanges: 1))
        let outcome = try await dispatcher.pump("notes")
        XCTAssertEqual(outcome, .advanced(to: HistoryToken(1), applied: 1, skipped: 0, quarantined: 0),
                       "only the one Note change counts against a change budget of 1")
    }

    // MARK: Failure isolation

    func testPoisonTransactionIsQuarantinedAloneAndTheRestOfTheBatchIsDelivered() async throws {
        let (store, _, _, dispatcher) = makeFeed()
        await commitEdits(store, count: 6)
        let poisoned = RecordingConsumer("index", permanentlyBad: [HistoryToken(3)])
        let healthy = RecordingConsumer("outbox")
        try await dispatcher.register(poisoned)
        try await dispatcher.register(healthy)

        let results = await dispatcher.pumpAll()
        XCTAssertEqual(results["index"], .advanced(to: HistoryToken(6), applied: 5, skipped: 0, quarantined: 1))
        XCTAssertEqual(results["outbox"], .advanced(to: HistoryToken(6), applied: 6, skipped: 0, quarantined: 0))

        let applied = await poisoned.applied
        XCTAssertEqual(applied.map(\.value), [1, 2, 4, 5, 6])
        let status = try await dispatcher.status(of: "index")
        XCTAssertEqual(status.deadLetters.map(\.token), [HistoryToken(3)])
        XCTAssertEqual(status.cursor, HistoryToken(6))
    }

    /// A transient failure *during* poison isolation must keep what was achieved and must
    /// not turn the transient failure into a dead letter (that would be silent loss).
    func testTransientFailureDuringIsolationIsRetriedNotQuarantined() async throws {
        let (store, cursors, clock, dispatcher) = makeFeed()
        await commitEdits(store, count: 1, prefix: "a")              // #1 user
        await commitEdits(store, count: 1, author: .sync, prefix: "b") // #2 sync (filtered out)
        await commitEdits(store, count: 2, prefix: "c")              // #3 user (bad), #4 user
        // apply calls: 1 = batch [1,3,4] (fails on 3), 2 = [1], 3 = [3] (permanent), 4 = [4] (transient)
        let consumer = RecordingConsumer(
            "r",
            subscription: Subscription(authors: .excluding([.sync])),
            permanentlyBad: [HistoryToken(3)],
            transientOnCall: 4
        )
        try await dispatcher.register(consumer)

        let first = try await dispatcher.pump("r")
        XCTAssertEqual(first, .failed(attempt: 1, retryAt: 1))
        var status = try await dispatcher.status(of: "r")
        XCTAssertEqual(status.cursor, HistoryToken(3), "progress up to the quarantined transaction is kept")
        XCTAssertEqual(status.deadLetters.map(\.token), [HistoryToken(3)], "only the permanent failure is a dead letter")
        XCTAssertEqual(status.skippedTransactions, 1, "the filtered #2 lies below the cursor and is counted")
        let durable = await cursors.load("r")
        XCTAssertEqual(durable, HistoryToken(3))

        clock.advance(by: 1)
        let second = try await dispatcher.pump("r")
        XCTAssertEqual(second, .advanced(to: HistoryToken(4), applied: 1, skipped: 0, quarantined: 0))
        status = try await dispatcher.status(of: "r")
        XCTAssertEqual(status.deadLetters.map(\.token), [HistoryToken(3)])
        XCTAssertEqual(status.skippedTransactions, 1)
        let applied = await consumer.applied
        XCTAssertEqual(applied.map(\.value), [1, 4])
    }

    func testTransientFailureBacksOffWithoutAdvancingAndThenDeliversTheSameBatch() async throws {
        let (store, _, clock, dispatcher) = makeFeed()
        await commitEdits(store, count: 3)
        let consumer = RecordingConsumer("r", transientFailures: 2)
        try await dispatcher.register(consumer, retry: RetryPolicy(baseDelay: 4, maxDelay: 100, stallAfter: 2))

        let first = try await dispatcher.pump("r")
        XCTAssertEqual(first, .failed(attempt: 1, retryAt: 4))
        let waiting = try await dispatcher.pump("r")
        XCTAssertEqual(waiting, .waiting(until: 4))
        var status = try await dispatcher.status(of: "r")
        XCTAssertEqual(status.cursor, .zero, "a transient failure must never advance the cursor")
        XCTAssertEqual(status.health, .backingOff(attempt: 1, until: 4))

        clock.advance(by: 4)
        let second = try await dispatcher.pump("r")
        XCTAssertEqual(second, .failed(attempt: 2, retryAt: 12), "delay doubles: 4 → 8")
        status = try await dispatcher.status(of: "r")
        XCTAssertEqual(status.health, .stalled(attempt: 2, until: 12))

        clock.advance(by: 8)
        let third = try await dispatcher.pump("r")
        XCTAssertEqual(third, .advanced(to: HistoryToken(3), applied: 3, skipped: 0, quarantined: 0))
        status = try await dispatcher.status(of: "r")
        XCTAssertEqual(status.health, .delivering)
        XCTAssertNil(status.lastError)
        let delivered = await consumer.applied
        XCTAssertEqual(delivered.map(\.value), [1, 2, 3])
    }

    func testUnknownErrorsAreTreatedAsTransientNotDropped() async throws {
        struct Weird: Error {}
        actor Throwing: ChangeConsumer {
            nonisolated let id: ConsumerID = "weird"
            nonisolated let subscription = Subscription()
            func apply(_ batch: [Transaction]) throws { throw Weird() }
            func rebuild(from snapshot: Snapshot) {}
        }
        let (store, _, _, dispatcher) = makeFeed()
        await commitEdits(store, count: 2)
        try await dispatcher.register(Throwing())
        let outcome = try await dispatcher.pump("weird")
        XCTAssertEqual(outcome, .failed(attempt: 1, retryAt: 1))
        let status = try await dispatcher.status(of: "weird")
        XCTAssertTrue(status.deadLetters.isEmpty, "an unclassified error must not quarantine data")
        XCTAssertEqual(status.cursor, .zero)
    }

    func testDeadLettersAreBoundedAndOverflowIsCounted() async throws {
        let (store, _, _, dispatcher) = makeFeed()
        let tokens = await commitEdits(store, count: 7)
        let consumer = RecordingConsumer("r", permanentlyBad: Set(tokens))
        try await dispatcher.register(consumer, retry: RetryPolicy(deadLetterCapacity: 3))
        await dispatcher.drain()
        let status = try await dispatcher.status(of: "r")
        XCTAssertEqual(status.deadLetters.map(\.token.value), [5, 6, 7])
        XCTAssertEqual(status.droppedDeadLetters, 4)
        XCTAssertEqual(status.cursor, HistoryToken(7))
    }

    // MARK: Concurrency

    func testSlowConsumerDoesNotHoldBackAnotherLane() async throws {
        let (store, _, _, dispatcher) = makeFeed()
        await commitEdits(store, count: 3)
        let gate = Gate()
        let slow = RecordingConsumer("slow", gate: gate)
        let fast = RecordingConsumer("fast")
        try await dispatcher.register(slow)
        try await dispatcher.register(fast)

        let slowStep = Task { try await dispatcher.pump("slow") }
        // Wait until the slow lane is genuinely suspended inside `apply`.
        while await gate.arrivals == 0 { await Task.yield() }

        let fastOutcome = try await dispatcher.pump("fast")
        XCTAssertEqual(fastOutcome, .advanced(to: HistoryToken(3), applied: 3, skipped: 0, quarantined: 0))
        let slowStatus = try await dispatcher.status(of: "slow")
        XCTAssertEqual(slowStatus.cursor, .zero, "slow lane is still mid-apply")

        // Reentrancy: a second step on the suspended lane must not start a duplicate delivery.
        // Run it in its own task so a regression shows up as a failed assertion, not a hang:
        // without the guard, the second step would also park inside `apply` on the gate.
        let finished = OutcomeBox()
        let reentrantStep = Task {
            let outcome = try await dispatcher.pump("slow")
            await finished.set(outcome)
            return outcome
        }
        var spins = 0
        while await finished.value == nil, await gate.arrivals < 2, spins < 100_000 {
            spins += 1
            await Task.yield()
        }
        let arrivals = await gate.arrivals
        XCTAssertEqual(arrivals, 1, "a concurrent step re-entered apply with the same cursor")
        let earlyOutcome = await finished.value
        XCTAssertEqual(earlyOutcome, .busy, "the concurrent step must return immediately, while the first is still suspended")

        await gate.open()
        _ = try await reentrantStep.value
        let slowOutcome = try await slowStep.value
        XCTAssertEqual(slowOutcome, .advanced(to: HistoryToken(3), applied: 3, skipped: 0, quarantined: 0))
        let batches = await slow.batches
        XCTAssertEqual(batches.count, 1, "exactly one delivery despite the concurrent step")
    }

    // MARK: Durability

    func testCrashBetweenApplyAndCursorSaveRedeliversAndIdempotentConsumerIgnoresIt() async throws {
        let store = InMemoryHistoryStore()
        let cursors = InMemoryCursorStore()
        await commitEdits(store, count: 4)

        let first = ChangeDispatcher(source: store, cursors: cursors, clock: ManualFeedClock())
        let consumer = RecordingConsumer("r")
        try await first.register(consumer)
        await cursors.failNextSaves(1)
        let outcome = try await first.pump("r")
        XCTAssertEqual(outcome, .advanced(to: HistoryToken(4), applied: 4, skipped: 0, quarantined: 0))
        let status = try await first.status(of: "r")
        XCTAssertFalse(status.cursorPersisted)
        let durable = await cursors.load("r")
        XCTAssertNil(durable, "the save failed: on disk, nothing was handled")

        // "Crash": a fresh dispatcher over the same durable cursor store.
        let restarted = ChangeDispatcher(source: store, cursors: cursors, clock: ManualFeedClock())
        try await restarted.register(consumer)
        let redelivered = try await restarted.pump("r")
        XCTAssertEqual(redelivered, .advanced(to: HistoryToken(4), applied: 4, skipped: 0, quarantined: 0))
        let batches = await consumer.batches
        XCTAssertEqual(batches.count, 2, "at-least-once: the batch was delivered twice")
        let applied = await consumer.applied
        XCTAssertEqual(applied.count, 4, "but applied once, because the consumer is idempotent per token")
    }

    func testFailedCursorSaveIsRetriedOnTheNextStep() async throws {
        let (store, cursors, _, dispatcher) = makeFeed()
        await commitEdits(store, count: 2)
        try await dispatcher.register(RecordingConsumer("r"))
        await cursors.failNextSaves(1)
        _ = try await dispatcher.pump("r")
        let next = try await dispatcher.pump("r")
        XCTAssertEqual(next, .caughtUp)
        let durable = await cursors.load("r")
        XCTAssertEqual(durable, HistoryToken(2))
        let status = try await dispatcher.status(of: "r")
        XCTAssertTrue(status.cursorPersisted)
    }

    func testCursorStoreNeverMovesBackwards() async throws {
        let cursors = InMemoryCursorStore()
        try await cursors.save(HistoryToken(9), for: "r")
        try await cursors.save(HistoryToken(3), for: "r")
        let value = await cursors.load("r")
        XCTAssertEqual(value, HistoryToken(9))
    }

    // MARK: Expiry, replay, start positions

    func testLaggingConsumerWhoseCursorWasPrunedRebuildsFromSnapshot() async throws {
        let (store, _, _, dispatcher) = makeFeed(retention: 5)
        await commitEdits(store, count: 3)
        let consumer = RecordingConsumer("r")
        try await dispatcher.register(consumer)
        _ = try await dispatcher.pump("r")
        await commitEdits(store, count: 10, prefix: "late")

        let outcome = try await dispatcher.pump("r")
        XCTAssertEqual(outcome, .rebuilt(at: HistoryToken(13)))
        let rebuilt = await consumer.rebuiltAt
        XCTAssertEqual(rebuilt, [HistoryToken(13)])
        let records = await consumer.rebuiltRecordCount
        XCTAssertEqual(records, [13])
        let status = try await dispatcher.status(of: "r")
        XCTAssertEqual(status.cursor, HistoryToken(13))
        XCTAssertEqual(status.rebuilds, 1)

        await commitEdits(store, count: 1, prefix: "after")
        let resumed = try await dispatcher.pump("r")
        XCTAssertEqual(resumed, .advanced(to: HistoryToken(14), applied: 1, skipped: 0, quarantined: 0))
    }

    func testFailedRebuildBacksOffAndRetries() async throws {
        let (store, _, clock, dispatcher) = makeFeed(retention: 2)
        await commitEdits(store, count: 6)
        let consumer = RecordingConsumer("r", rebuildFailures: 1)
        try await dispatcher.register(consumer, start: .snapshot)
        let first = try await dispatcher.pump("r")
        XCTAssertEqual(first, .failed(attempt: 1, retryAt: 1))
        clock.advance(by: 1)
        let second = try await dispatcher.pump("r")
        XCTAssertEqual(second, .rebuilt(at: HistoryToken(6)))
    }

    func testStartPositions() async throws {
        let (store, _, _, dispatcher) = makeFeed()
        await commitEdits(store, count: 4)
        let replay = RecordingConsumer("replay")
        let head = RecordingConsumer("head")
        let snapshot = RecordingConsumer("snap")
        try await dispatcher.register(replay, start: .replayAll)
        try await dispatcher.register(head, start: .fromHead)
        try await dispatcher.register(snapshot, start: .snapshot)
        await dispatcher.drain()

        let replayed = await replay.applied
        XCTAssertEqual(replayed.count, 4)
        let fromHead = await head.applied
        XCTAssertTrue(fromHead.isEmpty)
        let rebuilt = await snapshot.rebuiltAt
        XCTAssertEqual(rebuilt, [HistoryToken(4)])
        let snapApplied = await snapshot.applied
        XCTAssertTrue(snapApplied.isEmpty)

        await commitEdits(store, count: 1, prefix: "new")
        await dispatcher.drain()
        let headAfter = await head.applied
        XCTAssertEqual(headAfter, [HistoryToken(5)])
        let snapAfter = await snapshot.applied
        XCTAssertEqual(snapAfter, [HistoryToken(5)])
    }

    func testReplayAllFallsBackToRebuildWhenTheBeginningWasPruned() async throws {
        let (store, _, _, dispatcher) = makeFeed(retention: 2)
        await commitEdits(store, count: 5)
        let late = RecordingConsumer("late")
        try await dispatcher.register(late, start: .replayAll)
        let outcome = try await dispatcher.pump("late")
        XCTAssertEqual(outcome, .rebuilt(at: HistoryToken(5)))
    }

    func testStoredCursorWinsOverStartPosition() async throws {
        let store = InMemoryHistoryStore()
        await commitEdits(store, count: 5)
        let cursors = InMemoryCursorStore(initial: ["r": HistoryToken(3)])
        let dispatcher = ChangeDispatcher(source: store, cursors: cursors, clock: ManualFeedClock())
        let consumer = RecordingConsumer("r")
        try await dispatcher.register(consumer, start: .snapshot)
        let outcome = try await dispatcher.pump("r")
        XCTAssertEqual(outcome, .advanced(to: HistoryToken(5), applied: 2, skipped: 0, quarantined: 0))
    }

    // MARK: Registration

    func testDuplicateAndUnknownConsumersAreRejected() async throws {
        let (_, _, _, dispatcher) = makeFeed()
        try await dispatcher.register(RecordingConsumer("r"))
        do {
            try await dispatcher.register(RecordingConsumer("r"))
            XCTFail("expected duplicateConsumer")
        } catch let error as DispatcherError {
            XCTAssertEqual(error, .duplicateConsumer("r"))
        }
        do {
            _ = try await dispatcher.pump("nope")
            XCTFail("expected unknownConsumer")
        } catch let error as DispatcherError {
            XCTAssertEqual(error, .unknownConsumer("nope"))
        }
    }

    func testDrainIsBoundedAndEmptyFeedIsCaughtUp() async throws {
        let (_, _, _, dispatcher) = makeFeed()
        let none = await dispatcher.drain(maxRounds: 0)
        XCTAssertEqual(none, 0)
        let negative = await dispatcher.drain(maxRounds: -3)
        XCTAssertEqual(negative, 0)
        try await dispatcher.register(RecordingConsumer("r"))
        let outcome = try await dispatcher.pump("r")
        XCTAssertEqual(outcome, .caughtUp)
    }
}
