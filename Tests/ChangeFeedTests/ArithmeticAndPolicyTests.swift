import XCTest
@testable import ChangeFeed

final class ArithmeticAndPolicyTests: XCTestCase {
    func testSaturatingOperationsClampInsteadOfTrapping() {
        XCTAssertEqual(SaturatingMath.add(UInt64.max, 1), .max)
        XCTAssertEqual(SaturatingMath.add(UInt64(2), 3), 5)
        XCTAssertEqual(SaturatingMath.subtract(3, 10), 0)
        XCTAssertEqual(SaturatingMath.subtract(10, 3), 7)
        XCTAssertEqual(SaturatingMath.multiply(UInt64.max, 2), .max)
        XCTAssertEqual(SaturatingMath.multiply(6, 7), 42)
        XCTAssertEqual(SaturatingMath.add(Int.max, 1), .max)
        XCTAssertEqual(SaturatingMath.add(Int.min, -1), .min)
        XCTAssertEqual(SaturatingMath.add(-2, 5), 3)
        XCTAssertEqual(SaturatingMath.clampedInt(UInt64.max), Int.max)
        XCTAssertEqual(SaturatingMath.clampedInt(12), 12)
    }

    func testDoublingIsExactUntilItSaturates() {
        XCTAssertEqual(SaturatingMath.doubling(1, times: 0), 1)
        XCTAssertEqual(SaturatingMath.doubling(1, times: -4), 1)
        XCTAssertEqual(SaturatingMath.doubling(3, times: 4), 48)
        XCTAssertEqual(SaturatingMath.doubling(1, times: 63), UInt64(1) << 63, "the largest exact power of two")
        XCTAssertEqual(SaturatingMath.doubling(1, times: 64), .max)
        XCTAssertEqual(SaturatingMath.doubling(3, times: 63), .max)
        XCTAssertEqual(SaturatingMath.doubling(0, times: 1_000), 0)
        XCTAssertEqual(SaturatingMath.doubling(UInt64.max, times: 1), .max)
        XCTAssertEqual(SaturatingMath.doubling(5, times: Int.max), .max)
    }

    func testBackoffDoublesAndCapsForAnyAttempt() {
        let policy = RetryPolicy(baseDelay: 2, maxDelay: 50)
        XCTAssertEqual((1...6).map(policy.delay(forAttempt:)), [2, 4, 8, 16, 32, 50])
        XCTAssertEqual(policy.delay(forAttempt: 0), 2)
        XCTAssertEqual(policy.delay(forAttempt: -9), 2)
        XCTAssertEqual(policy.delay(forAttempt: 500), 50)
        XCTAssertEqual(policy.delay(forAttempt: Int.max), 50)
        XCTAssertEqual(policy.delay(forAttempt: Int.min), 2, "attempt - 1 would overflow here")
    }

    func testPoliciesClampNonsenseConfiguration() {
        let policy = RetryPolicy(baseDelay: 0, maxDelay: 0, stallAfter: -1, deadLetterCapacity: 0)
        XCTAssertEqual(policy.baseDelay, 1)
        XCTAssertEqual(policy.maxDelay, 1)
        XCTAssertEqual(policy.stallAfter, 1)
        XCTAssertEqual(policy.deadLetterCapacity, 1)
        let budget = BatchBudget(maxTransactions: 0, maxChanges: -3)
        XCTAssertEqual(budget.maxTransactions, 1)
        XCTAssertEqual(budget.maxChanges, 1)
    }

    func testTokenSuccessorSaturatesAndClockSaturates() {
        XCTAssertEqual(HistoryToken(UInt64.max).successor, HistoryToken(UInt64.max))
        XCTAssertEqual(HistoryToken(4).successor, HistoryToken(5))
        let clock = ManualFeedClock(start: UInt64.max - 1)
        clock.advance(by: 10)
        XCTAssertEqual(clock.now(), .max)
    }

    func testLagNeverUnderflowsWhenACursorIsAheadOfHead() async throws {
        // A cursor restored from a different (larger) store must not trap the lag computation.
        let store = InMemoryHistoryStore()
        let dispatcher = ChangeDispatcher(
            source: store,
            cursors: InMemoryCursorStore(initial: ["r": HistoryToken(50)]),
            clock: ManualFeedClock()
        )
        try await dispatcher.register(RecordingConsumer("r"))
        _ = try await dispatcher.pump("r")
        let status = try await dispatcher.status(of: "r")
        XCTAssertEqual(status.lag, 0)
    }

    func testAuthorFiltersAndSubscriptionFiltering() throws {
        XCTAssertTrue(AuthorFilter.all.admits(.sync))
        XCTAssertFalse(AuthorFilter.excluding([.sync]).admits(.sync))
        XCTAssertTrue(AuthorFilter.excluding([.sync]).admits(.agent("x")))
        XCTAssertTrue(AuthorFilter.only([.agent]).admits(.agent("x")))
        XCTAssertFalse(AuthorFilter.only([.agent]).admits(.user))
        XCTAssertEqual(Author.agent("a#1").description, "agent(a#1)")

        let transaction = Transaction(token: HistoryToken(1), author: .user, changes: [
            try XCTUnwrap(Change(key: note("a"), before: nil, after: Record(["x": "1"]))),
            try XCTUnwrap(Change(key: EntityKey(type: "Tag", id: "t"), before: nil, after: Record(["x": "1"])))
        ])
        XCTAssertEqual(Subscription(entityTypes: ["Note"]).filter(transaction)?.changes.count, 1)
        XCTAssertNil(Subscription(entityTypes: ["Folder"]).filter(transaction))
        XCTAssertEqual(Subscription().filter(transaction), transaction)
        XCTAssertNil(Change(key: note("a"), before: Record(["x": "1"]), after: Record(["x": "1"])), "identical images are not a change")
        XCTAssertNil(Change(key: note("a"), before: nil, after: nil))
    }
}
