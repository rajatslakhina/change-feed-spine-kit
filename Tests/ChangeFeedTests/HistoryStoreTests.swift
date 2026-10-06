import XCTest
@testable import ChangeFeed

final class HistoryStoreTests: XCTestCase {
    func testTokensAreContiguousAndAssignedOnlyToEffectiveCommits() async throws {
        let store = InMemoryHistoryStore()
        let first = await store.commit(author: .user, [.put(note("a"), Record(["title": "A"]))])
        let noOp = await store.commit(author: .user, [.patch(note("a"), set: ["title": "A"])])
        let empty = await store.commit(author: .user, [])
        let deleteMissing = await store.commit(author: .user, [.delete(note("zzz"))])
        let second = await store.commit(author: .agent("s1"), [.patch(note("a"), set: ["title": "B"])])

        XCTAssertEqual(first?.token, HistoryToken(1))
        XCTAssertNil(noOp, "a save that changes nothing must not wake consumers")
        XCTAssertNil(empty)
        XCTAssertNil(deleteMissing)
        XCTAssertEqual(second?.token, HistoryToken(2))
        let head = await store.head()
        XCTAssertEqual(head, HistoryToken(2))
    }

    func testOperationsOnOneEntityCoalesceIntoOneChangeWithTransactionLevelImages() async throws {
        let store = InMemoryHistoryStore()
        await store.commit(author: .user, [.put(note("a"), Record(["title": "A", "body": "x"]))])
        let committed = await store.commit(author: .user, [
            .patch(note("a"), set: ["title": "B"]),
            .patch(note("a"), set: ["title": "C"], remove: ["body"]),
            .put(note("b"), Record(["title": "new"])),
            .delete(note("b"))
        ])
        let transaction = try XCTUnwrap(committed)
        // note("b") was created and deleted inside the transaction: no net change at all.
        XCTAssertEqual(transaction.changes.count, 1)
        let change = try XCTUnwrap(transaction.changes.first)
        XCTAssertEqual(change.before, Record(["title": "A", "body": "x"]))
        XCTAssertEqual(change.after, Record(["title": "C"]))
        XCTAssertEqual(change.kind, .update)
        XCTAssertEqual(change.changedFields, ["title", "body"])
    }

    func testPagingRespectsLimitAndCursor() async throws {
        let store = InMemoryHistoryStore()
        await commitEdits(store, count: 10)
        let page = try await store.transactions(after: HistoryToken(3), limit: 4)
        XCTAssertEqual(page.map(\.token.value), [4, 5, 6, 7])
        let zero = try await store.transactions(after: .zero, limit: 0)
        XCTAssertTrue(zero.isEmpty)
        let negative = try await store.transactions(after: .zero, limit: -5)
        XCTAssertTrue(negative.isEmpty)
        let atHead = try await store.transactions(after: HistoryToken(10), limit: 10)
        XCTAssertTrue(atHead.isEmpty)
        let beyondHead = try await store.transactions(after: HistoryToken(UInt64.max), limit: 10)
        XCTAssertTrue(beyondHead.isEmpty)
    }

    func testReadingBehindTheRetentionHorizonThrowsCursorExpired() async throws {
        let store = InMemoryHistoryStore(retentionLimit: 3)
        await commitEdits(store, count: 8)
        let horizon = await store.retentionHorizon()
        XCTAssertEqual(horizon, HistoryToken(5))
        let retained = await store.retainedCount()
        XCTAssertEqual(retained, 3)

        do {
            _ = try await store.transactions(after: HistoryToken(4), limit: 10)
            XCTFail("expected cursorExpired")
        } catch let error as HistoryError {
            XCTAssertEqual(error, .cursorExpired(cursor: HistoryToken(4), prunedThrough: HistoryToken(5)))
        }
        // Exactly at the horizon is still readable: everything after it is retained.
        let page = try await store.transactions(after: HistoryToken(5), limit: 10)
        XCTAssertEqual(page.map(\.token.value), [6, 7, 8])
    }

    func testExplicitPruneIsClampedToHeadAndNeverMovesBackwards() async throws {
        let store = InMemoryHistoryStore()
        await commitEdits(store, count: 4)
        await store.prune(through: HistoryToken(UInt64.max))
        let horizon = await store.retentionHorizon()
        XCTAssertEqual(horizon, HistoryToken(4))
        await store.prune(through: HistoryToken(1))
        let unchanged = await store.retentionHorizon()
        XCTAssertEqual(unchanged, HistoryToken(4))
        let retained = await store.retainedCount()
        XCTAssertEqual(retained, 0)
    }

    func testSnapshotIsConsistentWithItsToken() async throws {
        let store = InMemoryHistoryStore()
        await store.commit(author: .user, [.put(note("a"), Record(["title": "A"]))])
        await store.commit(author: .user, [.put(note("b"), Record(["title": "B"]))])
        await store.commit(author: .user, [.delete(note("a"))])
        let snapshot = await store.snapshot()
        XCTAssertEqual(snapshot.token, HistoryToken(3))
        XCTAssertEqual(snapshot.records, [note("b"): Record(["title": "B"])])
    }

    func testRetentionLimitIsClampedToAtLeastOne() async {
        let store = InMemoryHistoryStore(retentionLimit: -10)
        XCTAssertEqual(store.retentionLimit, 1)
        await commitEdits(store, count: 3)
        let retained = await store.retainedCount()
        XCTAssertEqual(retained, 1)
    }
}
