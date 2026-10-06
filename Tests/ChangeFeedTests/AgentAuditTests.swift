import XCTest
@testable import ChangeFeed

final class AgentAuditTests: XCTestCase {
    private let session = "assistant#7"

    /// Seeds three notes, then an agent session that edits one, creates one, deletes one,
    /// with a user edit interleaved in the middle of the session.
    private func seededStore() async -> InMemoryHistoryStore {
        let store = InMemoryHistoryStore()
        await store.commit(author: .user, [
            .put(note("groceries"), Record(["title": "Groceries", "body": "milk", "pinned": "no"])),
            .put(note("trip"), Record(["title": "Trip", "body": "Lisbon"])),
            .put(note("old"), Record(["title": "Old", "body": "stale"]))
        ])
        await store.commit(author: .agent(session), [
            .patch(note("groceries"), set: ["body": "milk, eggs", "pinned": "yes"]),
            .put(note("summary"), Record(["title": "Weekly summary", "body": "3 notes"]))
        ])
        // The user edits a field the agent did not touch, on the same note, mid-session.
        await store.commit(author: .user, [.patch(note("groceries"), set: ["title": "Groceries!"])])
        await store.commit(author: .agent(session), [
            .delete(note("old")),
            .patch(note("groceries"), set: ["body": "milk, eggs, bread"])
        ])
        // A different agent session must not be attributed to this one.
        await store.commit(author: .agent("other#1"), [.patch(note("trip"), set: ["body": "Porto"])])
        return store
    }

    func testReportShowsOnlyThisSessionsNetFieldChanges() async throws {
        let store = await seededStore()
        let report = try await AgentAudit.report(session: session, source: store, pageSize: 2)

        XCTAssertEqual(report.tokens.map(\.value), [2, 4])
        XCTAssertEqual(report.changes.map(\.key), [note("groceries"), note("old"), note("summary")])

        let groceries = try XCTUnwrap(report.changes.first { $0.key == note("groceries") })
        XCTAssertEqual(groceries.net, .update)
        XCTAssertEqual(groceries.fieldDiffs, [
            FieldDiff(field: "body", before: "milk", after: "milk, eggs, bread"),
            FieldDiff(field: "pinned", before: "no", after: "yes")
        ], "the user's interleaved title edit is not the agent's change")
        XCTAssertEqual(groceries.original?["title"], "Groceries!", "reverting the agent keeps the user's title")

        let old = try XCTUnwrap(report.changes.first { $0.key == note("old") })
        XCTAssertEqual(old.net, .delete)
        XCTAssertEqual(old.original, Record(["title": "Old", "body": "stale"]))

        let summary = try XCTUnwrap(report.changes.first { $0.key == note("summary") })
        XCTAssertEqual(summary.net, .insert)
        XCTAssertNil(summary.original)
    }

    func testCleanUndoRevertsExactlyTheAgentAndPassesTheVerifier() async throws {
        let store = await seededStore()
        let report = try await AgentAudit.report(session: session, source: store)
        let plan = AgentAudit.undoPlan(for: report, current: await store.snapshot())
        XCTAssertTrue(plan.isComplete)
        let violations = UndoVerifier.foreignOverwrites(operations: plan.operations, report: report, current: await store.snapshot())
        XCTAssertTrue(violations.isEmpty)

        await store.commit(author: .user, plan.operations)
        let groceries = await store.record(for: note("groceries"))
        XCTAssertEqual(groceries, Record(["title": "Groceries!", "body": "milk", "pinned": "no"]))
        let old = await store.record(for: note("old"))
        XCTAssertEqual(old, Record(["title": "Old", "body": "stale"]))
        let summary = await store.record(for: note("summary"))
        XCTAssertNil(summary)
        let trip = await store.record(for: note("trip"))
        XCTAssertEqual(trip?["body"], "Porto", "another session's work is untouched")

        // Undoing twice is a no-op: everything is already reverted.
        let again = AgentAudit.undoPlan(for: report, current: await store.snapshot())
        XCTAssertTrue(again.operations.isEmpty)
        XCTAssertEqual(Set(again.alreadyReverted), [note("groceries"), note("old"), note("summary")])
        XCTAssertTrue(again.conflicts.isEmpty, "a completed undo is not a conflict")

        // But a note the agent deleted that was recreated *differently* afterwards is a conflict.
        await store.commit(author: .user, [.put(note("old"), Record(["title": "Old", "body": "rewritten by me"]))])
        let recreated = AgentAudit.undoPlan(for: report, current: await store.snapshot())
        XCTAssertEqual(recreated.conflicts.map(\.reason), [.recreatedSince])
    }

    func testUndoReportsConflictsForEditsMadeAfterTheAgentAndStillRevertsTheRest() async throws {
        let store = await seededStore()
        let report = try await AgentAudit.report(session: session, source: store)
        // After the session: the user rewrites the agent's body text and edits the agent-created note.
        await store.commit(author: .user, [
            .patch(note("groceries"), set: ["body": "milk only"]),
            .patch(note("summary"), set: ["body": "edited by me"])
        ])

        let current = await store.snapshot()
        let plan = AgentAudit.undoPlan(for: report, current: current)
        XCTAssertFalse(plan.isComplete)
        XCTAssertEqual(Set(plan.conflicts), [
            UndoConflict(key: note("groceries"), field: "body", reason: .fieldChangedSince(agentValue: "milk, eggs, bread", currentValue: "milk only")),
            UndoConflict(key: note("summary"), field: nil, reason: .modifiedSinceInsert)
        ])
        XCTAssertTrue(plan.operations.contains(.patch(note("groceries"), set: ["pinned": "no"], remove: [])),
                      "the non-conflicting field is still reverted")
        XCTAssertTrue(UndoVerifier.foreignOverwrites(operations: plan.operations, report: report, current: current).isEmpty)
    }

    /// Falsification of the safety property: a naive undo that restores each entity's
    /// before-image wholesale. The verifier must flag the user's later edits it destroys.
    func testVerifierCatchesANaiveWholesaleRestore() async throws {
        let store = await seededStore()
        let report = try await AgentAudit.report(session: session, source: store)
        await store.commit(author: .user, [.patch(note("groceries"), set: ["body": "milk only"])])
        let current = await store.snapshot()

        let naive: [WriteOperation] = report.changes.map { change in
            if let original = change.original { return .put(change.key, original) }
            return .delete(change.key)
        }
        let violations = UndoVerifier.foreignOverwrites(operations: naive, report: report, current: current)
        XCTAssertEqual(violations[note("groceries")], ["body"], "naive restore clobbers the user's post-session body edit")

        // Restoring a pre-session snapshot of the record (ignoring the interleaved user title edit)
        // is also caught: the title is not the agent's to revert.
        let stale: [WriteOperation] = [.put(note("groceries"), Record(["title": "Groceries", "body": "milk", "pinned": "no"]))]
        let staleViolations = UndoVerifier.foreignOverwrites(operations: stale, report: report, current: current)
        XCTAssertEqual(staleViolations[note("groceries")], ["title", "body"])
    }

    /// A user edit to the *same field*, between two agent writes, belongs to the user.
    /// Undo must restore the user's value, not the value from before the session.
    func testUndoPreservesAUserWriteInterleavedOnTheSameField() async throws {
        let store = InMemoryHistoryStore()
        await store.commit(author: .user, [.put(note("list"), Record(["body": "milk"]))])
        await store.commit(author: .agent("s"), [.patch(note("list"), set: ["body": "milk, eggs"])])
        await store.commit(author: .user, [.patch(note("list"), set: ["body": "oat milk"])])
        await store.commit(author: .agent("s"), [.patch(note("list"), set: ["body": "oat milk, bread"])])

        let report = try await AgentAudit.report(session: "s", source: store)
        XCTAssertEqual(report.changes.first?.fieldDiffs, [FieldDiff(field: "body", before: "oat milk", after: "oat milk, bread")])
        let current = await store.snapshot()
        let plan = AgentAudit.undoPlan(for: report, current: current)
        XCTAssertEqual(plan.operations, [.patch(note("list"), set: ["body": "oat milk"], remove: [])])
        await store.commit(author: .user, plan.operations)
        let after = await store.record(for: note("list"))
        XCTAssertEqual(after?["body"], "oat milk", "the user's mid-session edit survives the undo")
    }

    func testAgentEditOfADeletedEntityConflicts() async throws {
        let store = await seededStore()
        let report = try await AgentAudit.report(session: session, source: store)
        await store.commit(author: .user, [.delete(note("groceries"))])
        let plan = AgentAudit.undoPlan(for: report, current: await store.snapshot())
        XCTAssertTrue(plan.conflicts.contains(UndoConflict(key: note("groceries"), field: nil, reason: .deletedSince)))
    }

    func testCreateThenDeleteWithinSessionHasNoNetEffect() {
        let key = note("temp")
        let transactions = [
            Transaction(token: HistoryToken(1), author: .agent("s"), changes: [Change(key: key, before: nil, after: Record(["a": "1"]))].compactMap { $0 }),
            Transaction(token: HistoryToken(2), author: .agent("s"), changes: [Change(key: key, before: Record(["a": "1"]), after: nil)].compactMap { $0 })
        ]
        let report = AgentAudit.report(session: "s", transactions: transactions)
        XCTAssertEqual(report.tokens.count, 2)
        XCTAssertTrue(report.changes.isEmpty)
    }

    func testEmptySessionAndPrunedHistory() async throws {
        let store = InMemoryHistoryStore(retentionLimit: 2)
        let empty = try await AgentAudit.report(session: "nobody", source: store)
        XCTAssertTrue(empty.changes.isEmpty)
        let emptySnapshot = await store.snapshot()
        XCTAssertTrue(AgentAudit.undoPlan(for: empty, current: emptySnapshot).isComplete)

        await commitEdits(store, count: 5, author: .agent("s"))
        do {
            _ = try await AgentAudit.report(session: "s", source: store)
            XCTFail("a partial audit must not be returned as if complete")
        } catch let error as AgentAuditError {
            XCTAssertEqual(error, .historyPruned(prunedThrough: HistoryToken(3)))
        }
    }

    func testScanLimitBoundsTheRead() async throws {
        let store = InMemoryHistoryStore()
        await commitEdits(store, count: 10)
        do {
            _ = try await AgentAudit.report(session: "s", source: store, pageSize: 0, maxTransactions: 4)
            XCTFail("expected scanLimitExceeded")
        } catch let error as AgentAuditError {
            XCTAssertEqual(error, .scanLimitExceeded(limit: 4))
        }
    }
}
