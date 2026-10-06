/// One field's net change across an agent session.
public struct FieldDiff: Hashable, Sendable {
    public let field: String
    /// Value before the agent first touched it (`nil` = absent).
    public let before: String?
    /// Value the agent left behind (`nil` = absent).
    public let after: String?
}

/// The net effect of an agent session on one entity.
public struct AgentEntityChange: Hashable, Sendable, Identifiable {
    public enum Net: String, Hashable, Sendable {
        case insert, update, delete
    }

    public let key: EntityKey
    public let net: Net
    /// The entity as it would be with the agent's edits reverted (`nil` for an insert).
    public let original: Record?
    /// The entity as the agent left it (`nil` for a delete).
    public let final: Record?
    /// Only the fields the agent itself changed, sorted by name.
    public let fieldDiffs: [FieldDiff]

    public var id: EntityKey { key }
}

/// "What did the agent change?" — computed from the history stream alone.
///
/// Because the agent is a first-class transaction author, its session is just a filter
/// over the same history every other consumer reads. No second audit log, no
/// instrumentation inside the agent's tools.
public struct AgentSessionReport: Hashable, Sendable {
    public let session: String
    public let tokens: [HistoryToken]
    public let changes: [AgentEntityChange]
}

/// Why part of an undo cannot be applied safely.
public struct UndoConflict: Hashable, Sendable {
    public enum Reason: Hashable, Sendable {
        /// Someone else changed this field after the agent did.
        case fieldChangedSince(agentValue: String?, currentValue: String?)
        /// The agent edited this entity, and it has since been deleted.
        case deletedSince
        /// The agent created this entity, and it has since been edited.
        case modifiedSinceInsert
        /// The agent deleted this entity, and it has since been recreated.
        case recreatedSince
    }

    public let key: EntityKey
    public let field: String?
    public let reason: Reason
}

/// A compensating transaction for an agent session, checked against current state.
///
/// The safety property: an undo may only change a field that *still holds the value the
/// agent wrote*. Anything else is somebody's later edit, and reverting the agent must
/// not revert them. `UndoVerifier` checks exactly that property for any set of operations.
public struct UndoPlan: Hashable, Sendable {
    public let session: String
    public let operations: [WriteOperation]
    public let conflicts: [UndoConflict]
    /// Entities whose agent changes are already fully reverted (nothing to do).
    public let alreadyReverted: [EntityKey]

    public var isComplete: Bool { conflicts.isEmpty }
}

public enum AgentAuditError: Error, Sendable, Equatable {
    /// Part of the requested range has been pruned, so the session cannot be audited in full.
    case historyPruned(prunedThrough: HistoryToken)
    /// The scan exceeded `maxTransactions` (a guard against an unbounded read).
    case scanLimitExceeded(limit: Int)
}

public enum AgentAudit {
    /// Builds the report for `session` by paging through `source` after `cursor`.
    ///
    /// Refuses to produce a partial report when history has been pruned: an incomplete
    /// "what did the agent change" answer is worse than none, because it reads as complete.
    public static func report(
        session: String,
        source: any HistorySource,
        after cursor: HistoryToken = .zero,
        pageSize: Int = 256,
        maxTransactions: Int = 100_000
    ) async throws -> AgentSessionReport {
        let size = max(1, pageSize)
        let limit = max(0, maxTransactions)
        var position = cursor
        var scanned = 0
        var agentTransactions: [Transaction] = []
        while true {
            let page: [Transaction]
            do {
                page = try await source.transactions(after: position, limit: size)
            } catch HistoryError.cursorExpired(_, let prunedThrough) {
                throw AgentAuditError.historyPruned(prunedThrough: prunedThrough)
            }
            guard let last = page.last else { break }
            scanned = SaturatingMath.add(scanned, page.count)
            if scanned > limit { throw AgentAuditError.scanLimitExceeded(limit: limit) }
            agentTransactions.append(contentsOf: page.filter { $0.author == .agent(session) })
            position = last.token
        }
        return report(session: session, transactions: agentTransactions)
    }

    /// Pure form: folds the given transactions (others' authors are ignored).
    public static func report(session: String, transactions: [Transaction]) -> AgentSessionReport {
        struct Accumulator {
            var firstBefore: Record?
            var lastBefore: Record?
            var lastAfter: Record?
            var originals: [String: String?] = [:]
            var finals: [String: String?] = [:]
        }

        var order: [EntityKey] = []
        var accumulators: [EntityKey: Accumulator] = [:]
        var tokens: [HistoryToken] = []

        for transaction in transactions where transaction.author == .agent(session) {
            tokens.append(transaction.token)
            for change in transaction.changes {
                var accumulator: Accumulator
                if let existing = accumulators[change.key] {
                    accumulator = existing
                } else {
                    accumulator = Accumulator(firstBefore: change.before)
                    order.append(change.key)
                }
                for field in change.changedFields {
                    let valueBefore = change.before?[field]
                    if !accumulator.originals.keys.contains(field) {
                        accumulator.originals[field] = .some(valueBefore)
                    } else if let agentLast = accumulator.finals[field], agentLast != valueBefore {
                        // Someone else wrote this field between two agent writes. That write
                        // is theirs, not the agent's: the agent's net contribution now starts
                        // from it, so an undo restores their value instead of erasing it.
                        accumulator.originals[field] = .some(valueBefore)
                    }
                    accumulator.finals[field] = .some(change.after?[field])
                }
                accumulator.lastBefore = change.before
                accumulator.lastAfter = change.after
                accumulators[change.key] = accumulator
            }
        }

        var changes: [AgentEntityChange] = []
        for key in order.sorted() {
            guard let accumulator = accumulators[key] else { continue }
            let existedBefore = accumulator.firstBefore != nil
            let existsAfter = accumulator.lastAfter != nil
            switch (existedBefore, existsAfter) {
            case (false, false):
                continue // created and deleted within the session: no net effect
            case (false, true):
                let final = accumulator.lastAfter ?? Record()
                let diffs = final.fields.keys.sorted().map { FieldDiff(field: $0, before: nil, after: final[$0]) }
                changes.append(AgentEntityChange(key: key, net: .insert, original: nil, final: final, fieldDiffs: diffs))
            case (true, false):
                // Restore = the record just before deletion, with the agent's own edits rolled back.
                var restored = accumulator.lastBefore ?? Record()
                for (field, original) in accumulator.originals {
                    restored[field] = original
                }
                let diffs = restored.fields.keys.sorted().map { FieldDiff(field: $0, before: restored[$0], after: nil) }
                changes.append(AgentEntityChange(key: key, net: .delete, original: restored, final: nil, fieldDiffs: diffs))
            case (true, true):
                var diffs: [FieldDiff] = []
                for field in accumulator.originals.keys.sorted() {
                    let before: String? = accumulator.originals[field] ?? nil
                    let after: String? = accumulator.finals[field] ?? nil
                    if before != after {
                        diffs.append(FieldDiff(field: field, before: before, after: after))
                    }
                }
                guard !diffs.isEmpty else { continue }
                var original = accumulator.lastAfter ?? Record()
                for diff in diffs { original[diff.field] = diff.before }
                changes.append(AgentEntityChange(key: key, net: .update, original: original, final: accumulator.lastAfter, fieldDiffs: diffs))
            }
        }
        return AgentSessionReport(session: session, tokens: tokens, changes: changes)
    }

    /// Plans the compensating transaction for `report` against `current` state.
    /// Non-conflicting parts are planned even when some parts conflict; the caller
    /// decides whether a partial undo is acceptable (`UndoPlan.isComplete`).
    public static func undoPlan(for report: AgentSessionReport, current: Snapshot) -> UndoPlan {
        var operations: [WriteOperation] = []
        var conflicts: [UndoConflict] = []
        var alreadyReverted: [EntityKey] = []

        for change in report.changes {
            let live = current.records[change.key]
            switch change.net {
            case .insert:
                guard let live else {
                    alreadyReverted.append(change.key)
                    continue
                }
                if live == change.final {
                    operations.append(.delete(change.key))
                } else {
                    conflicts.append(UndoConflict(key: change.key, field: nil, reason: .modifiedSinceInsert))
                }
            case .delete:
                guard let live else {
                    if let original = change.original {
                        operations.append(.put(change.key, original))
                    }
                    continue
                }
                if live == change.original {
                    alreadyReverted.append(change.key)
                } else {
                    conflicts.append(UndoConflict(key: change.key, field: nil, reason: .recreatedSince))
                }
            case .update:
                guard let live else {
                    conflicts.append(UndoConflict(key: change.key, field: nil, reason: .deletedSince))
                    continue
                }
                var set: [String: String] = [:]
                var remove: Set<String> = []
                var reverted = 0
                for diff in change.fieldDiffs {
                    let currentValue = live[diff.field]
                    if currentValue == diff.after {
                        if let restore = diff.before {
                            set[diff.field] = restore
                        } else {
                            remove.insert(diff.field)
                        }
                    } else if currentValue == diff.before {
                        reverted += 1
                    } else {
                        conflicts.append(UndoConflict(
                            key: change.key,
                            field: diff.field,
                            reason: .fieldChangedSince(agentValue: diff.after, currentValue: currentValue)
                        ))
                    }
                }
                if !set.isEmpty || !remove.isEmpty {
                    operations.append(.patch(change.key, set: set, remove: remove))
                } else if reverted == change.fieldDiffs.count {
                    alreadyReverted.append(change.key)
                }
            }
        }
        return UndoPlan(session: report.session, operations: operations, conflicts: conflicts, alreadyReverted: alreadyReverted)
    }
}

/// Checks the undo safety property for *any* set of operations, independently of how
/// they were produced: an operation may change a field only if the agent session changed
/// that field and the field still holds the agent's value.
public enum UndoVerifier {
    /// Returns every (entity, field) the operations would overwrite that is not the
    /// agent's to revert. Empty means the operations are safe.
    public static func foreignOverwrites(
        operations: [WriteOperation],
        report: AgentSessionReport,
        current: Snapshot
    ) -> [EntityKey: Set<String>] {
        var byKey: [EntityKey: AgentEntityChange] = [:]
        for change in report.changes { byKey[change.key] = change }

        var working = current.records
        var violations: [EntityKey: Set<String>] = [:]
        for operation in operations {
            let key = operation.key
            let before = working[key]
            let after: Record?
            switch operation {
            case .put(_, let record):
                after = record
            case .patch(_, let set, let remove):
                var record = before ?? Record()
                for (field, value) in set { record[field] = value }
                for field in remove { record[field] = nil }
                after = record
            case .delete:
                after = nil
            }
            working[key] = after

            guard let changed = Change(key: key, before: before, after: after)?.changedFields else { continue }
            let agentChange = byKey[key]
            for field in changed {
                let agentTouched = agentChange?.fieldDiffs.first { $0.field == field }
                let isAgentsToRevert: Bool
                if let agentTouched {
                    isAgentsToRevert = before?[field] == agentTouched.after
                } else {
                    isAgentsToRevert = false
                }
                if !isAgentsToRevert {
                    violations[key, default: []].insert(field)
                }
            }
        }
        return violations
    }
}
