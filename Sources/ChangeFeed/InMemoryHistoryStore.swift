/// A write against the store. Several writes commit atomically as one transaction.
public enum WriteOperation: Hashable, Sendable {
    /// Create the entity, or replace every field of an existing one.
    case put(EntityKey, Record)
    /// Set and/or remove individual fields. Creates the entity if it does not exist.
    case patch(EntityKey, set: [String: String], remove: Set<String> = [])
    /// Delete the entity. Deleting a missing entity is a no-op.
    case delete(EntityKey)

    public var key: EntityKey {
        switch self {
        case .put(let key, _), .patch(let key, _, _), .delete(let key):
            return key
        }
    }
}

/// A reference `HistorySource` with SwiftData-like persistent-history semantics:
/// author-tagged atomic transactions, before/after images, monotonic tokens, and a
/// bounded retention window that prunes old history.
///
/// Retention is deliberately bounded. Real persistent history must be pruned or it
/// grows without limit, and pruning is exactly what forces the feed to handle
/// cursor expiry — so the reference store makes it impossible to forget.
public actor InMemoryHistoryStore: HistorySource {
    private var log: [Transaction] = []
    private var records: [EntityKey: Record] = [:]
    private var lastToken: UInt64 = 0
    private var prunedThrough: UInt64 = 0

    /// Maximum number of transactions retained. Always at least 1.
    public nonisolated let retentionLimit: Int

    public init(retentionLimit: Int = 10_000) {
        self.retentionLimit = max(1, retentionLimit)
    }

    // MARK: - Writes

    /// Applies `operations` atomically under `author`.
    ///
    /// Operations touching the same entity are coalesced into one `Change` whose
    /// before-image is the state before the transaction and whose after-image is the
    /// state after it. Returns `nil` (and assigns no token) when nothing changed —
    /// a no-op save must not wake every consumer.
    @discardableResult
    public func commit(author: Author, _ operations: [WriteOperation]) -> Transaction? {
        guard !operations.isEmpty else { return nil }

        var working: [EntityKey: Record?] = [:]
        var order: [EntityKey] = []
        var originals: [EntityKey: Record?] = [:]

        for operation in operations {
            let key = operation.key
            if !originals.keys.contains(key) {
                originals[key] = .some(records[key])
                order.append(key)
            }
            let current: Record? = working[key] ?? records[key]
            let next: Record?
            switch operation {
            case .put(_, let record):
                next = record
            case .patch(_, let set, let remove):
                var record = current ?? Record()
                for (field, value) in set { record[field] = value }
                for field in remove { record[field] = nil }
                next = record
            case .delete:
                next = nil
            }
            working[key] = .some(next)
        }

        var changes: [Change] = []
        for key in order {
            let before: Record? = originals[key] ?? nil
            let after: Record? = working[key] ?? nil
            if let change = Change(key: key, before: before, after: after) {
                changes.append(change)
            }
        }
        guard !changes.isEmpty else { return nil }

        for change in changes {
            records[change.key] = change.after
        }
        lastToken = SaturatingMath.add(lastToken, 1)
        let transaction = Transaction(token: HistoryToken(lastToken), author: author, changes: changes)
        log.append(transaction)
        enforceRetention()
        return transaction
    }

    /// Prunes every transaction with `token <= through`, as a history-cleanup job would.
    public func prune(through token: HistoryToken) {
        let bound = min(token.value, lastToken)
        guard bound > prunedThrough else { return }
        let drop = firstIndex(after: HistoryToken(bound))
        log.removeFirst(drop)
        prunedThrough = bound
    }

    // MARK: - HistorySource

    public func head() -> HistoryToken {
        HistoryToken(lastToken)
    }

    public func transactions(after cursor: HistoryToken, limit: Int) throws -> [Transaction] {
        guard limit > 0 else { return [] }
        guard cursor.value >= prunedThrough else {
            throw HistoryError.cursorExpired(cursor: cursor, prunedThrough: HistoryToken(prunedThrough))
        }
        let start = firstIndex(after: cursor)
        guard start < log.count else { return [] }
        let end = start + min(limit, log.count - start)
        return Array(log[start..<end])
    }

    public func snapshot() -> Snapshot {
        Snapshot(token: HistoryToken(lastToken), records: records)
    }

    // MARK: - Introspection

    /// The oldest pruned token (`zero` if nothing has been pruned).
    public func retentionHorizon() -> HistoryToken {
        HistoryToken(prunedThrough)
    }

    public func retainedCount() -> Int {
        log.count
    }

    public func record(for key: EntityKey) -> Record? {
        records[key]
    }

    // MARK: - Private

    private func enforceRetention() {
        let excess = log.count - retentionLimit
        guard excess > 0, excess <= log.count else { return }
        let newHorizon = log[excess - 1].token.value
        log.removeFirst(excess)
        prunedThrough = max(prunedThrough, newHorizon)
    }

    /// Index of the first retained transaction with `token > cursor` (binary search;
    /// `log` is sorted by token). Returns `log.count` if there is none.
    private func firstIndex(after cursor: HistoryToken) -> Int {
        var low = 0
        var high = log.count
        while low < high {
            let mid = low + (high - low) / 2
            if log[mid].token <= cursor {
                low = mid + 1
            } else {
                high = mid
            }
        }
        return low
    }
}
