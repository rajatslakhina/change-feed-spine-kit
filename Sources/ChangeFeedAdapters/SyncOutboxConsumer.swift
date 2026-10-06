import ChangeFeed

/// One pending upload.
public struct OutboxEntry: Hashable, Sendable {
    public enum Payload: Hashable, Sendable {
        /// Upload this entity change.
        case change(Change)
        /// History needed for incremental upload was pruned: the server must be
        /// reconciled from a full snapshot taken at `token`.
        case fullResync(HistoryToken, entityCount: Int)
    }

    public let token: HistoryToken
    public let author: Author
    public let payload: Payload
}

/// Feature-owned adapter: queues local changes for upload to the server.
///
/// **Echo-loop prevention lives in the subscription, not here.** The default
/// subscription excludes `.sync`-authored transactions, so a change that *arrived* from
/// the server is never queued to be sent back to it. Construct this with
/// `Subscription(authors: .all)` and every pull re-uploads itself — `AdapterTests.testWithoutTheAuthorFilterTheEchoLoopNeverTerminates`
/// does exactly that to prove the filter is load-bearing.
public actor SyncOutboxConsumer: ChangeConsumer {
    public nonisolated let id: ConsumerID
    public nonisolated let subscription: Subscription

    /// Maximum queued entries. When full, `apply` throws a *transient* failure so the
    /// lane backs off without advancing — back-pressure reaches the history cursor
    /// instead of the outbox growing without bound.
    public let capacity: Int

    private var pending: [OutboxEntry] = []
    private var lastApplied: HistoryToken = .zero
    private var duplicates = 0

    public init(
        id: ConsumerID = "sync-outbox",
        subscription: Subscription = Subscription(authors: .excluding([.sync])),
        capacity: Int = 1_000
    ) {
        self.id = id
        self.subscription = subscription
        self.capacity = max(1, capacity)
    }

    public func apply(_ batch: [Transaction]) throws {
        for transaction in batch {
            guard transaction.token > lastApplied else {
                duplicates = SaturatingMath.add(duplicates, 1)
                continue
            }
            guard SaturatingMath.add(pending.count, transaction.changes.count) <= capacity || pending.isEmpty else {
                throw ConsumerFailure.transient("outbox full (\(pending.count)/\(capacity))")
            }
            for change in transaction.changes {
                pending.append(OutboxEntry(token: transaction.token, author: transaction.author, payload: .change(change)))
            }
            lastApplied = transaction.token
        }
    }

    public func rebuild(from snapshot: Snapshot) {
        pending = [OutboxEntry(
            token: snapshot.token,
            author: .migration,
            payload: .fullResync(snapshot.token, entityCount: snapshot.records.count)
        )]
        lastApplied = max(lastApplied, snapshot.token)
    }

    /// Removes and returns up to `limit` entries, oldest first (a simulated upload).
    public func takeForUpload(limit: Int = .max) -> [OutboxEntry] {
        let count = min(max(0, limit), pending.count)
        let taken = Array(pending.prefix(count))
        pending.removeFirst(count)
        return taken
    }

    public func pendingEntries() -> [OutboxEntry] {
        pending
    }

    /// Redelivered transactions ignored by the idempotency check.
    public func duplicatesIgnored() -> Int {
        duplicates
    }
}
