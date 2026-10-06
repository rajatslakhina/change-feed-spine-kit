/// Stable identity of a consumer. Used as the key for its durable cursor, so renaming a
/// consumer is a migration (it would replay or rebuild), not a refactor.
public struct ConsumerID: Hashable, Comparable, Sendable, Codable, CustomStringConvertible, ExpressibleByStringLiteral {
    public let rawValue: String

    public init(_ rawValue: String) {
        self.rawValue = rawValue
    }

    public init(stringLiteral value: String) {
        self.rawValue = value
    }

    public static func < (lhs: ConsumerID, rhs: ConsumerID) -> Bool {
        lhs.rawValue < rhs.rawValue
    }

    public var description: String { rawValue }
}

/// Which authors a consumer wants to see.
public enum AuthorFilter: Hashable, Sendable {
    case all
    case only(Set<Author.Kind>)
    case excluding(Set<Author.Kind>)

    public func admits(_ author: Author) -> Bool {
        switch self {
        case .all: return true
        case .only(let kinds): return kinds.contains(author.kind)
        case .excluding(let kinds): return !kinds.contains(author.kind)
        }
    }
}

/// What a consumer subscribes to: an author filter plus an optional entity-type filter.
///
/// Filtering happens in the lane, not in the consumer, for one reason: a transaction
/// that is filtered out must still *advance the cursor*. A consumer that filters inside
/// `apply` and forgets that detail re-reads the same skipped transactions forever.
public struct Subscription: Hashable, Sendable {
    public var authors: AuthorFilter
    /// `nil` means every entity type.
    public var entityTypes: Set<String>?

    public init(authors: AuthorFilter = .all, entityTypes: Set<String>? = nil) {
        self.authors = authors
        self.entityTypes = entityTypes
    }

    /// The part of `transaction` this subscription sees, or `nil` if it sees nothing.
    public func filter(_ transaction: Transaction) -> Transaction? {
        guard authors.admits(transaction.author) else { return nil }
        guard let types = entityTypes else { return transaction }
        let kept = transaction.changes.filter { types.contains($0.key.type) }
        guard !kept.isEmpty else { return nil }
        if kept.count == transaction.changes.count { return transaction }
        return Transaction(token: transaction.token, author: transaction.author, changes: kept)
    }
}

/// How a consumer reports a failed delivery.
///
/// The distinction is load-bearing: a transient failure (offline, disk busy) must never
/// lose data, so the lane backs off and retries the same batch. A permanent failure
/// (the payload itself is unprocessable) must never block the lane forever, so the lane
/// isolates the offending transaction and quarantines it.
public enum ConsumerFailure: Error, Sendable, Equatable {
    case transient(String)
    case permanent(String)
}

/// A side system kept in sync from the history stream: a sync outbox, a search index,
/// a Spotlight/App Intents entity index, widget reloads.
///
/// Contract the lane relies on:
/// - **Idempotent per token.** Delivery is at-least-once: the cursor is persisted *after*
///   `apply` returns, so a crash in between redelivers the batch. A consumer that cannot
///   tolerate a repeated token will double-apply after every crash.
/// - **Not assumed atomic.** If `apply` throws partway through a batch, the lane will
///   redeliver some of those transactions one at a time while isolating the failure.
/// - **Throw `ConsumerFailure`.** Any other error is treated as transient — the safe
///   default is "retry and surface a stall", never "silently drop".
public protocol ChangeConsumer: Sendable {
    var id: ConsumerID { get }
    var subscription: Subscription { get }

    /// Applies a non-empty, token-ordered batch of already-filtered transactions.
    func apply(_ batch: [Transaction]) async throws

    /// Discards derived state and rebuilds it from a consistent snapshot. Called when the
    /// consumer's cursor fell behind history retention, or for a consumer registered with
    /// `.snapshot` start position.
    func rebuild(from snapshot: Snapshot) async throws
}

/// Durable per-consumer cursor storage (UserDefaults, a file, a SwiftData row — the
/// feed does not care). One consumer's cursor never depends on another's.
public protocol CursorStore: Sendable {
    func load(_ consumer: ConsumerID) async throws -> HistoryToken?
    /// Persists `token`. Implementations must ignore a token older than the stored one,
    /// so a stale writer can never move a cursor backwards.
    func save(_ token: HistoryToken, for consumer: ConsumerID) async throws
}

/// In-memory `CursorStore` with fault injection for tests and the demo.
public actor InMemoryCursorStore: CursorStore {
    public struct InjectedFailure: Error, Sendable, Equatable {}

    private var cursors: [ConsumerID: HistoryToken] = [:]
    private var failingSaves = 0

    public init(initial: [ConsumerID: HistoryToken] = [:]) {
        self.cursors = initial
    }

    public func load(_ consumer: ConsumerID) -> HistoryToken? {
        cursors[consumer]
    }

    public func save(_ token: HistoryToken, for consumer: ConsumerID) throws {
        if failingSaves > 0 {
            failingSaves -= 1
            throw InjectedFailure()
        }
        if let existing = cursors[consumer], existing >= token { return }
        cursors[consumer] = token
    }

    /// Makes the next `count` saves throw (negative counts are treated as zero).
    public func failNextSaves(_ count: Int) {
        failingSaves = max(0, count)
    }

    public func all() -> [ConsumerID: HistoryToken] {
        cursors
    }
}
