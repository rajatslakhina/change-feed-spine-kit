/// Errors a history source can report to a reader.
public enum HistoryError: Error, Sendable, Equatable {
    /// The requested position has been pruned. Every transaction up to and including
    /// `prunedThrough` is gone, so a reader at an earlier cursor cannot be caught up
    /// incrementally and must rebuild from a snapshot.
    case cursorExpired(cursor: HistoryToken, prunedThrough: HistoryToken)
}

/// The port the change feed reads from.
///
/// In production this is a thin adapter over SwiftData's persistent history
/// (`HistoryObserver` / history tokens on iOS 27). The core depends only on this
/// protocol, so the delivery semantics — cursors, budgets, quarantine, rebuild — are
/// testable on any platform with an in-memory implementation.
///
/// Contract:
/// - Tokens are strictly increasing in commit order.
/// - `transactions(after:limit:)` returns transactions with `token > cursor`, ascending,
///   at most `limit` of them; `limit <= 0` returns an empty array.
/// - When `cursor` is older than the retention horizon it throws `HistoryError.cursorExpired`.
/// - `snapshot()` returns the live records and the exact token they reflect, atomically.
public protocol HistorySource: Sendable {
    func head() async -> HistoryToken
    func transactions(after cursor: HistoryToken, limit: Int) async throws -> [Transaction]
    func snapshot() async -> Snapshot
}
