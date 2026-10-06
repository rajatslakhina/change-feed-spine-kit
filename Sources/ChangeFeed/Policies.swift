import Foundation

/// A monotonic tick source. Ticks are abstract (the demo advances one per pump; a
/// production adapter would read `ContinuousClock` in milliseconds), which keeps every
/// backoff decision deterministic under test.
public protocol FeedClock: Sendable {
    func now() -> UInt64
}

/// A clock that only moves when told to. Thread-safe.
public final class ManualFeedClock: FeedClock, @unchecked Sendable {
    // @unchecked: the only stored state is `ticks`, and every access goes through `lock`.
    private let lock = NSLock()
    private var ticks: UInt64

    public init(start: UInt64 = 0) {
        self.ticks = start
    }

    public func now() -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        return ticks
    }

    public func advance(by delta: UInt64) {
        lock.lock()
        defer { lock.unlock() }
        ticks = SaturatingMath.add(ticks, delta)
    }
}

/// Back-pressure: how much one lane may take in one step.
///
/// Both limits are clamped to at least 1. A single transaction larger than
/// `maxChanges` is still delivered on its own — otherwise one bulk import would wedge
/// the lane permanently, which is a worse failure than briefly exceeding the budget.
public struct BatchBudget: Hashable, Sendable {
    public let maxTransactions: Int
    public let maxChanges: Int

    public init(maxTransactions: Int = 64, maxChanges: Int = 512) {
        self.maxTransactions = max(1, maxTransactions)
        self.maxChanges = max(1, maxChanges)
    }
}

/// Retry and isolation policy for a lane.
public struct RetryPolicy: Hashable, Sendable {
    /// Delay after the first transient failure, in clock ticks.
    public let baseDelay: UInt64
    /// Upper bound on any single backoff delay.
    public let maxDelay: UInt64
    /// After this many consecutive transient failures the lane reports `.stalled`
    /// (it keeps retrying; stalling is a signal to a human, not a decision to drop data).
    public let stallAfter: Int
    /// Maximum dead letters kept per lane; older ones are counted, then discarded.
    public let deadLetterCapacity: Int

    public init(baseDelay: UInt64 = 1, maxDelay: UInt64 = 64, stallAfter: Int = 5, deadLetterCapacity: Int = 50) {
        self.baseDelay = max(1, baseDelay)
        self.maxDelay = max(self.baseDelay, maxDelay)
        self.stallAfter = max(1, stallAfter)
        self.deadLetterCapacity = max(1, deadLetterCapacity)
    }

    /// Exponential backoff for the `attempt`-th consecutive failure (1-based), capped at
    /// `maxDelay`. Total: never traps, whatever `attempt` is.
    public func delay(forAttempt attempt: Int) -> UInt64 {
        let exponent = attempt > 1 ? attempt - 1 : 0
        return min(maxDelay, SaturatingMath.doubling(baseDelay, times: exponent))
    }
}

/// Where a newly registered consumer (one with no stored cursor) starts.
public enum StartPosition: Hashable, Sendable {
    /// Replay all retained history from the beginning. Falls back to a snapshot rebuild
    /// if the beginning has already been pruned.
    case replayAll
    /// Ignore existing data; deliver only transactions committed from now on.
    case fromHead
    /// Rebuild from a snapshot, then follow the stream.
    case snapshot
}
