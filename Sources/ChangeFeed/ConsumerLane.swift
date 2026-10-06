/// A transaction a lane gave up on: it failed permanently when delivered on its own.
public struct DeadLetter: Hashable, Sendable {
    public let token: HistoryToken
    public let author: Author
    public let reason: String
}

/// The observable condition of one lane.
public enum LaneHealth: Hashable, Sendable {
    /// Registered, never stepped.
    case pending
    /// Caught up with the head of the stream (as of its last step).
    case idle
    /// Delivered at least one batch in its last step; may have more to do.
    case delivering
    /// Waiting out a backoff after a transient failure.
    case backingOff(attempt: Int, until: UInt64)
    /// Still retrying, but has failed `stallAfter` times in a row. Needs a human.
    case stalled(attempt: Int, until: UInt64)
    /// Rebuilding derived state from a snapshot.
    case rebuilding
}

/// What a single `step` did.
public enum StepOutcome: Hashable, Sendable {
    /// Another step on this lane is already running (actor reentrancy guard).
    case busy
    /// In backoff; nothing attempted.
    case waiting(until: UInt64)
    /// Nothing new to deliver.
    case caughtUp
    /// Cursor moved to `to`. `applied` transactions were delivered, `skipped` were
    /// filtered out by the subscription, `quarantined` failed permanently on their own.
    case advanced(to: HistoryToken, applied: Int, skipped: Int, quarantined: Int)
    /// Derived state was rebuilt from a snapshot at this token.
    case rebuilt(at: HistoryToken)
    /// A transient failure; the same work will be retried at `retryAt`.
    case failed(attempt: Int, retryAt: UInt64)

    /// Whether the step moved the lane forward.
    public var madeProgress: Bool {
        switch self {
        case .advanced, .rebuilt: return true
        case .busy, .waiting, .caughtUp, .failed: return false
        }
    }
}

/// A point-in-time view of one lane, for dashboards and tests.
public struct LaneStatus: Hashable, Sendable, Identifiable {
    public let id: ConsumerID
    public let cursor: HistoryToken?
    /// Transactions between the cursor and the head of the stream.
    public let lag: UInt64
    public let health: LaneHealth
    public let appliedTransactions: Int
    public let skippedTransactions: Int
    public let rebuilds: Int
    public let deadLetters: [DeadLetter]
    public let droppedDeadLetters: Int
    public let lastError: String?
    /// `false` when the in-memory cursor is ahead of the durable one (a save failed and
    /// will be retried). A crash in that window means redelivery, never loss.
    public let cursorPersisted: Bool
}

/// One consumer's private delivery pipeline: its own cursor, budget, backoff and
/// quarantine. Lanes share nothing mutable, which is the whole failure-isolation story —
/// a slow, crashing or poisoned consumer can only ever stall *its own* lane.
///
/// Delivery guarantees, per lane:
/// - **Ordered:** transactions are delivered in strictly increasing token order.
/// - **At-least-once:** the cursor is saved after `apply` returns; redelivery after a
///   crash is expected and consumers must be idempotent per token.
/// - **No silent loss:** a transaction is skipped only if the subscription filters it
///   out, or it is recorded as a `DeadLetter`.
actor ConsumerLane {
    let consumer: any ChangeConsumer
    private let source: any HistorySource
    private let cursors: any CursorStore
    private let budget: BatchBudget
    private let retry: RetryPolicy
    private let start: StartPosition

    private var cursor: HistoryToken?
    private var persisted: HistoryToken?
    private var inFlight = false
    private var attempt = 0
    private var retryAt: UInt64 = 0
    private var health: LaneHealth = .pending
    private var applied = 0
    private var skipped = 0
    private var rebuilds = 0
    private var deadLetters: [DeadLetter] = []
    private var droppedDeadLetters = 0
    private var lastError: String?

    init(
        consumer: any ChangeConsumer,
        source: any HistorySource,
        cursors: any CursorStore,
        budget: BatchBudget,
        retry: RetryPolicy,
        start: StartPosition
    ) {
        self.consumer = consumer
        self.source = source
        self.cursors = cursors
        self.budget = budget
        self.retry = retry
        self.start = start
    }

    /// Runs one bounded unit of work.
    ///
    /// Reentrancy: this method suspends at every `await` (history reads, consumer calls,
    /// cursor saves). Without the `inFlight` guard, a second caller could interleave at
    /// one of those points, read the same un-advanced cursor and deliver the same batch
    /// twice — or worse, move the cursor backwards when the slower call finished last.
    func step(now: UInt64) async -> StepOutcome {
        guard !inFlight else { return .busy }
        inFlight = true
        defer { inFlight = false }

        if attempt > 0, now < retryAt {
            return .waiting(until: retryAt)
        }

        let current: HistoryToken
        if let known = cursor {
            current = known
        } else {
            switch await resolveStart(now: now) {
            case .ready(let token): current = token
            case .finished(let outcome): return outcome
            }
        }

        await persistIfNeeded()

        let page: [Transaction]
        do {
            page = try await source.transactions(after: current, limit: budget.maxTransactions)
        } catch HistoryError.cursorExpired {
            return await rebuild(now: now)
        } catch {
            return recordTransient(error, now: now)
        }

        // Back-pressure: take whole transactions until the change budget is spent.
        var taken = 0
        var lastToken = current
        var spent = 0
        var visible: [Transaction] = []
        var filteredOut: [HistoryToken] = []
        for transaction in page {
            let filtered = consumer.subscription.filter(transaction)
            let cost = filtered?.changes.count ?? 0
            if taken > 0, SaturatingMath.add(spent, cost) > budget.maxChanges { break }
            taken += 1
            spent = SaturatingMath.add(spent, cost)
            lastToken = transaction.token
            if let filtered {
                visible.append(filtered)
            } else {
                filteredOut.append(transaction.token)
            }
        }
        guard taken > 0 else {
            health = .idle
            attempt = 0
            return .caughtUp
        }
        let skippedHere = taken - visible.count
        health = .delivering

        guard !visible.isEmpty else {
            return await advance(to: lastToken, applied: 0, skipped: skippedHere, quarantined: 0)
        }

        do {
            try await consumer.apply(visible)
            return await advance(to: lastToken, applied: visible.count, skipped: skippedHere, quarantined: 0)
        } catch ConsumerFailure.permanent {
            return await isolate(visible, through: lastToken, skipped: skippedHere, filteredOut: filteredOut, now: now)
        } catch {
            return recordTransient(error, now: now)
        }
    }

    func status(head: HistoryToken) -> LaneStatus {
        LaneStatus(
            id: consumer.id,
            cursor: cursor,
            lag: SaturatingMath.subtract(head.value, cursor?.value ?? 0),
            health: health,
            appliedTransactions: applied,
            skippedTransactions: skipped,
            rebuilds: rebuilds,
            deadLetters: deadLetters,
            droppedDeadLetters: droppedDeadLetters,
            lastError: lastError,
            cursorPersisted: (cursor ?? .zero) == (persisted ?? .zero)
        )
    }

    // MARK: - Private

    private enum StartResolution {
        case ready(HistoryToken)
        case finished(StepOutcome)
    }

    private func resolveStart(now: UInt64) async -> StartResolution {
        let stored: HistoryToken?
        do {
            stored = try await cursors.load(consumer.id)
        } catch {
            return .finished(recordTransient(error, now: now))
        }
        if let stored {
            cursor = stored
            persisted = stored
            return .ready(stored)
        }
        switch start {
        case .replayAll:
            cursor = .zero
            return .ready(.zero)
        case .fromHead:
            let head = await source.head()
            cursor = head
            return .ready(head)
        case .snapshot:
            return .finished(await rebuild(now: now))
        }
    }

    /// Delivers a failed batch one transaction at a time, so one unprocessable
    /// transaction costs exactly one dead letter instead of the whole batch.
    private func isolate(
        _ visible: [Transaction],
        through lastToken: HistoryToken,
        skipped skippedHere: Int,
        filteredOut: [HistoryToken],
        now: UInt64
    ) async -> StepOutcome {
        var appliedHere = 0
        var quarantinedHere = 0
        for transaction in visible {
            do {
                try await consumer.apply([transaction])
                appliedHere += 1
            } catch ConsumerFailure.permanent(let reason) {
                quarantine(transaction, reason: reason)
                quarantinedHere += 1
            } catch {
                // A transient failure mid-isolation: keep what was achieved, retry the rest.
                // `cursor` already sits on the last transaction handled in this loop. Filtered-out
                // transactions at or below it will not be read again, so count them now.
                applied = SaturatingMath.add(applied, appliedHere)
                let passed = cursor ?? .zero
                skipped = SaturatingMath.add(skipped, filteredOut.filter { $0 <= passed }.count)
                await persistIfNeeded()
                return recordTransient(error, now: now)
            }
            cursor = transaction.token
        }
        return await advance(to: lastToken, applied: appliedHere, skipped: skippedHere, quarantined: quarantinedHere)
    }

    private func advance(to token: HistoryToken, applied appliedHere: Int, skipped skippedHere: Int, quarantined: Int) async -> StepOutcome {
        // Never move backwards (cannot happen with a well-behaved source; defended anyway).
        cursor = max(cursor ?? .zero, token)
        applied = SaturatingMath.add(applied, appliedHere)
        skipped = SaturatingMath.add(skipped, skippedHere)
        attempt = 0
        if quarantined == 0 { lastError = nil }
        await persistIfNeeded()
        return .advanced(to: token, applied: appliedHere, skipped: skippedHere, quarantined: quarantined)
    }

    private func rebuild(now: UInt64) async -> StepOutcome {
        health = .rebuilding
        let snapshot = await source.snapshot()
        do {
            try await consumer.rebuild(from: snapshot)
        } catch {
            return recordTransient(error, now: now)
        }
        cursor = max(cursor ?? .zero, snapshot.token)
        rebuilds = SaturatingMath.add(rebuilds, 1)
        attempt = 0
        lastError = nil
        health = .idle
        await persistIfNeeded()
        return .rebuilt(at: snapshot.token)
    }

    private func persistIfNeeded() async {
        // An absent durable cursor already means `.zero` (replay from the start), so a zero
        // cursor needs no write. `.fromHead` cursors are non-zero and *are* persisted
        // immediately: re-resolving "head" after a crash would silently skip everything
        // committed in between.
        guard let target = cursor, target != (persisted ?? .zero) else { return }
        do {
            try await cursors.save(target, for: consumer.id)
            // Another step cannot run concurrently (inFlight), so `target` is still current.
            persisted = target
        } catch {
            // Leave `persisted` stale; the next step retries. Worst case on crash: redelivery.
        }
    }

    private func recordTransient(_ error: Error, now: UInt64) -> StepOutcome {
        attempt = SaturatingMath.add(attempt, 1)
        let delay = retry.delay(forAttempt: attempt)
        retryAt = SaturatingMath.add(now, delay)
        lastError = String(describing: error)
        health = attempt >= retry.stallAfter
            ? .stalled(attempt: attempt, until: retryAt)
            : .backingOff(attempt: attempt, until: retryAt)
        return .failed(attempt: attempt, retryAt: retryAt)
    }

    private func quarantine(_ transaction: Transaction, reason: String) {
        deadLetters.append(DeadLetter(token: transaction.token, author: transaction.author, reason: reason))
        lastError = reason
        let overflow = deadLetters.count - retry.deadLetterCapacity
        if overflow > 0 {
            deadLetters.removeFirst(overflow)
            droppedDeadLetters = SaturatingMath.add(droppedDeadLetters, overflow)
        }
    }
}
