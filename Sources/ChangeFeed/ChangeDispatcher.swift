/// Errors from registering consumers.
public enum DispatcherError: Error, Sendable, Equatable {
    case duplicateConsumer(ConsumerID)
    case unknownConsumer(ConsumerID)
}

/// Fans one history stream out to many independent consumer lanes.
///
/// The dispatcher owns no delivery state of its own — every cursor, backoff and dead
/// letter lives in a lane. That is deliberate: the dispatcher is the piece most likely
/// to be restarted (app relaunch, BGTask), so it should be the piece with nothing to lose.
///
/// Scheduling model: callers drive the feed with `pump(_:)` / `pumpAll()` (in an app, on
/// every `HistoryObserver` notification plus app-foreground and BGTask wakeups). Each
/// lane steps concurrently in `pumpAll()`, so a consumer that is slow inside `apply`
/// delays only its own cursor: the other lanes finish and persist their cursors while it
/// is still running. (The `pumpAll()` *call* returns when its slowest lane returns; a host
/// that cannot accept that drives each lane with `pump(_:)` from its own task.)
public actor ChangeDispatcher {
    private let source: any HistorySource
    private let cursors: any CursorStore
    private let clock: any FeedClock
    private var lanes: [ConsumerID: ConsumerLane] = [:]

    public init(source: any HistorySource, cursors: any CursorStore, clock: any FeedClock) {
        self.source = source
        self.cursors = cursors
        self.clock = clock
    }

    /// Adds a consumer. A consumer with a stored cursor resumes from it; otherwise it
    /// starts at `start`.
    public func register(
        _ consumer: any ChangeConsumer,
        budget: BatchBudget = BatchBudget(),
        retry: RetryPolicy = RetryPolicy(),
        start: StartPosition = .replayAll
    ) throws {
        guard lanes[consumer.id] == nil else {
            throw DispatcherError.duplicateConsumer(consumer.id)
        }
        lanes[consumer.id] = ConsumerLane(
            consumer: consumer,
            source: source,
            cursors: cursors,
            budget: budget,
            retry: retry,
            start: start
        )
    }

    public var consumerIDs: [ConsumerID] {
        lanes.keys.sorted()
    }

    /// Runs one step of one lane.
    public func pump(_ id: ConsumerID) async throws -> StepOutcome {
        guard let lane = lanes[id] else { throw DispatcherError.unknownConsumer(id) }
        return await lane.step(now: clock.now())
    }

    /// Runs one step of every lane, concurrently.
    public func pumpAll() async -> [ConsumerID: StepOutcome] {
        let snapshot = lanes
        let now = clock.now()
        return await withTaskGroup(of: (ConsumerID, StepOutcome).self) { group in
            for (id, lane) in snapshot {
                group.addTask { (id, await lane.step(now: now)) }
            }
            var results: [ConsumerID: StepOutcome] = [:]
            for await (id, outcome) in group {
                results[id] = outcome
            }
            return results
        }
    }

    /// Pumps every lane until none makes progress, or `maxRounds` is reached.
    /// Returns the number of rounds run. Bounded by construction.
    @discardableResult
    public func drain(maxRounds: Int = 1_000) async -> Int {
        var rounds = 0
        while rounds < max(0, maxRounds) {
            rounds += 1
            let results = await pumpAll()
            if !results.values.contains(where: \.madeProgress) { break }
        }
        return rounds
    }

    /// Status of every lane, sorted by consumer id.
    public func statuses() async -> [LaneStatus] {
        let head = await source.head()
        var result: [LaneStatus] = []
        for id in lanes.keys.sorted() {
            if let lane = lanes[id] {
                result.append(await lane.status(head: head))
            }
        }
        return result
    }

    public func status(of id: ConsumerID) async throws -> LaneStatus {
        guard let lane = lanes[id] else { throw DispatcherError.unknownConsumer(id) }
        return await lane.status(head: await source.head())
    }
}
