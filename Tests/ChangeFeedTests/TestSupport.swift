import ChangeFeed

/// A one-shot gate: `wait()` suspends until `open()` is called (or returns at once if it already was).
actor Gate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private(set) var arrivals = 0

    func wait() async {
        arrivals += 1
        if isOpen { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        isOpen = true
        let pending = waiters
        waiters = []
        for waiter in pending { waiter.resume() }
    }
}

/// A consumer that records exactly what it was handed, with scripted failures.
actor RecordingConsumer: ChangeConsumer {
    nonisolated let id: ConsumerID
    nonisolated let subscription: Subscription

    private(set) var batches: [[HistoryToken]] = []
    private(set) var applied: [HistoryToken] = []
    private(set) var rebuiltAt: [HistoryToken] = []
    private(set) var rebuiltRecordCount: [Int] = []
    private var permanentlyBad: Set<HistoryToken>
    private var transientFailuresLeft: Int
    private var rebuildFailuresLeft: Int
    private let gate: Gate?
    private let transientOnCall: Int?
    private(set) var applyCalls = 0

    init(
        _ id: ConsumerID,
        subscription: Subscription = Subscription(),
        permanentlyBad: Set<HistoryToken> = [],
        transientFailures: Int = 0,
        rebuildFailures: Int = 0,
        gate: Gate? = nil,
        transientOnCall: Int? = nil
    ) {
        self.transientOnCall = transientOnCall
        self.id = id
        self.subscription = subscription
        self.permanentlyBad = permanentlyBad
        self.transientFailuresLeft = transientFailures
        self.rebuildFailuresLeft = rebuildFailures
        self.gate = gate
    }

    func apply(_ batch: [Transaction]) async throws {
        if let gate { await gate.wait() }
        applyCalls += 1
        if applyCalls == transientOnCall {
            throw ConsumerFailure.transient("scripted transient on call \(applyCalls)")
        }
        if transientFailuresLeft > 0 {
            transientFailuresLeft -= 1
            throw ConsumerFailure.transient("scripted transient")
        }
        batches.append(batch.map(\.token))
        for transaction in batch {
            if permanentlyBad.contains(transaction.token) {
                throw ConsumerFailure.permanent("bad \(transaction.token)")
            }
            if !applied.contains(transaction.token) {
                applied.append(transaction.token)
            }
        }
    }

    func rebuild(from snapshot: Snapshot) async throws {
        if rebuildFailuresLeft > 0 {
            rebuildFailuresLeft -= 1
            throw ConsumerFailure.transient("scripted rebuild failure")
        }
        rebuiltAt.append(snapshot.token)
        rebuiltRecordCount.append(snapshot.records.count)
    }
}

func note(_ id: String) -> EntityKey {
    EntityKey(type: "Note", id: id)
}

/// Commits `count` single-note user edits and returns their tokens.
@discardableResult
func commitEdits(_ store: InMemoryHistoryStore, count: Int, author: Author = .user, prefix: String = "n") async -> [HistoryToken] {
    var tokens: [HistoryToken] = []
    for index in 0..<max(0, count) {
        if let transaction = await store.commit(author: author, [.patch(note("\(prefix)\(index)"), set: ["title": "t\(index)"])]) {
            tokens.append(transaction.token)
        }
    }
    return tokens
}

/// Holds the outcome of a background step once it finishes.
actor OutcomeBox {
    private(set) var value: StepOutcome?

    func set(_ outcome: StepOutcome) {
        value = outcome
    }
}
