import ChangeFeed

/// Feature-owned adapter: requests widget timeline reloads (WidgetKit's
/// `reloadTimelines(ofKind:)` in an app), coalescing a whole batch into one request.
///
/// WidgetKit rations reloads, so this adapter carries its own reload budget. When the
/// budget is spent it throws a *transient* failure: the lane backs off with its cursor
/// unmoved, and the reload happens once budget returns — deferred, never lost. That is
/// back-pressure expressed as a delivery outcome rather than as dropped work.
public actor WidgetReloadConsumer: ChangeConsumer {
    public nonisolated let id: ConsumerID
    public nonisolated let subscription: Subscription

    private let budgetPerWindow: Int
    private var remaining: Int
    private var reloads = 0
    private var coalesced = 0
    private var lastApplied: HistoryToken = .zero

    public init(id: ConsumerID = "widget-reload", entityTypes: Set<String> = ["Note"], budgetPerWindow: Int = 40) {
        self.id = id
        self.subscription = Subscription(authors: .all, entityTypes: entityTypes)
        self.budgetPerWindow = max(1, budgetPerWindow)
        self.remaining = max(1, budgetPerWindow)
    }

    public func apply(_ batch: [Transaction]) throws {
        let fresh = batch.filter { $0.token > lastApplied }
        guard let newest = fresh.last else { return }
        guard remaining > 0 else {
            throw ConsumerFailure.transient("widget reload budget exhausted")
        }
        remaining -= 1
        reloads = SaturatingMath.add(reloads, 1)
        coalesced = SaturatingMath.add(coalesced, fresh.count)
        lastApplied = newest.token
    }

    public func rebuild(from snapshot: Snapshot) throws {
        guard remaining > 0 else {
            throw ConsumerFailure.transient("widget reload budget exhausted")
        }
        remaining -= 1
        reloads = SaturatingMath.add(reloads, 1)
        lastApplied = max(lastApplied, snapshot.token)
    }

    /// Starts a new budget window (WidgetKit's is roughly daily).
    public func resetBudget() {
        remaining = budgetPerWindow
    }

    /// Reload requests issued so far, and how many transactions they covered.
    public func counters() -> (reloads: Int, coalescedTransactions: Int, budgetRemaining: Int) {
        (reloads, coalesced, remaining)
    }
}
