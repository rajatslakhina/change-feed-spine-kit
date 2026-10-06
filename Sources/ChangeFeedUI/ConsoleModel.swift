#if canImport(SwiftUI)
import SwiftUI
import ChangeFeed
import ChangeFeedAdapters

/// Settings the host app owns and hands to the console.
public struct ConsoleConfiguration: Sendable {
    public var budget: BatchBudget
    public var retry: RetryPolicy
    /// History retention (transactions). Small on purpose in the demo, so a burst
    /// visibly forces cursor-expiry rebuilds.
    public var retention: Int
    public var agentSessionPrefix: String

    public init(budget: BatchBudget, retry: RetryPolicy, retention: Int, agentSessionPrefix: String = "assistant") {
        self.budget = budget
        self.retry = retry
        self.retention = max(1, retention)
        self.agentSessionPrefix = agentSessionPrefix
    }
}

/// One row of the recent-history list.
public struct HistoryRow: Identifiable, Hashable, Sendable {
    public let id: HistoryToken
    public let author: Author
    public let summary: String
}

/// Drives the demo: a real `InMemoryHistoryStore`, a real `ChangeDispatcher` and the
/// three reference adapters. Nothing here is simulated except the server and the agent.
@MainActor
public final class ConsoleModel: ObservableObject {
    @Published public private(set) var lanes: [LaneStatus] = []
    @Published public private(set) var history: [HistoryRow] = []
    @Published public private(set) var head: HistoryToken = .zero
    @Published public private(set) var horizon: HistoryToken = .zero
    @Published public private(set) var report: AgentSessionReport?
    @Published public private(set) var plan: UndoPlan?
    @Published public private(set) var outboxDepth = 0
    @Published public private(set) var widgetReloads = 0
    /// Uploads performed by the simulated network, by author kind of the source change.
    @Published public private(set) var uploadsByAuthor: [Author.Kind: Int] = [:]
    /// Set when the agent audit cannot be produced (its history was pruned).
    @Published public private(set) var auditNote: String?
    @Published public private(set) var searchHits: [EntityKey] = []
    @Published public private(set) var log: [String] = []
    /// True while an action runs; the console disables its buttons.
    @Published public private(set) var isBusy = false
    @Published public var searchTerm = "eggs" {
        didSet { Task { await self.refreshSearch() } }
    }

    public let configuration: ConsoleConfiguration
    private let store: InMemoryHistoryStore
    private let cursors = InMemoryCursorStore()
    private let clock = ManualFeedClock()
    private let dispatcher: ChangeDispatcher
    private let outbox = SyncOutboxConsumer()
    private let index = SearchIndexConsumer()
    private let widget = WidgetReloadConsumer(budgetPerWindow: 12)
    private var started = false
    private var sessionNumber = 0
    /// Head just before the current agent session began: the audit reads from here.
    private var sessionStart: HistoryToken = .zero
    private var editNumber = 0
    private static let logLimit = 40
    private static let historyRows = 14

    public init(configuration: ConsoleConfiguration) {
        self.configuration = configuration
        self.store = InMemoryHistoryStore(retentionLimit: configuration.retention)
        self.dispatcher = ChangeDispatcher(source: store, cursors: cursors, clock: clock)
    }

    public var currentSession: String {
        "\(configuration.agentSessionPrefix)#\(sessionNumber)"
    }

    /// Registers the lanes, seeds data and runs one agent session, so the very first
    /// screen already shows three lanes, an agent diff and an undo plan.
    public func start() async {
        guard !started, begin() else { return }
        started = true
        defer { isBusy = false }
        do {
            try await dispatcher.register(outbox, budget: configuration.budget, retry: configuration.retry)
            try await dispatcher.register(index, budget: configuration.budget, retry: configuration.retry)
            try await dispatcher.register(widget, budget: configuration.budget, retry: configuration.retry)
        } catch {
            append("registration failed: \(error)")
        }
        await store.commit(author: .user, [
            .put(Self.key("groceries"), Record(["title": "Groceries", "body": "milk", "pinned": "no"])),
            .put(Self.key("trip"), Record(["title": "Trip", "body": "Lisbon in May"]))
        ])
        append("user seeded 2 notes")
        await agentSession()
    }

    // MARK: - Actions
    //
    // Every action runs exclusively (`isBusy`): actions suspend at each `await`, and two
    // interleaved actions would make the demo show states no single action produces — an
    // audit taken between a session's two commits, or a burst that lanes read half of.

    public func userEdit() async {
        guard begin() else { return }
        defer { isBusy = false }
        editNumber += 1
        await store.commit(author: .user, [.patch(Self.key("trip"), set: ["body": "Lisbon in May, edit \(editNumber)"])])
        append("user edited trip")
        await step()
    }

    /// The agent edits one note, creates one and deletes one, as its own author.
    public func runAgentSession() async {
        guard begin() else { return }
        defer { isBusy = false }
        await agentSession()
    }

    /// A change arrives from the server. The outbox must not send it back.
    public func syncPull() async {
        guard begin() else { return }
        defer { isBusy = false }
        editNumber += 1
        await store.commit(author: .sync, [.put(Self.key("shared-\(editNumber)"), Record(["title": "From server", "body": "shared eggs recipe"]))])
        append("sync pulled 1 note (outbox should skip it)")
        await step()
    }

    /// A record the search indexer cannot tokenize. Only that lane quarantines it.
    public func injectPoison() async {
        guard begin() else { return }
        defer { isBusy = false }
        editNumber += 1
        await store.commit(author: .user, [.put(Self.key("broken-\(editNumber)"), Record(["title": "Broken", SearchIndexConsumer.malformedField: "1"]))])
        append("user saved a malformed note (search index should quarantine it)")
        await step()
    }

    /// The user edits the agent's text, so the next undo has a conflict to report.
    public func editAfterAgent() async {
        guard begin() else { return }
        defer { isBusy = false }
        let committed = await store.commit(author: .user, [.patch(Self.key("groceries"), set: ["body": "milk only, thanks"])])
        append(committed == nil ? "groceries body already says that" : "user rewrote the agent's groceries body")
        await step()
    }

    /// Commits more transactions than history retains, before any lane can read them.
    public func burst() async {
        guard begin() else { return }
        defer { isBusy = false }
        let count = SaturatingMath.add(configuration.retention, 6)
        editNumber += 1
        for index in 0..<count {
            await store.commit(author: .user, [.patch(Self.key("counter"), set: ["value": "\(editNumber)-\(index)"])])
        }
        append("burst of \(count) edits: history pruned past every cursor")
        await step()
    }

    /// Applies the non-conflicting part of the undo plan as one user transaction.
    public func undoAgent() async {
        guard begin() else { return }
        defer { isBusy = false }
        guard let plan, !plan.operations.isEmpty else {
            append("nothing to undo")
            return
        }
        await store.commit(author: .user, plan.operations)
        append("undid \(plan.session): \(plan.operations.count) ops, \(plan.conflicts.count) conflicts left alone")
        await step()
    }

    public func resetWidgetBudget() async {
        guard begin() else { return }
        defer { isBusy = false }
        await widget.resetBudget()
        append("widget reload budget reset")
        await step()
    }

    /// One scheduling round: advance the clock one tick, step every lane, upload the outbox.
    public func pump() async {
        guard begin() else { return }
        defer { isBusy = false }
        await step()
    }

    public func drain() async {
        guard begin() else { return }
        defer { isBusy = false }
        clock.advance(by: 1)
        let rounds = await dispatcher.drain(maxRounds: 50)
        append("drained in \(rounds) round(s)")
        await upload()
        await refresh()
    }

    // MARK: - Internals

    private func begin() -> Bool {
        guard !isBusy else { return false }
        isBusy = true
        return true
    }

    /// One agent session: a user first saves a note the agent will tidy away, then the
    /// agent edits one note, creates one and deletes that one — in two transactions.
    private func agentSession() async {
        let number = sessionNumber + 1
        let session = "\(configuration.agentSessionPrefix)#\(number)"
        let victim = Self.key("scratch-\(number)")
        await store.commit(author: .user, [.put(victim, Record(["title": "Scratch \(number)", "body": "stale list"]))])
        let start = await store.head()
        let author = Author.agent(session)
        let first = await store.commit(author: author, [
            .patch(Self.key("groceries"), set: ["body": "milk, eggs, bread (\(session))", "pinned": number.isMultiple(of: 2) ? "no" : "yes"]),
            .put(Self.key("summary-\(number)"), Record(["title": "Weekly summary", "body": "eggs are on two lists"]))
        ])
        let second = await store.commit(author: author, [.delete(victim)])
        sessionNumber = number
        sessionStart = start
        let changes = (first?.changes ?? []) + (second?.changes ?? [])
        let summary = changes.map { "\($0.kind.rawValue) \($0.key.id)" }.joined(separator: ", ")
        append("\(author) wrote: \(summary.isEmpty ? "nothing" : summary)")
        await step()
    }

    private func step() async {
        clock.advance(by: 1)
        let results = await dispatcher.pumpAll()
        for id in results.keys.sorted() {
            if let outcome = results[id], outcome != .caughtUp {
                append("\(id): \(Self.describe(outcome))")
            }
        }
        await upload()
        await refresh()
    }

    private func upload() async {
        for entry in await outbox.takeForUpload(limit: 100) {
            let kind: Author.Kind
            switch entry.payload {
            case .change: kind = entry.author.kind
            case .fullResync: kind = .migration
            }
            uploadsByAuthor[kind, default: 0] += 1
        }
    }

    // MARK: - Refresh

    private func refresh() async {
        lanes = await dispatcher.statuses()
        head = await store.head()
        horizon = await store.retentionHorizon()
        outboxDepth = await outbox.pendingEntries().count
        widgetReloads = await widget.counters().reloads
        let start = HistoryToken(max(horizon.value, SaturatingMath.subtract(head.value, UInt64(Self.historyRows))))
        let recent = (try? await store.transactions(after: start, limit: Self.historyRows)) ?? []
        history = recent.reversed().map { transaction in
            let parts = transaction.changes.map { "\($0.kind.rawValue) \($0.key.id)" }
            return HistoryRow(id: transaction.token, author: transaction.author, summary: parts.joined(separator: ", "))
        }
        do {
            // Scan from just before the session began: if any of the session was pruned this
            // throws instead of returning a partial diff that looks complete.
            let current = try await AgentAudit.report(session: currentSession, source: store, after: sessionStart)
            report = current
            plan = AgentAudit.undoPlan(for: current, current: await store.snapshot())
            auditNote = nil
        } catch {
            report = nil
            plan = nil
            auditNote = "Audit unavailable for \(currentSession): history through \(horizon) was pruned. Run a new agent session."
        }
        await refreshSearch()
    }

    private func refreshSearch() async {
        searchHits = await index.search(searchTerm.trimmingCharacters(in: .whitespaces))
    }

    private func append(_ line: String) {
        log.insert(line, at: 0)
        if log.count > Self.logLimit {
            log.removeLast(log.count - Self.logLimit)
        }
    }

    private static func key(_ id: String) -> EntityKey {
        EntityKey(type: "Note", id: id)
    }

    static func describe(_ outcome: StepOutcome) -> String {
        switch outcome {
        case .busy: return "busy"
        case .waiting(let until): return "backing off until tick \(until)"
        case .caughtUp: return "caught up"
        case let .advanced(to, applied, skipped, quarantined):
            return "→ \(to) (applied \(applied), skipped \(skipped), quarantined \(quarantined))"
        case .rebuilt(let token): return "rebuilt from snapshot at \(token)"
        case let .failed(attempt, retryAt): return "transient failure #\(attempt), retry at tick \(retryAt)"
        }
    }
}
#endif
