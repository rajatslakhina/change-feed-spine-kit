#if canImport(SwiftUI)
import SwiftUI
import ChangeFeed

/// A live console for the change-feed spine: one history stream, three independent
/// lanes, and an agent session you can audit and undo.
public struct ChangeFeedConsole: View {
    @StateObject private var model: ConsoleModel

    public init(configuration: ConsoleConfiguration) {
        _model = StateObject(wrappedValue: ConsoleModel(configuration: configuration))
    }

    public var body: some View {
        NavigationStack {
            List {
                streamSection
                controlsSection
                lanesSection
                agentSection
                searchSection
                historySection
                logSection
            }
            .navigationTitle("Change Feed")
        }
        .task { await model.start() }
    }

    // MARK: Sections

    private var streamSection: some View {
        Section("History stream") {
            LabeledContent("Head", value: model.head.description)
            LabeledContent("Pruned through", value: model.horizon.description)
            LabeledContent("Retention", value: "\(model.configuration.retention) transactions")
            LabeledContent("Uploaded (user / agent / sync)", value: uploadsText)
            LabeledContent("Widget reloads", value: "\(model.widgetReloads)")
        }
    }

    private var uploadsText: String {
        let user = model.uploadsByAuthor[.user] ?? 0
        let agent = model.uploadsByAuthor[.agent] ?? 0
        let sync = model.uploadsByAuthor[.sync] ?? 0
        let resync = model.uploadsByAuthor[.migration] ?? 0
        let base = "\(user) / \(agent) / \(sync)"
        return resync > 0 ? base + " + \(resync) full resync" : base
    }

    private var controlsSection: some View {
        Section("Write as…") {
            action("User edits a note", systemImage: "person") { await model.userEdit() }
            action("Agent session (edit, insert, delete)", systemImage: "sparkles") { await model.runAgentSession() }
            action("Sync pull from server", systemImage: "arrow.down.circle") { await model.syncPull() }
            action("Save a malformed note (poison)", systemImage: "exclamationmark.triangle") { await model.injectPoison() }
            action("User rewrites the agent's text", systemImage: "pencil") { await model.editAfterAgent() }
            action("Burst past retention (forces rebuild)", systemImage: "bolt") { await model.burst() }
            action("Reset widget reload budget", systemImage: "arrow.counterclockwise") { await model.resetWidgetBudget() }
            action("Pump lanes once", systemImage: "play") { await model.pump() }
            action("Drain until caught up", systemImage: "forward.end") { await model.drain() }
        }
    }

    private var lanesSection: some View {
        Section("Lanes (one cursor each)") {
            if model.lanes.isEmpty {
                Text("Starting…").foregroundStyle(.secondary)
            }
            ForEach(model.lanes) { lane in
                LaneRow(lane: lane)
            }
        }
    }

    @ViewBuilder
    private var agentSection: some View {
        Section("What did \(model.currentSession) change?") {
            if let note = model.auditNote {
                Text(note).foregroundStyle(.orange)
            }
            if let report = model.report {
                if report.changes.isEmpty {
                    Text("No net changes from this session.").foregroundStyle(.secondary)
                }
                ForEach(report.changes) { change in
                    VStack(alignment: .leading, spacing: 4) {
                        Text("\(change.net.rawValue.uppercased())  \(change.key.id)").font(.headline)
                        ForEach(change.fieldDiffs, id: \.field) { diff in
                            Text("\(diff.field): \(diff.before ?? "∅") → \(diff.after ?? "∅")")
                                .font(.caption.monospaced())
                                .foregroundStyle(.secondary)
                        }
                    }
                }
            }
            if let plan = model.plan {
                if plan.conflicts.isEmpty {
                    Text("Undo plan: \(plan.operations.count) operation(s), no conflicts.")
                } else {
                    Text("Undo plan: \(plan.operations.count) operation(s), \(plan.conflicts.count) conflict(s) left alone:")
                    ForEach(Array(plan.conflicts.enumerated()), id: \.offset) { item in
                        Text(Self.describe(item.element))
                            .font(.caption)
                            .foregroundStyle(.orange)
                    }
                }
                Button(role: .destructive) {
                    Task { await model.undoAgent() }
                } label: {
                    Label("Undo agent session", systemImage: "arrow.uturn.backward")
                }
                .disabled(plan.operations.isEmpty || model.isBusy)
            }
        }
    }

    private var searchSection: some View {
        Section("Search index lane") {
            TextField("Search term", text: $model.searchTerm)
            if model.searchHits.isEmpty {
                Text("No hits").foregroundStyle(.secondary)
            }
            ForEach(model.searchHits, id: \.self) { key in
                Text(key.id)
            }
        }
    }

    private var historySection: some View {
        Section("Recent transactions") {
            if model.history.isEmpty {
                Text("No retained history").foregroundStyle(.secondary)
            }
            ForEach(model.history) { row in
                HStack(alignment: .firstTextBaseline) {
                    Text(row.id.description).font(.caption.monospaced())
                    AuthorBadge(author: row.author)
                    Text(row.summary).font(.caption).lineLimit(2)
                }
            }
        }
    }

    private var logSection: some View {
        Section("Delivery log") {
            ForEach(Array(model.log.enumerated()), id: \.offset) { item in
                Text(item.element).font(.caption.monospaced())
            }
        }
    }

    private func action(_ title: String, systemImage: String, perform: @escaping @MainActor () async -> Void) -> some View {
        Button {
            Task { await perform() }
        } label: {
            Label(title, systemImage: systemImage)
        }
        .disabled(model.isBusy)
    }

    static func describe(_ conflict: UndoConflict) -> String {
        switch conflict.reason {
        case let .fieldChangedSince(agentValue, currentValue):
            return "\(conflict.key.id).\(conflict.field ?? "?"): agent wrote \"\(agentValue ?? "∅")\", now \"\(currentValue ?? "∅")\""
        case .deletedSince:
            return "\(conflict.key.id): deleted after the agent edited it"
        case .modifiedSinceInsert:
            return "\(conflict.key.id): edited after the agent created it"
        case .recreatedSince:
            return "\(conflict.key.id): recreated after the agent deleted it"
        }
    }
}

struct LaneRow: View {
    let lane: LaneStatus

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text(lane.id.description).font(.headline)
                Spacer()
                Text(healthText)
                    .font(.caption.bold())
                    .padding(.horizontal, 8)
                    .padding(.vertical, 2)
                    .background(healthColor.opacity(0.2), in: Capsule())
                    .foregroundStyle(healthColor)
            }
            Text("cursor \(lane.cursor?.description ?? "–") · lag \(lane.lag) · applied \(lane.appliedTransactions) · skipped \(lane.skippedTransactions) · rebuilds \(lane.rebuilds)")
                .font(.caption.monospaced())
            if !lane.deadLetters.isEmpty {
                Text("quarantined: " + lane.deadLetters.map(\.token.description).joined(separator: ", "))
                    .font(.caption)
                    .foregroundStyle(.red)
            }
            if let error = lane.lastError {
                Text(error).font(.caption2).foregroundStyle(.secondary).lineLimit(2)
            }
        }
        .padding(.vertical, 2)
    }

    private var healthText: String {
        switch lane.health {
        case .pending: return "pending"
        case .idle: return "idle"
        case .delivering: return "delivering"
        case .backingOff(let attempt, _): return "backoff #\(attempt)"
        case .stalled(let attempt, _): return "stalled #\(attempt)"
        case .rebuilding: return "rebuilding"
        }
    }

    private var healthColor: Color {
        switch lane.health {
        case .pending, .idle: return .secondary
        case .delivering: return .green
        case .backingOff, .rebuilding: return .orange
        case .stalled: return .red
        }
    }
}

struct AuthorBadge: View {
    let author: Author

    var body: some View {
        Text(author.description)
            .font(.caption2.bold())
            .padding(.horizontal, 6)
            .padding(.vertical, 1)
            .background(color.opacity(0.18), in: RoundedRectangle(cornerRadius: 4))
            .foregroundStyle(color)
    }

    private var color: Color {
        switch author.kind {
        case .user: return .blue
        case .sync: return .teal
        case .migration: return .gray
        case .agent: return .purple
        }
    }
}
#endif
