/// Who wrote a transaction. Every write into the store carries exactly one author.
///
/// This mirrors SwiftData's persistent-history `author` string, but as a closed type:
/// echo-loop prevention and agent auditing both depend on authorship being impossible
/// to misspell.
public enum Author: Hashable, Sendable, Codable, CustomStringConvertible {
    /// A direct edit by the person using the app.
    case user
    /// A change that arrived from the server (a sync pull).
    case sync
    /// A schema or data migration.
    case migration
    /// An AI agent acting inside the app. The associated value identifies the agent
    /// *session* (for example `"assistant#42"`), which is the unit of audit and undo.
    case agent(String)

    /// The coarse category used by subscription filters.
    public enum Kind: String, Hashable, Sendable, Codable, CaseIterable {
        case user, sync, migration, agent
    }

    public var kind: Kind {
        switch self {
        case .user: return .user
        case .sync: return .sync
        case .migration: return .migration
        case .agent: return .agent
        }
    }

    public var description: String {
        switch self {
        case .user: return "user"
        case .sync: return "sync"
        case .migration: return "migration"
        case .agent(let session): return "agent(\(session))"
        }
    }
}

/// A position in the history stream. Mirrors SwiftData's history token / `eventCounter`:
/// strictly increasing, assigned by the store at commit time.
///
/// A consumer's cursor is the token of the *last* transaction it has fully handled;
/// `HistoryToken.zero` means "nothing handled yet".
public struct HistoryToken: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
    public let value: UInt64

    public init(_ value: UInt64) {
        self.value = value
    }

    public static let zero = HistoryToken(0)

    public static func < (lhs: HistoryToken, rhs: HistoryToken) -> Bool {
        lhs.value < rhs.value
    }

    /// The next token, saturating at `UInt64.max` instead of trapping.
    public var successor: HistoryToken {
        HistoryToken(SaturatingMath.add(value, 1))
    }

    public var description: String { "#\(value)" }
}

/// Identifies one persisted entity (a SwiftData model instance, in production).
public struct EntityKey: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
    public let type: String
    public let id: String

    public init(type: String, id: String) {
        self.type = type
        self.id = id
    }

    public static func < (lhs: EntityKey, rhs: EntityKey) -> Bool {
        (lhs.type, lhs.id) < (rhs.type, rhs.id)
    }

    public var description: String { "\(type)/\(id)" }
}

/// The persisted field values of one entity. Deliberately untyped (`String` values):
/// the feed transports changes, it does not interpret them.
public struct Record: Hashable, Sendable, Codable {
    public var fields: [String: String]

    public init(_ fields: [String: String] = [:]) {
        self.fields = fields
    }

    public subscript(field: String) -> String? {
        get { fields[field] }
        set { fields[field] = newValue }
    }
}

/// What happened to one entity inside one transaction, with before- and after-images.
///
/// Before-images are what make agent undo possible without a second audit log: the
/// history stream itself is the audit log.
public struct Change: Hashable, Sendable, Codable {
    public enum Kind: String, Hashable, Sendable, Codable {
        case insert, update, delete
    }

    public let key: EntityKey
    public let before: Record?
    public let after: Record?

    /// Returns `nil` when the change has no effect (both images absent, or identical).
    public init?(key: EntityKey, before: Record?, after: Record?) {
        guard before != after else { return nil }
        self.key = key
        self.before = before
        self.after = after
    }

    public var kind: Kind {
        switch (before, after) {
        case (nil, _): return .insert
        case (_, nil): return .delete
        default: return .update
        }
    }

    /// Fields whose value differs between the before- and after-image.
    public var changedFields: Set<String> {
        let beforeFields = before?.fields ?? [:]
        let afterFields = after?.fields ?? [:]
        var result = Set<String>()
        for name in Set(beforeFields.keys).union(afterFields.keys) where beforeFields[name] != afterFields[name] {
            result.insert(name)
        }
        return result
    }
}

/// One committed transaction in the history stream.
public struct Transaction: Hashable, Sendable, Codable, Identifiable {
    public let token: HistoryToken
    public let author: Author
    public let changes: [Change]

    public init(token: HistoryToken, author: Author, changes: [Change]) {
        self.token = token
        self.author = author
        self.changes = changes
    }

    public var id: HistoryToken { token }
}

/// A consistent copy of every live record, taken at exactly `token`.
///
/// Used to rebuild a consumer whose cursor fell behind the history retention horizon,
/// and to bootstrap a consumer that should not replay history.
public struct Snapshot: Sendable, Equatable {
    public let token: HistoryToken
    public let records: [EntityKey: Record]

    public init(token: HistoryToken, records: [EntityKey: Record]) {
        self.token = token
        self.records = records
    }
}
