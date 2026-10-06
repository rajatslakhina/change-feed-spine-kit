import ChangeFeed

/// Feature-owned adapter: an inverted index over note text — the on-device search /
/// semantic-index refresher, reduced to its delivery contract.
///
/// It is the adapter that can be *poisoned*: a record carrying the `malformedField`
/// marker cannot be tokenized, so `apply` throws `ConsumerFailure.permanent`. Each
/// transaction is validated before any of it is applied, so a failed transaction leaves
/// the index untouched (atomic per transaction, idempotent per token).
public actor SearchIndexConsumer: ChangeConsumer {
    public nonisolated let id: ConsumerID
    public nonisolated let subscription: Subscription

    /// Field whose presence marks a record as unprocessable (fault injection).
    public static let malformedField = "__malformed"

    /// Fields that are tokenized.
    public let indexedFields: [String]

    private var postings: [String: Set<EntityKey>] = [:]
    private var termsByEntity: [EntityKey: Set<String>] = [:]
    private var lastApplied: HistoryToken = .zero
    private var duplicates = 0

    public init(
        id: ConsumerID = "search-index",
        entityTypes: Set<String> = ["Note"],
        indexedFields: [String] = ["title", "body"]
    ) {
        self.id = id
        self.subscription = Subscription(authors: .all, entityTypes: entityTypes)
        self.indexedFields = indexedFields
    }

    public func apply(_ batch: [Transaction]) throws {
        for transaction in batch {
            guard transaction.token > lastApplied else {
                duplicates = SaturatingMath.add(duplicates, 1)
                continue
            }
            for change in transaction.changes where change.after?[Self.malformedField] != nil {
                throw ConsumerFailure.permanent("cannot tokenize \(change.key) at \(transaction.token)")
            }
            for change in transaction.changes {
                index(change.key, record: change.after)
            }
            lastApplied = transaction.token
        }
    }

    public func rebuild(from snapshot: Snapshot) {
        postings = [:]
        termsByEntity = [:]
        let types = subscription.entityTypes
        for (key, record) in snapshot.records where types?.contains(key.type) ?? true {
            if record[Self.malformedField] == nil {
                index(key, record: record)
            }
        }
        lastApplied = max(lastApplied, snapshot.token)
    }

    /// Entities whose indexed text contains `term` (case-insensitive whole word).
    public func search(_ term: String) -> [EntityKey] {
        (postings[term.lowercased()] ?? []).sorted()
    }

    public func indexedEntities() -> [EntityKey] {
        termsByEntity.keys.sorted()
    }

    public func duplicatesIgnored() -> Int {
        duplicates
    }

    private func index(_ key: EntityKey, record: Record?) {
        if let old = termsByEntity.removeValue(forKey: key) {
            for term in old {
                postings[term]?.remove(key)
                if postings[term]?.isEmpty == true { postings[term] = nil }
            }
        }
        guard let record else { return }
        var terms = Set<String>()
        for field in indexedFields {
            guard let text = record[field] else { continue }
            for word in text.lowercased().split(whereSeparator: { !$0.isLetter && !$0.isNumber }) {
                terms.insert(String(word))
            }
        }
        guard !terms.isEmpty else { return }
        termsByEntity[key] = terms
        for term in terms {
            postings[term, default: []].insert(key)
        }
    }
}
