// swift-tools-version:5.9
import PackageDescription

// Three modules, one direction of dependency:
//   ChangeFeed (core: history port, lanes, cursors, audit) <- ChangeFeedAdapters (feature-owned consumers)
//                                                          <- ChangeFeedUI (SwiftUI console, Apple platforms only)
// The core never imports an adapter. That rule is enforced by this graph, not by a code-review comment.
let package = Package(
    name: "ChangeFeedSpine",
    platforms: [
        .iOS(.v17),
        .macOS(.v14)
    ],
    products: [
        .library(name: "ChangeFeed", targets: ["ChangeFeed"]),
        .library(name: "ChangeFeedAdapters", targets: ["ChangeFeedAdapters"]),
        .library(name: "ChangeFeedUI", targets: ["ChangeFeedUI"])
    ],
    targets: [
        .target(name: "ChangeFeed"),
        .target(name: "ChangeFeedAdapters", dependencies: ["ChangeFeed"]),
        .target(name: "ChangeFeedUI", dependencies: ["ChangeFeed", "ChangeFeedAdapters"]),
        .testTarget(name: "ChangeFeedTests", dependencies: ["ChangeFeed", "ChangeFeedAdapters"])
    ]
)
