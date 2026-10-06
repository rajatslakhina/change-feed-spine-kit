/// Total arithmetic for every counter in the feed.
///
/// History tokens, lag, backoff delays and tick deadlines are all derived from values
/// that are either externally supplied (a persisted cursor, a clock reading) or grow
/// without an upper bound in a long-lived process. Plain `+`, `-`, `*` and `<<` trap
/// on overflow, so every one of those paths goes through here instead.
public enum SaturatingMath {
    /// `a + b`, clamped to `UInt64.max`.
    @inlinable
    public static func add(_ a: UInt64, _ b: UInt64) -> UInt64 {
        let (sum, overflow) = a.addingReportingOverflow(b)
        return overflow ? .max : sum
    }

    /// `a - b`, clamped to `0`.
    @inlinable
    public static func subtract(_ a: UInt64, _ b: UInt64) -> UInt64 {
        a > b ? a - b : 0
    }

    /// `a * b`, clamped to `UInt64.max`.
    @inlinable
    public static func multiply(_ a: UInt64, _ b: UInt64) -> UInt64 {
        let (product, overflow) = a.multipliedReportingOverflow(by: b)
        return overflow ? .max : product
    }

    /// `base * 2^exponent`, clamped to `UInt64.max`. Never shifts by 64 or more.
    @inlinable
    public static func doubling(_ base: UInt64, times exponent: Int) -> UInt64 {
        guard exponent > 0 else { return base }
        guard base != 0 else { return 0 }
        // Each doubling at least doubles a non-zero base, so 64 doublings saturate any UInt64.
        guard exponent < 64 else { return .max }
        let leadingZeros = base.leadingZeroBitCount
        guard exponent <= leadingZeros else { return .max }
        return base << UInt64(exponent)
    }

    /// `a + b` for `Int` counters, clamped to `Int.max` / `Int.min`.
    @inlinable
    public static func add(_ a: Int, _ b: Int) -> Int {
        let (sum, overflow) = a.addingReportingOverflow(b)
        if overflow { return b > 0 ? .max : .min }
        return sum
    }

    /// Converts a `UInt64` to `Int` without trapping (`Int` is 32-bit on some platforms).
    @inlinable
    public static func clampedInt(_ value: UInt64) -> Int {
        value > UInt64(Int.max) ? Int.max : Int(value)
    }
}
