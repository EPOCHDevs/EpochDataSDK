#pragma once
#include "generic_event_types.h"
#include <functional>
#include <optional>
#include <set>

namespace data_sdk::events {

// Bring enum into local namespace for convenience
using epoch_core::GenericEventType;

/**
 * GenericEventFilter - Filter events by type and/or path
 *
 * Supports:
 * - Type filtering: Only lifecycle events, only progress events, etc.
 * - Path prefix filtering: Only events from "job:abc/stage:Run" subtree
 * - Depth filtering: Only events at or below certain depth
 * - Custom predicates: Arbitrary filter logic
 *
 * Filters are composable using operator& (AND) and operator| (OR).
 */
class GenericEventFilter {
public:
    using Predicate = std::function<bool(const GenericEvent&)>;

    // Factory methods for common filters

    /// Accept all events
    static GenericEventFilter All();

    /// Accept no events
    static GenericEventFilter None();

    /// Accept only specific event types
    static GenericEventFilter OnlyTypes(std::initializer_list<GenericEventType> types);

    /// Accept all except specific types
    static GenericEventFilter ExceptTypes(std::initializer_list<GenericEventType> types);

    /// Accept only lifecycle events
    static GenericEventFilter LifecycleOnly();

    /// Accept only progress events
    static GenericEventFilter ProgressOnly();

    /// Accept only log events
    static GenericEventFilter LogOnly();

    /// Accept only summary events
    static GenericEventFilter SummaryOnly();

    // Path-based filters

    /// Accept events whose path starts with the given prefix
    [[nodiscard]] GenericEventFilter WithPathPrefix(const EventPath& prefix) const;

    /// Accept events at or below the given depth (0 = root, 1 = first child, etc.)
    [[nodiscard]] GenericEventFilter AtOrBelowDepth(size_t maxDepth) const;

    /// Accept events at exactly the given depth
    [[nodiscard]] GenericEventFilter AtDepth(size_t depth) const;

    /// Accept events where path contains a specific scope
    [[nodiscard]] GenericEventFilter WithScope(const std::string& scope) const;

    /// Accept events where path has a specific segment value
    [[nodiscard]] GenericEventFilter WithSegment(const std::string& scope,
                                                   const std::string& id) const;

    // Custom filter

    /// Add arbitrary predicate filter
    [[nodiscard]] GenericEventFilter WithPredicate(Predicate pred) const;

    // Evaluation

    /// Check if filter accepts the given event
    [[nodiscard]] bool Accepts(const GenericEvent& event) const;

    /// Check if filter accepts the given event type
    [[nodiscard]] bool AcceptsType(GenericEventType type) const;

    // Composition

    /// AND composition - both filters must accept
    GenericEventFilter operator&(const GenericEventFilter& other) const;

    /// OR composition - either filter must accept
    GenericEventFilter operator|(const GenericEventFilter& other) const;

    /// NOT - invert filter
    [[nodiscard]] GenericEventFilter Negate() const;

private:
    GenericEventFilter() = default;

    // Type filter (nullopt = accept all types)
    std::optional<std::set<GenericEventType>> m_allowedTypes;
    bool m_isTypeWhitelist{true};  // true = whitelist, false = blacklist

    // Path filter
    std::optional<EventPath> m_pathPrefix;
    std::optional<size_t> m_maxDepth;
    std::optional<size_t> m_exactDepth;
    std::optional<std::string> m_requiredScope;
    std::optional<EventPath::Segment> m_requiredSegment;

    // Custom predicates
    std::vector<Predicate> m_predicates;

    // For composition
    std::vector<GenericEventFilter> m_andFilters;
    std::vector<GenericEventFilter> m_orFilters;
    bool m_negated{false};

    bool EvaluateSelf(const GenericEvent& event) const;
};

// Implementation

inline GenericEventFilter GenericEventFilter::All() {
    return GenericEventFilter{};
}

inline GenericEventFilter GenericEventFilter::None() {
    GenericEventFilter f;
    f.m_allowedTypes = std::set<GenericEventType>{};
    return f;
}

inline GenericEventFilter GenericEventFilter::OnlyTypes(
    std::initializer_list<GenericEventType> types) {
    GenericEventFilter f;
    f.m_allowedTypes = std::set<GenericEventType>(types);
    f.m_isTypeWhitelist = true;
    return f;
}

inline GenericEventFilter GenericEventFilter::ExceptTypes(
    std::initializer_list<GenericEventType> types) {
    GenericEventFilter f;
    f.m_allowedTypes = std::set<GenericEventType>(types);
    f.m_isTypeWhitelist = false;
    return f;
}

inline GenericEventFilter GenericEventFilter::LifecycleOnly() {
    return OnlyTypes({GenericEventType::Lifecycle});
}

inline GenericEventFilter GenericEventFilter::ProgressOnly() {
    return OnlyTypes({GenericEventType::Progress});
}

inline GenericEventFilter GenericEventFilter::LogOnly() {
    return OnlyTypes({GenericEventType::Log});
}

inline GenericEventFilter GenericEventFilter::SummaryOnly() {
    return OnlyTypes({GenericEventType::Summary});
}

inline GenericEventFilter GenericEventFilter::WithPathPrefix(const EventPath& prefix) const {
    GenericEventFilter f = *this;
    f.m_pathPrefix = prefix;
    return f;
}

inline GenericEventFilter GenericEventFilter::AtOrBelowDepth(size_t maxDepth) const {
    GenericEventFilter f = *this;
    f.m_maxDepth = maxDepth;
    return f;
}

inline GenericEventFilter GenericEventFilter::AtDepth(size_t depth) const {
    GenericEventFilter f = *this;
    f.m_exactDepth = depth;
    return f;
}

inline GenericEventFilter GenericEventFilter::WithScope(const std::string& scope) const {
    GenericEventFilter f = *this;
    f.m_requiredScope = scope;
    return f;
}

inline GenericEventFilter GenericEventFilter::WithSegment(const std::string& scope,
                                                            const std::string& id) const {
    GenericEventFilter f = *this;
    f.m_requiredSegment = EventPath::Segment{scope, id};
    return f;
}

inline GenericEventFilter GenericEventFilter::WithPredicate(Predicate pred) const {
    GenericEventFilter f = *this;
    f.m_predicates.push_back(std::move(pred));
    return f;
}

inline bool GenericEventFilter::AcceptsType(GenericEventType type) const {
    if (!m_allowedTypes.has_value()) {
        return true;  // No type filter = accept all
    }

    bool found = m_allowedTypes->count(type) > 0;
    return m_isTypeWhitelist ? found : !found;
}

inline bool GenericEventFilter::EvaluateSelf(const GenericEvent& event) const {
    // Type filter
    if (!AcceptsType(GetEventType(event))) {
        return false;
    }

    const EventPath& path = GetEventPath(event);

    // Path prefix filter
    if (m_pathPrefix.has_value() && !path.IsDescendantOf(*m_pathPrefix)) {
        return false;
    }

    // Depth filters
    if (m_maxDepth.has_value() && path.Depth() > *m_maxDepth) {
        return false;
    }

    if (m_exactDepth.has_value() && path.Depth() != *m_exactDepth) {
        return false;
    }

    // Scope filter
    if (m_requiredScope.has_value()) {
        bool hasScope = false;
        for (const auto& seg : path.Segments()) {
            if (seg.scope == *m_requiredScope) {
                hasScope = true;
                break;
            }
        }
        if (!hasScope) return false;
    }

    // Segment filter
    if (m_requiredSegment.has_value()) {
        auto value = path.GetSegment(m_requiredSegment->scope);
        if (!value.has_value() || *value != m_requiredSegment->id) {
            return false;
        }
    }

    // Custom predicates
    for (const auto& pred : m_predicates) {
        if (!pred(event)) {
            return false;
        }
    }

    return true;
}

inline bool GenericEventFilter::Accepts(const GenericEvent& event) const {
    bool selfResult = EvaluateSelf(event);

    if (m_negated) {
        selfResult = !selfResult;
    }

    // AND composition: all must pass
    if (!m_andFilters.empty()) {
        if (!selfResult) return false;
        for (const auto& f : m_andFilters) {
            if (!f.Accepts(event)) return false;
        }
        return true;
    }

    // OR composition: any must pass
    if (!m_orFilters.empty()) {
        if (selfResult) return true;
        for (const auto& f : m_orFilters) {
            if (f.Accepts(event)) return true;
        }
        return false;
    }

    return selfResult;
}

inline GenericEventFilter GenericEventFilter::operator&(const GenericEventFilter& other) const {
    GenericEventFilter f = *this;
    f.m_andFilters.push_back(other);
    return f;
}

inline GenericEventFilter GenericEventFilter::operator|(const GenericEventFilter& other) const {
    GenericEventFilter f = *this;
    f.m_orFilters.push_back(other);
    return f;
}

inline GenericEventFilter GenericEventFilter::Negate() const {
    GenericEventFilter f = *this;
    f.m_negated = !f.m_negated;
    return f;
}

} // namespace data_sdk::events
