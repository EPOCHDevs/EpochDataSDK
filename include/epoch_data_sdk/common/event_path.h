#pragma once
#include <algorithm>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace data_sdk::events {

/**
 * EventPath - Hierarchical path identification for events
 *
 * Supports paths like:
 *   "job:abc123"
 *   "job:abc123/stage:RunCampaign"
 *   "job:abc123/stage:RunCampaign/pipeline:transforms/node:SMA_20/asset:AAPL"
 *
 * Used for:
 *   - Hierarchical event filtering (subscribe to events from specific subtrees)
 *   - Building tree structures in UI (parent-child relationships)
 *   - Scoped progress emission (transforms emit events with their path)
 *
 * Thread-safe: Immutable value type, safe to copy/share across threads.
 */
class EventPath {
public:
    struct Segment {
        std::string scope;  // e.g., "job", "stage", "pipeline", "node", "asset"
        std::string id;     // e.g., "abc123", "RunCampaign", "SMA_20", "AAPL"

        bool operator==(const Segment& other) const {
            return scope == other.scope && id == other.id;
        }

        bool operator!=(const Segment& other) const {
            return !(*this == other);
        }
    };

    // Default constructor - creates empty path
    EventPath() = default;

    // Construct from single segment
    EventPath(const std::string& scope, const std::string& id)
        : m_segments{{scope, id}} {}

    // Construct from initializer list of segments
    EventPath(std::initializer_list<Segment> segments)
        : m_segments(segments) {}

    // Construct from vector of segments
    explicit EventPath(std::vector<Segment> segments)
        : m_segments(std::move(segments)) {}

    // Parse from string representation
    // Format: "scope:id/scope:id/..."
    // Example: "job:abc123/stage:RunCampaign/node:SMA_20"
    static EventPath Parse(const std::string& path) {
        if (path.empty()) {
            return EventPath{};
        }

        std::vector<Segment> segments;
        std::istringstream stream(path);
        std::string part;

        while (std::getline(stream, part, '/')) {
            if (part.empty()) continue;

            auto colonPos = part.find(':');
            if (colonPos == std::string::npos) {
                throw std::invalid_argument(
                    "Invalid EventPath segment (missing colon): " + part);
            }

            segments.push_back({
                part.substr(0, colonPos),
                part.substr(colonPos + 1)
            });
        }

        return EventPath(std::move(segments));
    }

    // Create a child path with additional segment
    [[nodiscard]] EventPath Child(const std::string& scope, const std::string& id) const {
        std::vector<Segment> newSegments = m_segments;
        newSegments.push_back({scope, id});
        return EventPath(std::move(newSegments));
    }

    // Get parent path (without last segment)
    // Returns empty path if already at root
    [[nodiscard]] EventPath Parent() const {
        if (m_segments.empty()) {
            return EventPath{};
        }
        std::vector<Segment> parentSegments(m_segments.begin(), m_segments.end() - 1);
        return EventPath(std::move(parentSegments));
    }

    // Check if this path is a descendant of (or equal to) the ancestor path
    [[nodiscard]] bool IsDescendantOf(const EventPath& ancestor) const {
        if (ancestor.m_segments.size() > m_segments.size()) {
            return false;
        }

        return std::equal(
            ancestor.m_segments.begin(),
            ancestor.m_segments.end(),
            m_segments.begin()
        );
    }

    // Check if this path is an ancestor of (or equal to) the descendant path
    [[nodiscard]] bool IsAncestorOf(const EventPath& descendant) const {
        return descendant.IsDescendantOf(*this);
    }

    // Get the value of a specific scope segment
    // Returns nullopt if scope not found
    [[nodiscard]] std::optional<std::string> GetSegment(const std::string& scope) const {
        for (const auto& seg : m_segments) {
            if (seg.scope == scope) {
                return seg.id;
            }
        }
        return std::nullopt;
    }

    // Get the last segment's scope (e.g., "node" for "job:a/node:SMA")
    [[nodiscard]] std::optional<std::string> GetLastScope() const {
        if (m_segments.empty()) {
            return std::nullopt;
        }
        return m_segments.back().scope;
    }

    // Get the last segment's id (e.g., "SMA" for "job:a/node:SMA")
    [[nodiscard]] std::optional<std::string> GetLastId() const {
        if (m_segments.empty()) {
            return std::nullopt;
        }
        return m_segments.back().id;
    }

    // Get the first segment (typically the root, e.g., "job:abc123")
    [[nodiscard]] std::optional<Segment> GetRoot() const {
        if (m_segments.empty()) {
            return std::nullopt;
        }
        return m_segments.front();
    }

    // Serialize to string
    [[nodiscard]] std::string ToString() const {
        if (m_segments.empty()) {
            return "";
        }

        std::ostringstream oss;
        bool first = true;
        for (const auto& seg : m_segments) {
            if (!first) {
                oss << '/';
            }
            oss << seg.scope << ':' << seg.id;
            first = false;
        }
        return oss.str();
    }

    // Get depth (number of segments)
    [[nodiscard]] size_t Depth() const {
        return m_segments.size();
    }

    // Check if empty
    [[nodiscard]] bool IsEmpty() const {
        return m_segments.empty();
    }

    // Access segments directly
    [[nodiscard]] const std::vector<Segment>& Segments() const {
        return m_segments;
    }

    // Equality operators
    bool operator==(const EventPath& other) const {
        return m_segments == other.m_segments;
    }

    bool operator!=(const EventPath& other) const {
        return !(*this == other);
    }

    // Less-than for use in ordered containers
    bool operator<(const EventPath& other) const {
        return ToString() < other.ToString();
    }

    // Hash support for unordered containers
    struct Hash {
        size_t operator()(const EventPath& path) const {
            return std::hash<std::string>{}(path.ToString());
        }
    };

private:
    std::vector<Segment> m_segments;
};

// Convenience factory functions
inline EventPath MakeEventPath(const std::string& scope, const std::string& id) {
    return EventPath(scope, id);
}

inline EventPath MakeJobPath(const std::string& jobId) {
    return EventPath("job", jobId);
}

inline EventPath MakeStagePath(const std::string& jobId, const std::string& stageName) {
    return MakeJobPath(jobId).Child("stage", stageName);
}

inline EventPath MakePipelinePath(const std::string& jobId,
                                   const std::string& stageName,
                                   const std::string& pipelineName) {
    return MakeStagePath(jobId, stageName).Child("pipeline", pipelineName);
}

inline EventPath MakeNodePath(const std::string& jobId,
                               const std::string& stageName,
                               const std::string& pipelineName,
                               const std::string& nodeId) {
    return MakePipelinePath(jobId, stageName, pipelineName).Child("node", nodeId);
}

inline EventPath MakeAssetPath(const std::string& jobId,
                                const std::string& stageName,
                                const std::string& pipelineName,
                                const std::string& nodeId,
                                const std::string& assetId) {
    return MakeNodePath(jobId, stageName, pipelineName, nodeId).Child("asset", assetId);
}

} // namespace data_sdk::events
