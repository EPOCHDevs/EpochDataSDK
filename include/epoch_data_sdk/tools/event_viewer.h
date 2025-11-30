#pragma once
#include <epoch_data_sdk/events/dispatcher.h>
#include <epoch_data_sdk/events/types.h>
#include <deque>
#include <map>
#include <memory>
#include <string>

namespace data_sdk::tools {

/**
 * NodeState - Represents the state of a single operation node in the event viewer
 */
struct NodeState {
    std::string path;
    std::string operation_type;
    std::string operation_name;
    events::OperationStatus status = events::OperationStatus::Pending;
    size_t current = 0;
    size_t total = 0;
    std::string message;
    std::map<std::string, std::string> context;
    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point end_time;
};

/**
 * IEventViewer - Interface for event viewing tools
 *
 * Provides a unified interface for visualizing SDK events.
 * Implementations can render to console (FTXUI), web UI, or other outputs.
 */
class IEventViewer {
public:
    virtual ~IEventViewer() = default;

    /// Start the event viewer (begins listening and rendering)
    virtual void Start() = 0;

    /// Stop the event viewer
    virtual void Stop() = 0;

    /// Check if the viewer is currently running
    [[nodiscard]] virtual bool IsRunning() const = 0;

    /// Get a snapshot of all tracked node states (thread-safe)
    [[nodiscard]] virtual std::map<std::string, NodeState> GetStateSnapshot() const = 0;

    /// Get recent log messages (thread-safe)
    [[nodiscard]] virtual std::deque<std::string> GetRecentLogs(size_t max = 10) const = 0;
};

using IEventViewerPtr = std::shared_ptr<IEventViewer>;

/**
 * Factory function to create a console-based event viewer
 *
 * Creates an FTXUI-based terminal UI that displays:
 * - Hierarchical operation tree with status icons
 * - Progress bars for in-progress operations
 * - Timing information for completed operations
 * - Recent log messages
 *
 * @param dispatcher The event dispatcher to subscribe to
 * @return A shared pointer to the event viewer
 */
IEventViewerPtr MakeConsoleEventViewer(events::IGenericEventDispatcherPtr dispatcher);

} // namespace data_sdk::tools