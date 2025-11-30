#pragma once
#include <epoch_data_sdk/tools/event_viewer.h>
#include <epoch_data_sdk/events/all.h>
#include <ftxui/dom/elements.hpp>
#include <atomic>
#include <mutex>
#include <thread>

namespace data_sdk::tools {

/**
 * ConsoleEventViewer - FTXUI-based terminal event viewer
 *
 * Implements IEventViewer with a rich terminal UI showing:
 * - Hierarchical operation tree with status icons
 * - Progress bars for in-progress operations
 * - Timing information for completed operations
 * - Recent log messages
 */
class ConsoleEventViewer : public IEventViewer {
public:
    explicit ConsoleEventViewer(events::IGenericEventDispatcherPtr dispatcher);
    ~ConsoleEventViewer() override;

    void Start() override;
    void Stop() override;
    [[nodiscard]] bool IsRunning() const override { return m_running.load(); }

    [[nodiscard]] std::map<std::string, NodeState> GetStateSnapshot() const override;
    [[nodiscard]] std::deque<std::string> GetRecentLogs(size_t max = 10) const override;

private:
    void HandleEvent(const events::GenericEvent& event);
    void RenderLoop();
    ftxui::Element RenderState();
    std::string FormatDuration(std::chrono::steady_clock::duration dur) const;
    std::string StatusIcon(events::OperationStatus status) const;

    events::IGenericEventDispatcherPtr m_dispatcher;
    boost::signals2::connection m_connection;

    mutable std::mutex m_mutex;
    std::map<std::string, NodeState> m_nodeStates;
    std::deque<std::string> m_logs;

    std::atomic<bool> m_running{false};
    std::thread m_renderThread;
};

} // namespace data_sdk::tools
