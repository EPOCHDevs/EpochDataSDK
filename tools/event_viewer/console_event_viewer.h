#pragma once
#include <epoch_data_sdk/events/all.h>
#include <ftxui/dom/elements.hpp>
#include <atomic>
#include <mutex>
#include <thread>
#include <map>
#include <deque>
#include <functional>

namespace data_sdk::tools {

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

class ConsoleEventViewer {
public:
    explicit ConsoleEventViewer(events::IGenericEventDispatcherPtr dispatcher);
    ~ConsoleEventViewer();

    void Start();
    void Stop();
    bool IsRunning() const { return m_running.load(); }

    // Get current state snapshot (thread-safe)
    std::map<std::string, NodeState> GetStateSnapshot() const;
    std::deque<std::string> GetRecentLogs(size_t max = 10) const;

private:
    void HandleEvent(const events::GenericEvent& event);
    void RenderLoop();
    ftxui::Element RenderState();  // Returns FTXUI element for rendering
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

using ConsoleEventViewerPtr = std::shared_ptr<ConsoleEventViewer>;

inline ConsoleEventViewerPtr MakeConsoleEventViewer(events::IGenericEventDispatcherPtr dispatcher) {
    return std::make_shared<ConsoleEventViewer>(std::move(dispatcher));
}

} // namespace data_sdk::tools
