#include "console_event_viewer.h"
#include <ftxui/component/component.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/component/loop.hpp>
#include <ftxui/dom/elements.hpp>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <algorithm>

namespace data_sdk::tools {

using namespace ftxui;

ConsoleEventViewer::ConsoleEventViewer(events::IGenericEventDispatcherPtr dispatcher)
    : m_dispatcher(std::move(dispatcher)) {}

ConsoleEventViewer::~ConsoleEventViewer() {
    Stop();
}

void ConsoleEventViewer::Start() {
    if (m_running.exchange(true)) return;

    // Subscribe to all events
    m_connection = m_dispatcher->Subscribe(
        [this](const events::GenericEvent& event) {
            HandleEvent(event);
        },
        events::GenericEventFilter::All()
    );

    // Start render thread
    m_renderThread = std::thread(&ConsoleEventViewer::RenderLoop, this);
}

void ConsoleEventViewer::Stop() {
    if (!m_running.exchange(false)) return;

    if (m_connection.connected()) {
        m_connection.disconnect();
    }

    if (m_renderThread.joinable()) {
        m_renderThread.join();
    }
}

void ConsoleEventViewer::HandleEvent(const events::GenericEvent& event) {
    std::lock_guard<std::mutex> lock(m_mutex);

    std::string pathStr = events::GetEventPath(event).ToString();

    std::visit([&](auto&& e) {
        using T = std::decay_t<decltype(e)>;

        if constexpr (std::is_same_v<T, events::LifecycleEvent>) {
            auto& node = m_nodeStates[pathStr];
            node.path = pathStr;
            node.operation_type = e.operation_type;
            node.operation_name = e.operation_name;
            node.status = e.status;

            if (e.status == events::OperationStatus::Started ||
                e.status == events::OperationStatus::InProgress) {
                node.start_time = std::chrono::steady_clock::now();
            } else if (e.status == events::OperationStatus::Completed ||
                       e.status == events::OperationStatus::Failed) {
                node.end_time = std::chrono::steady_clock::now();
            }

            if (e.error_message) {
                node.message = *e.error_message;
            }

            for (const auto& [k, v] : e.context) {
                std::ostringstream oss;
                oss << glz::write_json(v).value_or("");
                node.context[k] = oss.str();
            }
        }
        else if constexpr (std::is_same_v<T, events::ProgressEvent>) {
            auto& node = m_nodeStates[pathStr];
            node.path = pathStr;
            node.current = e.current.value_or(0);
            node.total = e.total.value_or(0);
            node.message = e.message;
            node.status = events::OperationStatus::InProgress;

            for (const auto& [k, v] : e.context) {
                std::ostringstream oss;
                oss << glz::write_json(v).value_or("");
                node.context[k] = oss.str();
            }
        }
        else if constexpr (std::is_same_v<T, events::LogEvent>) {
            std::ostringstream oss;
            oss << "[" << events::ToString(e.level) << "] " << pathStr << ": " << e.message;
            m_logs.push_back(oss.str());
            if (m_logs.size() > 10) m_logs.pop_front();
        }
    }, event);
}

void ConsoleEventViewer::RenderLoop() {
    auto screen = ScreenInteractive::TerminalOutput();

    auto renderer = Renderer([this] {
        return RenderState();
    });

    // Refresh thread posts events to trigger redraws
    std::atomic<bool> refreshing{true};
    std::thread refresh_thread([&screen, &refreshing] {
        while (refreshing.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            screen.Post(Event::Custom);
        }
    });

    // Run loop until stopped
    std::thread stop_watcher([this, &screen] {
        while (m_running.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        screen.Exit();
    });

    screen.Loop(renderer);

    refreshing.store(false);
    if (refresh_thread.joinable()) refresh_thread.join();
    if (stop_watcher.joinable()) stop_watcher.join();
}

Element ConsoleEventViewer::RenderState() {
    std::lock_guard<std::mutex> lock(m_mutex);

    Elements nodes;

    // Sort by path, but for nodes with exec_seq, sort by that within their parent
    std::vector<std::pair<std::string, NodeState>> sorted(
        m_nodeStates.begin(), m_nodeStates.end());
    std::sort(sorted.begin(), sorted.end(),
        [](const auto& a, const auto& b) {
            // Get parent paths
            auto aLastSlash = a.first.rfind('/');
            auto bLastSlash = b.first.rfind('/');
            std::string aParent = (aLastSlash != std::string::npos) ? a.first.substr(0, aLastSlash) : "";
            std::string bParent = (bLastSlash != std::string::npos) ? b.first.substr(0, bLastSlash) : "";

            // If same parent, try to sort by exec_seq
            if (aParent == bParent) {
                auto aSeqIt = a.second.context.find("exec_seq");
                auto bSeqIt = b.second.context.find("exec_seq");
                if (aSeqIt != a.second.context.end() && bSeqIt != b.second.context.end()) {
                    try {
                        return std::stol(aSeqIt->second) < std::stol(bSeqIt->second);
                    } catch (...) {}
                }
            }
            return a.first < b.first;
        });

    for (const auto& [path, node] : sorted) {
        size_t depth = std::count(path.begin(), path.end(), '/');

        auto lastSlash = path.rfind('/');
        std::string name = (lastSlash != std::string::npos)
            ? path.substr(lastSlash + 1) : path;

        Element icon;
        switch (node.status) {
            case events::OperationStatus::Pending:
                icon = text("○") | color(Color::GrayDark);
                break;
            case events::OperationStatus::Started:
            case events::OperationStatus::InProgress:
                icon = text("◐") | color(Color::Blue);
                break;
            case events::OperationStatus::Completed:
                icon = text("✓") | color(Color::Green);
                break;
            case events::OperationStatus::Failed:
                icon = text("✗") | color(Color::Red);
                break;
            case events::OperationStatus::Cancelled:
            case events::OperationStatus::Skipped:
                icon = text("⊘") | color(Color::Yellow);
                break;
        }

        Elements row;
        row.push_back(text(std::string(depth * 2, ' ')));
        row.push_back(icon);
        row.push_back(text(" " + name) | bold);

        // Show execution sequence for nodes (helps identify parallel execution order)
        auto execSeqIt = node.context.find("exec_seq");
        if (execSeqIt != node.context.end()) {
            row.push_back(text(" [#" + execSeqIt->second + "]") | color(Color::GrayDark));
        }

        if (node.status == events::OperationStatus::InProgress && node.total > 0) {
            float pct = static_cast<float>(node.current) / static_cast<float>(node.total);
            row.push_back(text(" "));
            row.push_back(gauge(pct) | size(WIDTH, EQUAL, 20) | color(Color::Green));
            row.push_back(text(" " + std::to_string(static_cast<int>(pct * 100)) + "%"));
            row.push_back(text(" (" + std::to_string(node.current) + "/" +
                              std::to_string(node.total) + ")") | dim);
        }

        if (node.status == events::OperationStatus::Completed ||
            node.status == events::OperationStatus::Failed) {
            row.push_back(text(" " + FormatDuration(node.end_time - node.start_time)) | dim);
        }

        nodes.push_back(hbox(std::move(row)));

        if (!node.context.empty() && node.status == events::OperationStatus::InProgress) {
            Elements metrics;
            for (const auto& [k, v] : node.context) {
                if (k == "loss" || k == "accuracy" || k == "epoch" || k == "lr") {
                    metrics.push_back(text("    " + k + ": " + v) | dim);
                }
            }
            if (!metrics.empty()) {
                nodes.push_back(hbox({
                    text(std::string(depth * 2, ' ')),
                    vbox(std::move(metrics))
                }));
            }
        }
    }

    Elements logElements;
    for (const auto& log : m_logs) {
        logElements.push_back(text(log) | dim);
    }

    return vbox({
        text("══════════════════════════════════════════════════════════════") | color(Color::Cyan),
        text("              EpochDataSDK Event Viewer") | color(Color::Cyan) | bold,
        text("══════════════════════════════════════════════════════════════") | color(Color::Cyan),
        text(""),
        vbox(std::move(nodes)),
        text(""),
        separator(),
        text(" Logs:") | bold,
        vbox(std::move(logElements)),
    });
}

std::string ConsoleEventViewer::StatusIcon(events::OperationStatus status) const {
    switch (status) {
        case events::OperationStatus::Pending:    return "○";
        case events::OperationStatus::Started:    return "◐";
        case events::OperationStatus::InProgress: return "◐";
        case events::OperationStatus::Completed:  return "✓";
        case events::OperationStatus::Failed:     return "✗";
        case events::OperationStatus::Cancelled:  return "⊘";
        case events::OperationStatus::Skipped:    return "⊘";
        default: return "?";
    }
}

std::string ConsoleEventViewer::FormatDuration(std::chrono::steady_clock::duration dur) const {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
    if (ms < 1000) return std::to_string(ms) + "ms";
    if (ms < 60000) return std::to_string(ms / 1000) + "." + std::to_string((ms % 1000) / 100) + "s";
    return std::to_string(ms / 60000) + "m " + std::to_string((ms % 60000) / 1000) + "s";
}

std::map<std::string, NodeState> ConsoleEventViewer::GetStateSnapshot() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_nodeStates;
}

std::deque<std::string> ConsoleEventViewer::GetRecentLogs(size_t max) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::deque<std::string> result;
    size_t count = std::min(max, m_logs.size());
    auto it = m_logs.end() - static_cast<std::ptrdiff_t>(count);
    for (; it != m_logs.end(); ++it) {
        result.push_back(*it);
    }
    return result;
}

// Factory function implementation
IEventViewerPtr MakeConsoleEventViewer(events::IGenericEventDispatcherPtr dispatcher) {
    return std::make_shared<ConsoleEventViewer>(std::move(dispatcher));
}

} // namespace data_sdk::tools
