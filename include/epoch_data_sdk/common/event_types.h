#pragma once
#include <boost/signals2/signal.hpp>
#include <chrono>
#include <glaze/json/generic.hpp>
#include <glaze/glaze.hpp>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace data_sdk::events {

using Timestamp = std::chrono::steady_clock::time_point;
using JsonMetadata = std::map<std::string, glz::generic>;

struct PipelineStartedEvent {
    Timestamp timestamp{};
    std::size_t total_nodes{};
    std::size_t total_assets{};
    std::vector<std::string> node_ids;
};

struct PipelineCompletedEvent {
    Timestamp timestamp{};
    std::chrono::milliseconds duration{};
    std::size_t nodes_succeeded{};
    std::size_t nodes_failed{};
    std::size_t nodes_skipped{};
};

struct PipelineFailedEvent {
    Timestamp timestamp{};
    std::chrono::milliseconds elapsed{};
    std::string error_message;
};

struct PipelineCancelledEvent {
    Timestamp timestamp{};
    std::chrono::milliseconds elapsed{};
    std::size_t nodes_completed{};
    std::size_t nodes_total{};
};

struct NodeStartedEvent {
    Timestamp timestamp{};
    std::string node_id;
    std::string operation_name;
    bool is_cross_sectional{false};
    std::size_t node_index{};
    std::size_t total_nodes{};
    std::size_t asset_count{};
};

struct NodeCompletedEvent {
    Timestamp timestamp{};
    std::string node_id;
    std::string operation_name;
    std::chrono::milliseconds duration{};
    std::size_t assets_processed{};
    std::size_t assets_failed{};
};

struct NodeFailedEvent {
    Timestamp timestamp{};
    std::string node_id;
    std::string operation_name;
    std::string error_message;
    std::optional<std::string> asset_id;
};

struct NodeSkippedEvent {
    Timestamp timestamp{};
    std::string node_id;
    std::string operation_name;
    std::string reason;
};

struct TransformProgressEvent {
    Timestamp timestamp{};
    std::string node_id;
    std::string operation_name;
    std::optional<std::string> asset_id;
    std::optional<std::size_t> current_step;
    std::optional<std::size_t> total_steps;
    std::optional<double> progress_percent;
    std::optional<double> loss;
    std::optional<double> accuracy;
    std::optional<double> learning_rate;
    std::optional<std::size_t> iteration;
    JsonMetadata metadata;
    std::string message;
};

struct ProgressSummaryEvent {
    Timestamp timestamp{};
    double overall_progress_percent{};
    std::size_t nodes_completed{};
    std::size_t nodes_total{};
    std::vector<std::string> currently_running;
    std::optional<std::chrono::milliseconds> estimated_remaining;
};

using OrchestratorEvent = std::variant<
    PipelineStartedEvent,
    PipelineCompletedEvent,
    PipelineFailedEvent,
    PipelineCancelledEvent,
    NodeStartedEvent,
    NodeCompletedEvent,
    NodeFailedEvent,
    NodeSkippedEvent,
    TransformProgressEvent,
    ProgressSummaryEvent
>;

enum class EventType : std::uint8_t {
    PipelineStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineCancelled,
    NodeStarted,
    NodeCompleted,
    NodeFailed,
    NodeSkipped,
    TransformProgress,
    ProgressSummary
};

template <typename T> struct EventTypeFor;
template <> struct EventTypeFor<PipelineStartedEvent> { static constexpr EventType value = EventType::PipelineStarted; };
template <> struct EventTypeFor<PipelineCompletedEvent> { static constexpr EventType value = EventType::PipelineCompleted; };
template <> struct EventTypeFor<PipelineFailedEvent> { static constexpr EventType value = EventType::PipelineFailed; };
template <> struct EventTypeFor<PipelineCancelledEvent> { static constexpr EventType value = EventType::PipelineCancelled; };
template <> struct EventTypeFor<NodeStartedEvent> { static constexpr EventType value = EventType::NodeStarted; };
template <> struct EventTypeFor<NodeCompletedEvent> { static constexpr EventType value = EventType::NodeCompleted; };
template <> struct EventTypeFor<NodeFailedEvent> { static constexpr EventType value = EventType::NodeFailed; };
template <> struct EventTypeFor<NodeSkippedEvent> { static constexpr EventType value = EventType::NodeSkipped; };
template <> struct EventTypeFor<TransformProgressEvent> { static constexpr EventType value = EventType::TransformProgress; };
template <> struct EventTypeFor<ProgressSummaryEvent> { static constexpr EventType value = EventType::ProgressSummary; };

inline EventType GetEventType(const OrchestratorEvent& event) {
    return std::visit([](const auto& e) { return EventTypeFor<std::decay_t<decltype(e)>>::value; }, event);
}

using OrchestratorEventSignal = boost::signals2::signal<void(const OrchestratorEvent&)>;
using OrchestratorEventSlot = OrchestratorEventSignal::slot_type;

inline Timestamp Now() {
    return std::chrono::steady_clock::now();
}

template <typename Duration>
inline std::chrono::milliseconds ToMillis(Duration d) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(d);
}

} // namespace data_sdk::events
