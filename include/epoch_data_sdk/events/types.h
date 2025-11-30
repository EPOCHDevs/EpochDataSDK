#pragma once
#include "path.h"
#include <boost/signals2.hpp>
#include <chrono>
#include <glaze/json/generic.hpp>
#include <glaze/glaze.hpp>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>
#include <epoch_core/enum_wrapper.h>


/**
 * GenericEventType - Enum for type identification and filtering
 */
CREATE_ENUM(GenericEventType, Lifecycle, Progress, Metric, Summary, Log);


namespace data_sdk::events {

using Timestamp = std::chrono::steady_clock::time_point;
using JsonMetadata = std::map<std::string, glz::generic>;

/**
 * OperationStatus - Universal status for any operation
 *
 * State transitions:
 *   Pending -> Started -> InProgress -> Completed/Failed/Cancelled
 *   Pending -> Skipped (if operation is bypassed)
 */
enum class OperationStatus : uint8_t {
    Pending,     // Not yet started
    Started,     // Just began execution
    InProgress,  // Actively running (may have progress)
    Completed,   // Finished successfully
    Failed,      // Finished with error
    Cancelled,   // Terminated by user/system
    Skipped      // Bypassed (e.g., cache hit, condition not met)
};


/**
 * EventBase - Common fields for all generic events
 *
 * Every event has:
 * - timestamp: When the event occurred
 * - path: Hierarchical location (e.g., "job:abc/node:SMA")
 * - context: Domain-specific key-value metadata
 */
struct EventBase {
    Timestamp timestamp{std::chrono::steady_clock::now()};
    EventPath path{};
    JsonMetadata context{};  // Domain-specific data (e.g., {"asset": "AAPL", "rows": 1234})
};

/**
 * LifecycleEvent - Status transitions for any operation
 *
 * Replaces: PipelineStartedEvent, PipelineCompletedEvent, NodeStartedEvent, etc.
 * Used for: stage transitions, pipeline start/end, node start/end, data load start/end
 */
struct LifecycleEvent : EventBase {
    static constexpr epoch_core::GenericEventType TYPE = epoch_core::GenericEventType::Lifecycle;

    OperationStatus status{OperationStatus::Pending};
    std::string operation_type{};  // "stage", "pipeline", "node", "data_load", "asset_load"
    std::string operation_name{};  // Human-readable name (e.g., "SMA_20", "Loading market data")
    std::optional<std::chrono::milliseconds> duration{};  // Set on completion
    std::optional<std::string> error_message{};           // Set on failure

    // Counts for aggregate operations
    std::optional<size_t> items_succeeded{};
    std::optional<size_t> items_failed{};
    std::optional<size_t> items_skipped{};
    std::optional<size_t> items_total{};
};

/**
 * ProgressEvent - Progress updates during operation execution
 *
 * Replaces: TransformProgressEvent
 * Used for: ML training epochs, data loading progress, batch processing
 *
 * ML metrics are stored in context:
 *   context["loss"], context["accuracy"], context["learning_rate"], etc.
 */
struct ProgressEvent : EventBase {
    static constexpr epoch_core::GenericEventType TYPE = epoch_core::GenericEventType::Progress;

    std::optional<size_t> current{};      // Current step/item (e.g., epoch 67)
    std::optional<size_t> total{};        // Total steps/items (e.g., 100 epochs)
    std::optional<double> progress_percent{};  // 0.0 - 100.0
    std::string message{};                // Human-readable status
    std::optional<std::string> unit{};    // "epochs", "rows", "assets", "batches"

    // Estimated time remaining (if calculable)
    std::optional<std::chrono::milliseconds> estimated_remaining{};

    // Convenience: calculate percent if current/total available
    [[nodiscard]] double GetPercentOrCompute() const {
        if (progress_percent.has_value()) {
            return *progress_percent;
        }
        if (current.has_value() && total.has_value() && *total > 0) {
            return (static_cast<double>(*current) / static_cast<double>(*total)) * 100.0;
        }
        return 0.0;
    }
};

/**
 * MetricEvent - Single metric value emission
 *
 * Used for: training metrics, performance counters, business metrics
 */
struct MetricEvent : EventBase {
    static constexpr epoch_core::GenericEventType TYPE = epoch_core::GenericEventType::Metric;

    std::string metric_name{};  // "loss", "accuracy", "processing_time", "cache_hit_rate"
    double value{};
    std::optional<std::string> unit{};   // "ms", "%", "rows/sec"
    std::optional<double> min_value{};   // For range tracking
    std::optional<double> max_value{};   // For range tracking
};

/**
 * SummaryEvent - Aggregate status across multiple operations
 *
 * Replaces: ProgressSummaryEvent
 * Used for: Overall pipeline progress, stage summaries
 */
struct SummaryEvent : EventBase {
    static constexpr epoch_core::GenericEventType TYPE = epoch_core::GenericEventType::Summary;

    double overall_progress_percent{0.0};
    size_t operations_completed{0};
    size_t operations_total{0};
    size_t operations_failed{0};
    size_t operations_running{0};
    std::vector<std::string> currently_running{};  // List of running operation names
    std::optional<std::chrono::milliseconds> estimated_remaining{};
};

/**
 * LogEvent - Textual log messages with severity
 *
 * Used for: Debug output, warnings, errors that don't change status
 */
struct LogEvent : EventBase {
    static constexpr epoch_core::GenericEventType TYPE = epoch_core::GenericEventType::Log;

    enum class Level : uint8_t {
        Debug,
        Info,
        Warning,
        Error
    };

    Level level{Level::Info};
    std::string message{};
    std::optional<std::string> source{};  // e.g., "rolling_lightgbm", "data_loader"
};

/**
 * GenericEvent - Variant of all generic event types
 */
using GenericEvent = std::variant<
    LifecycleEvent,
    ProgressEvent,
    MetricEvent,
    SummaryEvent,
    LogEvent
>;

/**
 * Type traits for generic events
 */
template <typename T>
struct GenericEventTypeFor;

template <>
struct GenericEventTypeFor<LifecycleEvent> {
    static constexpr epoch_core::GenericEventType value = epoch_core::GenericEventType::Lifecycle;
};
template <>
struct GenericEventTypeFor<ProgressEvent> {
    static constexpr epoch_core::GenericEventType value = epoch_core::GenericEventType::Progress;
};
template <>
struct GenericEventTypeFor<MetricEvent> {
    static constexpr epoch_core::GenericEventType value = epoch_core::GenericEventType::Metric;
};
template <>
struct GenericEventTypeFor<SummaryEvent> {
    static constexpr epoch_core::GenericEventType value = epoch_core::GenericEventType::Summary;
};
template <>
struct GenericEventTypeFor<LogEvent> {
    static constexpr epoch_core::GenericEventType value = epoch_core::GenericEventType::Log;
};

/**
 * Get event type from variant
 */
inline epoch_core::GenericEventType GetEventType(const GenericEvent& event) {
    return std::visit([]([[maybe_unused]] const auto& e) {
        return GenericEventTypeFor<std::decay_t<decltype(e)>>::value;
    }, event);
}

/**
 * Get event path from variant
 */
inline const EventPath& GetEventPath(const GenericEvent& event) {
    return std::visit([](const auto& e) -> const EventPath& {
        return e.path;
    }, event);
}

/**
 * Get event timestamp from variant
 */
inline Timestamp GetEventTimestamp(const GenericEvent& event) {
    return std::visit([](const auto& e) {
        return e.timestamp;
    }, event);
}

/**
 * Set event path in variant
 */
inline void SetEventPath(GenericEvent& event, EventPath path) {
    std::visit([&path](auto& e) {
        e.path = std::move(path);
    }, event);
}

/**
 * Helper to convert OperationStatus to string
 */
inline std::string ToString(OperationStatus status) {
    switch (status) {
        case OperationStatus::Pending:    return "pending";
        case OperationStatus::Started:    return "started";
        case OperationStatus::InProgress: return "in_progress";
        case OperationStatus::Completed:  return "completed";
        case OperationStatus::Failed:     return "failed";
        case OperationStatus::Cancelled:  return "cancelled";
        case OperationStatus::Skipped:    return "skipped";
        default: return "unknown";
    }
}

/**
 * Helper to convert GenericEventType to string
 */
inline std::string ToString(epoch_core::GenericEventType type) {
    return epoch_core::GenericEventTypeWrapper::ToString(type);
}

/**
 * Helper to convert LogEvent::Level to string
 */
inline std::string ToString(LogEvent::Level level) {
    switch (level) {
        case LogEvent::Level::Debug:   return "debug";
        case LogEvent::Level::Info:    return "info";
        case LogEvent::Level::Warning: return "warn";
        case LogEvent::Level::Error:   return "error";
        default: return "unknown";
    }
}

/**
 * Timestamp helper - Note: This is also defined in event_types.h
 * Using inline to avoid ODR violations
 */
inline Timestamp Now() {
    return std::chrono::steady_clock::now();
}

template <typename Duration>
inline std::chrono::milliseconds ToMillis(Duration d) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(d);
}

// Signal types for generic events
using GenericEventSignal = boost::signals2::signal<void(const GenericEvent&)>;
using GenericEventSlot = GenericEventSignal::slot_type;

} // namespace data_sdk::events
