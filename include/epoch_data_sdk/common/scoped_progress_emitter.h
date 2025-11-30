#pragma once
#include "cancellation_token.h"
#include "generic_event_dispatcher.h"
#include <chrono>
#include <format>
#include <memory>

namespace data_sdk::events {

/**
 * ScopedProgressEmitter - Emit events with hierarchical paths
 *
 * Key features:
 * 1. Creates child scopes that inherit path prefix
 * 2. Checks cancellation token on emission
 * 3. Tracks operation timing for lifecycle events
 * 4. Supports context metadata that propagates to all events
 *
 * Usage:
 *   auto rootEmitter = ScopedProgressEmitter(dispatcher, token, MakeJobPath("job123"));
 *   rootEmitter.EmitStarted("stage", "DataLoading");
 *
 *   auto nodeEmitter = rootEmitter.ChildScope("node", "SMA_20");
 *   nodeEmitter.EmitStarted("transform", "Simple Moving Average");
 *   nodeEmitter.EmitProgress(50, 100, "Processing rows");
 *   nodeEmitter.EmitCompleted("transform", "Simple Moving Average", duration);
 *
 * Thread safety: Safe to use from multiple threads if dispatcher is thread-safe.
 * The emitter itself is immutable except for context, which uses atomic operations.
 */
class ScopedProgressEmitter {
public:
    ScopedProgressEmitter(
        IGenericEventDispatcherPtr dispatcher,
        CancellationTokenPtr cancellationToken,
        EventPath basePath);

    // Default constructor creates a no-op emitter
    ScopedProgressEmitter();

    // Copy/move
    ScopedProgressEmitter(const ScopedProgressEmitter&) = default;
    ScopedProgressEmitter& operator=(const ScopedProgressEmitter&) = default;
    ScopedProgressEmitter(ScopedProgressEmitter&&) = default;
    ScopedProgressEmitter& operator=(ScopedProgressEmitter&&) = default;

    // ===================
    // Scope management
    // ===================

    /// Create a child emitter with extended path
    [[nodiscard]] ScopedProgressEmitter ChildScope(
        ScopeType scope,
        const std::string& id) const;

    /// Get the base path for this emitter
    [[nodiscard]] const EventPath& GetPath() const;

    /// Check if this emitter is functional (has dispatcher)
    [[nodiscard]] bool IsActive() const;

    // ===================
    // Context management
    // ===================

    /// Set a context value (will be included in all emitted events)
    void SetContext(const std::string& key, const glz::generic& value);

    /// Set context from common types
    void SetContext(const std::string& key, int64_t value);
    void SetContext(const std::string& key, double value);
    void SetContext(const std::string& key, const std::string& value);
    void SetContext(const std::string& key, bool value);

    /// Get current context
    [[nodiscard]] const JsonMetadata& GetContext() const;

    /// Clear context
    void ClearContext();

    // ===================
    // Cancellation
    // ===================

    /// Check if operation is cancelled
    [[nodiscard]] bool IsCancelled() const noexcept;

    /// Throw OperationCancelledException if cancelled
    void ThrowIfCancelled() const;

    /// Throw with context message if cancelled
    void ThrowIfCancelled(const std::string& context) const;

    /// Get the cancellation token (may be null)
    [[nodiscard]] CancellationTokenPtr GetCancellationToken() const;

    // ===================
    // Lifecycle events
    // ===================

    /// Emit operation started
    void EmitStarted(
        const std::string& operation_type,
        const std::string& operation_name);

    /// Emit operation completed (automatically calculates duration if StartTiming was called)
    void EmitCompleted(
        const std::string& operation_type,
        const std::string& operation_name,
        std::chrono::milliseconds duration);

    /// Emit operation completed (use internal timing)
    void EmitCompleted(
        const std::string& operation_type,
        const std::string& operation_name);

    /// Emit operation failed
    void EmitFailed(
        const std::string& operation_type,
        const std::string& operation_name,
        const std::string& error_message);

    /// Emit operation cancelled
    void EmitCancelled(
        const std::string& operation_type,
        const std::string& operation_name);

    /// Emit operation skipped
    void EmitSkipped(
        const std::string& operation_type,
        const std::string& operation_name,
        const std::string& reason = "");

    // ===================
    // Progress events
    // ===================

    /// Emit progress with current/total
    void EmitProgress(
        size_t current,
        size_t total,
        const std::string& message = "",
        const std::string& unit = "");

    /// Emit progress with percentage only
    void EmitProgressPercent(
        double percent,
        const std::string& message = "");

    /// Emit epoch progress (for ML training)
    void EmitEpoch(
        size_t epoch,
        size_t total_epochs,
        const std::string& message = "");

    /// Emit epoch with ML metrics
    void EmitEpochWithMetrics(
        size_t epoch,
        size_t total_epochs,
        std::optional<double> loss = std::nullopt,
        std::optional<double> accuracy = std::nullopt,
        std::optional<double> learning_rate = std::nullopt,
        std::optional<double> validation_loss = std::nullopt,
        std::optional<double> validation_accuracy = std::nullopt);

    // ===================
    // Metric events
    // ===================

    /// Emit single metric
    void EmitMetric(
        const std::string& name,
        double value,
        const std::string& unit = "");

    // ===================
    // Log events
    // ===================

    /// Emit debug log
    void EmitDebug(const std::string& message);

    /// Emit info log
    void EmitInfo(const std::string& message);

    /// Emit warning log
    void EmitWarning(const std::string& message);

    /// Emit error log
    void EmitError(const std::string& message);

    /// Emit log with level
    void EmitLog(LogEvent::Level level, const std::string& message);

    // ===================
    // Summary events
    // ===================

    /// Emit summary of operations
    void EmitSummary(
        double overall_percent,
        size_t completed,
        size_t total,
        size_t failed = 0,
        const std::vector<std::string>& currently_running = {});

    // ===================
    // Raw emission
    // ===================

    /// Emit a raw event (path will be set to base path)
    void Emit(GenericEvent event);

    /// Emit a raw event with custom path extension
    void Emit(GenericEvent event, ScopeType scope, const std::string& id);

    // ===================
    // Timing utilities
    // ===================

    /// Start timing for duration calculation
    void StartTiming();

    /// Get elapsed time since StartTiming was called
    [[nodiscard]] std::chrono::milliseconds GetElapsed() const;

    /// Get the dispatcher (for advanced use)
    [[nodiscard]] IGenericEventDispatcherPtr GetDispatcher() const;

private:
    IGenericEventDispatcherPtr m_dispatcher;
    CancellationTokenPtr m_cancellationToken;
    EventPath m_basePath;
    std::shared_ptr<JsonMetadata> m_context;
    std::optional<std::chrono::steady_clock::time_point> m_startTime;

    /// Apply context metadata to any event type (for concrete types)
    template<typename T>
    void ApplyContext(T& event) const {
        for (const auto& [key, value] : *m_context) {
            event.context[key] = value;
        }
    }

    /// Apply context metadata to a variant (uses std::visit)
    void ApplyContextToVariant(GenericEvent& event) const {
        std::visit([this](auto& e) {
            for (const auto& [key, value] : *m_context) {
                e.context[key] = value;
            }
        }, event);
    }
};

using ScopedProgressEmitterPtr = std::shared_ptr<ScopedProgressEmitter>;

// Factory functions

inline ScopedProgressEmitterPtr MakeScopedProgressEmitter(
    IGenericEventDispatcherPtr dispatcher,
    CancellationTokenPtr cancellationToken,
    EventPath basePath) {
    return std::make_shared<ScopedProgressEmitter>(
        std::move(dispatcher),
        std::move(cancellationToken),
        std::move(basePath));
}

inline ScopedProgressEmitterPtr MakeNullScopedProgressEmitter() {
    return std::make_shared<ScopedProgressEmitter>();
}

// ===================
// Implementation
// ===================

inline ScopedProgressEmitter::ScopedProgressEmitter(
    IGenericEventDispatcherPtr dispatcher,
    CancellationTokenPtr cancellationToken,
    EventPath basePath)
    : m_dispatcher(std::move(dispatcher))
    , m_cancellationToken(std::move(cancellationToken))
    , m_basePath(std::move(basePath))
    , m_context(std::make_shared<JsonMetadata>()) {}

inline ScopedProgressEmitter::ScopedProgressEmitter()
    : m_dispatcher(MakeNullGenericEventDispatcher())
    , m_cancellationToken(nullptr)
    , m_basePath()
    , m_context(std::make_shared<JsonMetadata>()) {}

inline ScopedProgressEmitter ScopedProgressEmitter::ChildScope(
    ScopeType scope,
    const std::string& id) const {
    ScopedProgressEmitter child(
        m_dispatcher,
        m_cancellationToken,
        m_basePath.Child(scope, id));
    // Copy context to child
    child.m_context = std::make_shared<JsonMetadata>(*m_context);
    return child;
}

inline const EventPath& ScopedProgressEmitter::GetPath() const {
    return m_basePath;
}

inline bool ScopedProgressEmitter::IsActive() const {
    return m_dispatcher != nullptr;
}

inline void ScopedProgressEmitter::SetContext(const std::string& key, const glz::generic& value) {
    (*m_context)[key] = value;
}

inline void ScopedProgressEmitter::SetContext(const std::string& key, int64_t value) {
    (*m_context)[key] = glz::generic(value);
}

inline void ScopedProgressEmitter::SetContext(const std::string& key, double value) {
    (*m_context)[key] = glz::generic(value);
}

inline void ScopedProgressEmitter::SetContext(const std::string& key, const std::string& value) {
    (*m_context)[key] = glz::generic(value);
}

inline void ScopedProgressEmitter::SetContext(const std::string& key, bool value) {
    (*m_context)[key] = glz::generic(value);
}

inline const JsonMetadata& ScopedProgressEmitter::GetContext() const {
    return *m_context;
}

inline void ScopedProgressEmitter::ClearContext() {
    m_context->clear();
}

inline bool ScopedProgressEmitter::IsCancelled() const noexcept {
    return m_cancellationToken && m_cancellationToken->IsCancelled();
}

inline void ScopedProgressEmitter::ThrowIfCancelled() const {
    if (m_cancellationToken) {
        m_cancellationToken->ThrowIfCancelled();
    }
}

inline void ScopedProgressEmitter::ThrowIfCancelled(const std::string& context) const {
    if (m_cancellationToken) {
        m_cancellationToken->ThrowIfCancelled(context);
    }
}

inline CancellationTokenPtr ScopedProgressEmitter::GetCancellationToken() const {
    return m_cancellationToken;
}

inline void ScopedProgressEmitter::EmitStarted(
    const std::string& operation_type,
    const std::string& operation_name) {

    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.status = OperationStatus::Started;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    ApplyContext(event);
    m_dispatcher->Emit(event);
    StartTiming();
}

inline void ScopedProgressEmitter::EmitCompleted(
    const std::string& operation_type,
    const std::string& operation_name,
    std::chrono::milliseconds duration) {

    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.status = OperationStatus::Completed;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    event.duration = duration;
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitCompleted(
    const std::string& operation_type,
    const std::string& operation_name) {
    EmitCompleted(operation_type, operation_name, GetElapsed());
}

inline void ScopedProgressEmitter::EmitFailed(
    const std::string& operation_type,
    const std::string& operation_name,
    const std::string& error_message) {

    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.status = OperationStatus::Failed;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    event.error_message = error_message;
    event.duration = GetElapsed();
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitCancelled(
    const std::string& operation_type,
    const std::string& operation_name) {

    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.status = OperationStatus::Cancelled;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    event.duration = GetElapsed();
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitSkipped(
    const std::string& operation_type,
    const std::string& operation_name,
    const std::string& reason) {

    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.status = OperationStatus::Skipped;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    if (!reason.empty()) {
        event.error_message = reason;  // Reuse error_message for skip reason
    }
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitProgress(
    size_t current,
    size_t total,
    const std::string& message,
    const std::string& unit) {

    ProgressEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.current = current;
    event.total = total;
    event.message = message;
    if (!unit.empty()) {
        event.unit = unit;
    }
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitProgressPercent(
    double percent,
    const std::string& message) {

    ProgressEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.progress_percent = percent;
    event.message = message;
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitEpoch(
    size_t epoch,
    size_t total_epochs,
    const std::string& message) {

    ProgressEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.current = epoch;
    event.total = total_epochs;
    event.unit = "epochs";
    event.message = message.empty()
        ? "Epoch " + std::to_string(epoch) + "/" + std::to_string(total_epochs)
        : message;
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitEpochWithMetrics(
    size_t epoch,
    size_t total_epochs,
    std::optional<double> loss,
    std::optional<double> accuracy,
    std::optional<double> learning_rate,
    std::optional<double> validation_loss,
    std::optional<double> validation_accuracy) {

    ProgressEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.current = epoch;
    event.total = total_epochs;
    event.unit = "epochs";
    event.message = "Epoch " + std::to_string(epoch) + "/" + std::to_string(total_epochs);

    // Add ML metrics to context
    if (loss.has_value()) {
        event.context["loss"] = glz::generic(*loss);
    }
    if (accuracy.has_value()) {
        event.context["accuracy"] = glz::generic(*accuracy);
    }
    if (learning_rate.has_value()) {
        event.context["learning_rate"] = glz::generic(*learning_rate);
    }
    if (validation_loss.has_value()) {
        event.context["validation_loss"] = glz::generic(*validation_loss);
    }
    if (validation_accuracy.has_value()) {
        event.context["validation_accuracy"] = glz::generic(*validation_accuracy);
    }

    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitMetric(
    const std::string& name,
    double value,
    const std::string& unit) {

    MetricEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.metric_name = name;
    event.value = value;
    if (!unit.empty()) {
        event.unit = unit;
    }
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitDebug(const std::string& message) {
    EmitLog(LogEvent::Level::Debug, message);
}

inline void ScopedProgressEmitter::EmitInfo(const std::string& message) {
    EmitLog(LogEvent::Level::Info, message);
}

inline void ScopedProgressEmitter::EmitWarning(const std::string& message) {
    EmitLog(LogEvent::Level::Warning, message);
}

inline void ScopedProgressEmitter::EmitError(const std::string& message) {
    EmitLog(LogEvent::Level::Error, message);
}

inline void ScopedProgressEmitter::EmitLog(LogEvent::Level level, const std::string& message) {
    LogEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.level = level;
    event.message = message;
    if (auto lastScope = m_basePath.GetLastScope()) {
        event.source = std::format("{}:{}", ScopeTypeWrapper::ToString(*lastScope), m_basePath.GetLastId().value_or(""));
    }
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::EmitSummary(
    double overall_percent,
    size_t completed,
    size_t total,
    size_t failed,
    const std::vector<std::string>& currently_running) {

    SummaryEvent event;
    event.timestamp = Now();
    event.path = m_basePath;
    event.overall_progress_percent = overall_percent;
    event.operations_completed = completed;
    event.operations_total = total;
    event.operations_failed = failed;
    event.operations_running = currently_running.size();
    event.currently_running = currently_running;
    ApplyContext(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::Emit(GenericEvent event) {
    SetEventPath(event, m_basePath);
    ApplyContextToVariant(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::Emit(
    GenericEvent event,
    ScopeType scope,
    const std::string& id) {
    SetEventPath(event, m_basePath.Child(scope, id));
    ApplyContextToVariant(event);
    m_dispatcher->Emit(event);
}

inline void ScopedProgressEmitter::StartTiming() {
    m_startTime = std::chrono::steady_clock::now();
}

inline std::chrono::milliseconds ScopedProgressEmitter::GetElapsed() const {
    if (!m_startTime.has_value()) {
        return std::chrono::milliseconds{0};
    }
    return ToMillis(std::chrono::steady_clock::now() - *m_startTime);
}

inline IGenericEventDispatcherPtr ScopedProgressEmitter::GetDispatcher() const {
    return m_dispatcher;
}

/**
 * ScopedOperation - RAII guard for operation lifecycle
 *
 * Automatically emits Started on construction and Completed/Failed on destruction.
 */
class ScopedOperation {
public:
    ScopedOperation(ScopedProgressEmitter& emitter,
                    std::string operation_type,
                    std::string operation_name)
        : m_emitter(emitter)
        , m_operationType(std::move(operation_type))
        , m_operationName(std::move(operation_name))
        , m_failed(false) {
        m_emitter.EmitStarted(m_operationType, m_operationName);
    }

    ~ScopedOperation() {
        if (m_failed) {
            m_emitter.EmitFailed(m_operationType, m_operationName, m_errorMessage);
        } else {
            m_emitter.EmitCompleted(m_operationType, m_operationName);
        }
    }

    void SetFailed(const std::string& error) {
        m_failed = true;
        m_errorMessage = error;
    }

    ScopedOperation(const ScopedOperation&) = delete;
    ScopedOperation& operator=(const ScopedOperation&) = delete;

private:
    ScopedProgressEmitter& m_emitter;
    std::string m_operationType;
    std::string m_operationName;
    bool m_failed;
    std::string m_errorMessage;
};

/**
 * MLProgressThrottler - Adaptive throttling for ML training progress
 *
 * Determines emission interval based on total epochs to avoid flooding SSE.
 */
class MLProgressThrottler {
public:
    /// Get recommended emit interval based on total epochs
    static size_t GetEmitInterval(size_t totalEpochs) {
        if (totalEpochs <= 20)   return 1;     // Every epoch for very short runs
        if (totalEpochs <= 100)  return 10;    // Every 10 for short
        if (totalEpochs <= 500)  return 50;    // Every 50 for medium
        return 100;                             // Every 100 for long
    }

    explicit MLProgressThrottler(size_t totalEpochs = 100)
        : m_totalEpochs(totalEpochs)
        , m_interval(GetEmitInterval(totalEpochs)) {}

    void SetTotalEpochs(size_t total) {
        m_totalEpochs = total;
        m_interval = GetEmitInterval(total);
    }

    [[nodiscard]] bool ShouldEmit(size_t currentEpoch) const {
        return currentEpoch == 1 ||                // Always first
               currentEpoch == m_totalEpochs ||    // Always last
               (currentEpoch % m_interval == 0);   // Every N
    }

    [[nodiscard]] size_t GetInterval() const { return m_interval; }

private:
    size_t m_totalEpochs;
    size_t m_interval;
};

} // namespace data_sdk::events
