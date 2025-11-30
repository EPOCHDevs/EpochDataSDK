#pragma once
#include "cancellation.h"
#include "dispatcher.h"
#include <chrono>
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
 *   auto emitter = MakeScopedProgressEmitter(dispatcher, token, MakeJobPath("job123"));
 *   emitter->EmitStarted("stage", "DataLoading");
 *
 *   auto nodeEmitter = emitter->ChildScope(ScopeType::Node, "SMA_20");
 *   nodeEmitter.EmitProgress(50, 100, "Processing rows");
 *
 * Thread safety: Safe to use from multiple threads if dispatcher is thread-safe.
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

    [[nodiscard]] ScopedProgressEmitter ChildScope(ScopeType scope, const std::string& id) const;
    [[nodiscard]] const EventPath& GetPath() const;
    [[nodiscard]] bool IsActive() const;

    // ===================
    // Context management
    // ===================

    void SetContext(const std::string& key, const glz::generic& value);
    void SetContext(const std::string& key, int64_t value);
    void SetContext(const std::string& key, double value);
    void SetContext(const std::string& key, const std::string& value);
    void SetContext(const std::string& key, bool value);
    [[nodiscard]] const JsonMetadata& GetContext() const;
    void ClearContext();

    // ===================
    // Cancellation
    // ===================

    [[nodiscard]] bool IsCancelled() const noexcept;
    void ThrowIfCancelled() const;
    void ThrowIfCancelled(const std::string& context) const;
    [[nodiscard]] CancellationTokenPtr GetCancellationToken() const;

    // ===================
    // Lifecycle events
    // ===================

    void EmitStarted(const std::string& operation_type, const std::string& operation_name);
    void EmitCompleted(const std::string& operation_type, const std::string& operation_name, std::chrono::milliseconds duration);
    void EmitCompleted(const std::string& operation_type, const std::string& operation_name);
    void EmitFailed(const std::string& operation_type, const std::string& operation_name, const std::string& error_message);
    void EmitCancelled(const std::string& operation_type, const std::string& operation_name);
    void EmitSkipped(const std::string& operation_type, const std::string& operation_name, const std::string& reason = "");

    // ===================
    // Progress events
    // ===================

    void EmitProgress(size_t current, size_t total, const std::string& message = "", const std::string& unit = "");
    void EmitProgressPercent(double percent, const std::string& message = "");
    void EmitEpoch(size_t epoch, size_t total_epochs, const std::string& message = "");
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

    void EmitMetric(const std::string& name, double value, const std::string& unit = "");

    // ===================
    // Log events
    // ===================

    void EmitDebug(const std::string& message);
    void EmitInfo(const std::string& message);
    void EmitWarning(const std::string& message);
    void EmitError(const std::string& message);
    void EmitLog(LogEvent::Level level, const std::string& message);

    // ===================
    // Summary events
    // ===================

    void EmitSummary(
        double overall_percent,
        size_t completed,
        size_t total,
        size_t failed = 0,
        const std::vector<std::string>& currently_running = {});

    // ===================
    // Raw emission
    // ===================

    void Emit(GenericEvent event);
    void Emit(GenericEvent event, ScopeType scope, const std::string& id);

    // ===================
    // Timing utilities
    // ===================

    void StartTiming();
    [[nodiscard]] std::chrono::milliseconds GetElapsed() const;
    [[nodiscard]] IGenericEventDispatcherPtr GetDispatcher() const;

private:
    struct Impl;
    std::shared_ptr<Impl> m_impl;
};

using ScopedProgressEmitterPtr = std::shared_ptr<ScopedProgressEmitter>;

// Factory functions (implementations in .cpp)
ScopedProgressEmitterPtr MakeScopedProgressEmitter(
    IGenericEventDispatcherPtr dispatcher,
    CancellationTokenPtr cancellationToken,
    EventPath basePath);

ScopedProgressEmitterPtr MakeNullScopedProgressEmitter();

/**
 * ScopedOperation - RAII guard for operation lifecycle
 */
class ScopedOperation {
public:
    ScopedOperation(ScopedProgressEmitter& emitter, std::string operation_type, std::string operation_name);
    ~ScopedOperation();

    void SetFailed(const std::string& error);

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
 */
class MLProgressThrottler {
public:
    static size_t GetEmitInterval(size_t totalEpochs);

    explicit MLProgressThrottler(size_t totalEpochs = 100);

    void SetTotalEpochs(size_t total);
    [[nodiscard]] bool ShouldEmit(size_t currentEpoch) const;
    [[nodiscard]] size_t GetInterval() const;

private:
    size_t m_totalEpochs;
    size_t m_interval;
};

} // namespace data_sdk::events
