#include <epoch_data_sdk/events/emitter.h>
#include <format>

namespace data_sdk::events {

// ===================
// Impl structure (PIMPL)
// ===================

struct ScopedProgressEmitter::Impl {
    IGenericEventDispatcherPtr dispatcher;
    CancellationTokenPtr cancellationToken;
    EventPath basePath;
    std::shared_ptr<JsonMetadata> context;
    std::optional<std::chrono::steady_clock::time_point> startTime;

    Impl(IGenericEventDispatcherPtr d, CancellationTokenPtr t, EventPath p)
        : dispatcher(std::move(d))
        , cancellationToken(std::move(t))
        , basePath(std::move(p))
        , context(std::make_shared<JsonMetadata>()) {}

    Impl()
        : dispatcher(MakeNullGenericEventDispatcher())
        , cancellationToken(nullptr)
        , basePath()
        , context(std::make_shared<JsonMetadata>()) {}

    template<typename T>
    void ApplyContext(T& event) const {
        for (const auto& [key, value] : *context) {
            event.context[key] = value;
        }
    }

    void ApplyContextToVariant(GenericEvent& event) const {
        std::visit([this](auto& e) {
            for (const auto& [key, value] : *context) {
                e.context[key] = value;
            }
        }, event);
    }
};

// ===================
// Constructors
// ===================

ScopedProgressEmitter::ScopedProgressEmitter(
    IGenericEventDispatcherPtr dispatcher,
    CancellationTokenPtr cancellationToken,
    EventPath basePath)
    : m_impl(std::make_shared<Impl>(std::move(dispatcher), std::move(cancellationToken), std::move(basePath))) {}

ScopedProgressEmitter::ScopedProgressEmitter()
    : m_impl(std::make_shared<Impl>()) {}

// ===================
// Scope management
// ===================

ScopedProgressEmitter ScopedProgressEmitter::ChildScope(ScopeType scope, const std::string& id) const {
    ScopedProgressEmitter child(m_impl->dispatcher, m_impl->cancellationToken, m_impl->basePath.Child(scope, id));
    child.m_impl->context = std::make_shared<JsonMetadata>(*m_impl->context);
    return child;
}

const EventPath& ScopedProgressEmitter::GetPath() const {
    return m_impl->basePath;
}

bool ScopedProgressEmitter::IsActive() const {
    return m_impl->dispatcher != nullptr;
}

// ===================
// Context management
// ===================

void ScopedProgressEmitter::SetContext(const std::string& key, const glz::generic& value) {
    (*m_impl->context)[key] = value;
}

void ScopedProgressEmitter::SetContext(const std::string& key, int64_t value) {
    (*m_impl->context)[key] = glz::generic(value);
}

void ScopedProgressEmitter::SetContext(const std::string& key, double value) {
    (*m_impl->context)[key] = glz::generic(value);
}

void ScopedProgressEmitter::SetContext(const std::string& key, const std::string& value) {
    (*m_impl->context)[key] = glz::generic(value);
}

void ScopedProgressEmitter::SetContext(const std::string& key, bool value) {
    (*m_impl->context)[key] = glz::generic(value);
}

const JsonMetadata& ScopedProgressEmitter::GetContext() const {
    return *m_impl->context;
}

void ScopedProgressEmitter::ClearContext() {
    m_impl->context->clear();
}

// ===================
// Cancellation
// ===================

bool ScopedProgressEmitter::IsCancelled() const noexcept {
    return m_impl->cancellationToken && m_impl->cancellationToken->IsCancelled();
}

void ScopedProgressEmitter::ThrowIfCancelled() const {
    if (m_impl->cancellationToken) {
        m_impl->cancellationToken->ThrowIfCancelled();
    }
}

void ScopedProgressEmitter::ThrowIfCancelled(const std::string& context) const {
    if (m_impl->cancellationToken) {
        m_impl->cancellationToken->ThrowIfCancelled(context);
    }
}

CancellationTokenPtr ScopedProgressEmitter::GetCancellationToken() const {
    return m_impl->cancellationToken;
}

// ===================
// Lifecycle events
// ===================

void ScopedProgressEmitter::EmitStarted(const std::string& operation_type, const std::string& operation_name) {
    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.status = OperationStatus::Started;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
    StartTiming();
}

void ScopedProgressEmitter::EmitCompleted(const std::string& operation_type, const std::string& operation_name, std::chrono::milliseconds duration) {
    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.status = OperationStatus::Completed;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    event.duration = duration;
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

void ScopedProgressEmitter::EmitCompleted(const std::string& operation_type, const std::string& operation_name) {
    EmitCompleted(operation_type, operation_name, GetElapsed());
}

void ScopedProgressEmitter::EmitFailed(const std::string& operation_type, const std::string& operation_name, const std::string& error_message) {
    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.status = OperationStatus::Failed;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    event.error_message = error_message;
    event.duration = GetElapsed();
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

void ScopedProgressEmitter::EmitCancelled(const std::string& operation_type, const std::string& operation_name) {
    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.status = OperationStatus::Cancelled;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    event.duration = GetElapsed();
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

void ScopedProgressEmitter::EmitSkipped(const std::string& operation_type, const std::string& operation_name, const std::string& reason) {
    LifecycleEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.status = OperationStatus::Skipped;
    event.operation_type = operation_type;
    event.operation_name = operation_name;
    if (!reason.empty()) {
        event.error_message = reason;
    }
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

// ===================
// Progress events
// ===================

void ScopedProgressEmitter::EmitProgress(size_t current, size_t total, const std::string& message, const std::string& unit) {
    ProgressEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.current = current;
    event.total = total;
    event.message = message;
    if (!unit.empty()) {
        event.unit = unit;
    }
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

void ScopedProgressEmitter::EmitProgressPercent(double percent, const std::string& message) {
    ProgressEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.progress_percent = percent;
    event.message = message;
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

void ScopedProgressEmitter::EmitEpoch(size_t epoch, size_t total_epochs, const std::string& message) {
    ProgressEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.current = epoch;
    event.total = total_epochs;
    event.unit = "epochs";
    event.message = message.empty()
        ? "Epoch " + std::to_string(epoch) + "/" + std::to_string(total_epochs)
        : message;
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

void ScopedProgressEmitter::EmitEpochWithMetrics(
    size_t epoch,
    size_t total_epochs,
    std::optional<double> loss,
    std::optional<double> accuracy,
    std::optional<double> learning_rate,
    std::optional<double> validation_loss,
    std::optional<double> validation_accuracy) {

    ProgressEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.current = epoch;
    event.total = total_epochs;
    event.unit = "epochs";
    event.message = "Epoch " + std::to_string(epoch) + "/" + std::to_string(total_epochs);

    if (loss.has_value()) event.context["loss"] = glz::generic(*loss);
    if (accuracy.has_value()) event.context["accuracy"] = glz::generic(*accuracy);
    if (learning_rate.has_value()) event.context["learning_rate"] = glz::generic(*learning_rate);
    if (validation_loss.has_value()) event.context["validation_loss"] = glz::generic(*validation_loss);
    if (validation_accuracy.has_value()) event.context["validation_accuracy"] = glz::generic(*validation_accuracy);

    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

// ===================
// Metric events
// ===================

void ScopedProgressEmitter::EmitMetric(const std::string& name, double value, const std::string& unit) {
    MetricEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.metric_name = name;
    event.value = value;
    if (!unit.empty()) {
        event.unit = unit;
    }
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

// ===================
// Log events
// ===================

void ScopedProgressEmitter::EmitDebug(const std::string& message) {
    EmitLog(LogEvent::Level::Debug, message);
}

void ScopedProgressEmitter::EmitInfo(const std::string& message) {
    EmitLog(LogEvent::Level::Info, message);
}

void ScopedProgressEmitter::EmitWarning(const std::string& message) {
    EmitLog(LogEvent::Level::Warning, message);
}

void ScopedProgressEmitter::EmitError(const std::string& message) {
    EmitLog(LogEvent::Level::Error, message);
}

void ScopedProgressEmitter::EmitLog(LogEvent::Level level, const std::string& message) {
    LogEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.level = level;
    event.message = message;
    if (auto lastScope = m_impl->basePath.GetLastScope()) {
        event.source = std::format("{}:{}", ScopeTypeWrapper::ToString(*lastScope), m_impl->basePath.GetLastId().value_or(""));
    }
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

// ===================
// Summary events
// ===================

void ScopedProgressEmitter::EmitSummary(
    double overall_percent,
    size_t completed,
    size_t total,
    size_t failed,
    const std::vector<std::string>& currently_running) {

    SummaryEvent event;
    event.timestamp = Now();
    event.path = m_impl->basePath;
    event.overall_progress_percent = overall_percent;
    event.operations_completed = completed;
    event.operations_total = total;
    event.operations_failed = failed;
    event.operations_running = currently_running.size();
    event.currently_running = currently_running;
    m_impl->ApplyContext(event);
    m_impl->dispatcher->Emit(event);
}

// ===================
// Raw emission
// ===================

void ScopedProgressEmitter::Emit(GenericEvent event) {
    SetEventPath(event, m_impl->basePath);
    m_impl->ApplyContextToVariant(event);
    m_impl->dispatcher->Emit(event);
}

void ScopedProgressEmitter::Emit(GenericEvent event, ScopeType scope, const std::string& id) {
    SetEventPath(event, m_impl->basePath.Child(scope, id));
    m_impl->ApplyContextToVariant(event);
    m_impl->dispatcher->Emit(event);
}

// ===================
// Timing utilities
// ===================

void ScopedProgressEmitter::StartTiming() {
    m_impl->startTime = std::chrono::steady_clock::now();
}

std::chrono::milliseconds ScopedProgressEmitter::GetElapsed() const {
    if (!m_impl->startTime.has_value()) {
        return std::chrono::milliseconds{0};
    }
    return ToMillis(std::chrono::steady_clock::now() - *m_impl->startTime);
}

IGenericEventDispatcherPtr ScopedProgressEmitter::GetDispatcher() const {
    return m_impl->dispatcher;
}

// ===================
// Factory functions
// ===================

ScopedProgressEmitterPtr MakeScopedProgressEmitter(
    IGenericEventDispatcherPtr dispatcher,
    CancellationTokenPtr cancellationToken,
    EventPath basePath) {
    return std::make_shared<ScopedProgressEmitter>(
        std::move(dispatcher),
        std::move(cancellationToken),
        std::move(basePath));
}

ScopedProgressEmitterPtr MakeNullScopedProgressEmitter() {
    return std::make_shared<ScopedProgressEmitter>();
}

// ===================
// ScopedOperation
// ===================

ScopedOperation::ScopedOperation(ScopedProgressEmitter& emitter, std::string operation_type, std::string operation_name)
    : m_emitter(emitter)
    , m_operationType(std::move(operation_type))
    , m_operationName(std::move(operation_name))
    , m_failed(false) {
    m_emitter.EmitStarted(m_operationType, m_operationName);
}

ScopedOperation::~ScopedOperation() {
    if (m_failed) {
        m_emitter.EmitFailed(m_operationType, m_operationName, m_errorMessage);
    } else {
        m_emitter.EmitCompleted(m_operationType, m_operationName);
    }
}

void ScopedOperation::SetFailed(const std::string& error) {
    m_failed = true;
    m_errorMessage = error;
}

// ===================
// MLProgressThrottler
// ===================

size_t MLProgressThrottler::GetEmitInterval(size_t totalEpochs) {
    if (totalEpochs <= 20)   return 1;
    if (totalEpochs <= 100)  return 10;
    if (totalEpochs <= 500)  return 50;
    return 100;
}

MLProgressThrottler::MLProgressThrottler(size_t totalEpochs)
    : m_totalEpochs(totalEpochs)
    , m_interval(GetEmitInterval(totalEpochs)) {}

void MLProgressThrottler::SetTotalEpochs(size_t total) {
    m_totalEpochs = total;
    m_interval = GetEmitInterval(total);
}

bool MLProgressThrottler::ShouldEmit(size_t currentEpoch) const {
    return currentEpoch == 1 ||
           currentEpoch == m_totalEpochs ||
           (currentEpoch % m_interval == 0);
}

size_t MLProgressThrottler::GetInterval() const {
    return m_interval;
}

} // namespace data_sdk::events
