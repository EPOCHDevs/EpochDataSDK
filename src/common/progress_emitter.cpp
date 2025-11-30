#include <epoch_data_sdk/common/progress_emitter.h>

namespace data_sdk::events {

TransformProgressEmitter::TransformProgressEmitter(
    IEventDispatcherPtr dispatcher,
    CancellationTokenPtr cancellationToken,
    std::string node_id,
    std::string operation_name)
    : m_dispatcher(std::move(dispatcher))
    , m_cancellationToken(std::move(cancellationToken))
    , m_nodeId(std::move(node_id))
    , m_operationName(std::move(operation_name)) {}

void TransformProgressEmitter::SetAssetId(const std::string& asset_id) {
    m_assetId = asset_id;
}

void TransformProgressEmitter::ClearAssetId() {
    m_assetId.reset();
}

std::optional<std::string> TransformProgressEmitter::GetAssetId() const {
    return m_assetId;
}

bool TransformProgressEmitter::IsCancelled() const noexcept {
    return m_cancellationToken && m_cancellationToken->IsCancelled();
}

void TransformProgressEmitter::ThrowIfCancelled() const {
    if (m_cancellationToken) {
        m_cancellationToken->ThrowIfCancelled();
    }
}

void TransformProgressEmitter::ThrowIfCancelled(const std::string& context) const {
    if (m_cancellationToken) {
        m_cancellationToken->ThrowIfCancelled(context);
    }
}

TransformProgressEvent TransformProgressEmitter::MakeBaseEvent() const {
    TransformProgressEvent event;
    event.timestamp = Now();
    event.node_id = m_nodeId;
    event.operation_name = m_operationName;
    event.asset_id = m_assetId;
    return event;
}

void TransformProgressEmitter::EmitProgress(
    std::size_t current,
    std::size_t total,
    const std::string& message) {

    if (!m_dispatcher) return;

    auto event = MakeBaseEvent();
    event.current_step = current;
    event.total_steps = total;
    if (total > 0) {
        event.progress_percent = (static_cast<double>(current) / total) * 100.0;
    }
    event.message = message;

    m_dispatcher->Emit(std::move(event));
}

void TransformProgressEmitter::EmitEpoch(
    std::size_t epoch,
    std::size_t total_epochs,
    std::optional<double> loss,
    std::optional<double> accuracy,
    std::optional<double> learning_rate) {

    if (!m_dispatcher) return;

    auto event = MakeBaseEvent();
    event.current_step = epoch;
    event.total_steps = total_epochs;
    if (total_epochs > 0) {
        event.progress_percent = (static_cast<double>(epoch) / total_epochs) * 100.0;
    }
    event.loss = loss;
    event.accuracy = accuracy;
    event.learning_rate = learning_rate;

    std::string msg = "Epoch " + std::to_string(epoch) + "/" + std::to_string(total_epochs);
    if (loss) {
        msg += " loss=" + std::to_string(*loss);
    }
    if (accuracy) {
        msg += " acc=" + std::to_string(*accuracy);
    }
    event.message = msg;

    m_dispatcher->Emit(std::move(event));
}

void TransformProgressEmitter::EmitIteration(
    std::size_t iteration,
    std::optional<double> metric,
    const std::string& message) {

    if (!m_dispatcher) return;

    auto event = MakeBaseEvent();
    event.iteration = iteration;
    event.message = message.empty()
        ? "Iteration " + std::to_string(iteration)
        : message;

    if (metric) {
        event.metadata["metric"] = glz::generic(*metric);
    }

    m_dispatcher->Emit(std::move(event));
}

void TransformProgressEmitter::EmitCustomProgress(
    const JsonMetadata& metadata,
    const std::string& message) {

    if (!m_dispatcher) return;

    auto event = MakeBaseEvent();
    event.metadata = metadata;
    event.message = message;

    m_dispatcher->Emit(std::move(event));
}

void TransformProgressEmitter::Emit(TransformProgressEvent event) {
    if (!m_dispatcher) return;

    if (event.timestamp == Timestamp{}) {
        event.timestamp = Now();
    }
    if (event.node_id.empty()) {
        event.node_id = m_nodeId;
    }
    if (event.operation_name.empty()) {
        event.operation_name = m_operationName;
    }
    if (!event.asset_id && m_assetId) {
        event.asset_id = m_assetId;
    }

    m_dispatcher->Emit(std::move(event));
}

void TransformProgressEmitter::EmitEpochOrCancel(
    std::size_t epoch,
    std::size_t total_epochs,
    std::optional<double> loss,
    std::optional<double> accuracy) {

    ThrowIfCancelled("Training epoch " + std::to_string(epoch));
    EmitEpoch(epoch, total_epochs, loss, accuracy);
}

void TransformProgressEmitter::EmitIterationOrCancel(
    std::size_t iteration,
    std::optional<double> metric,
    const std::string& message) {

    ThrowIfCancelled("Iteration " + std::to_string(iteration));
    EmitIteration(iteration, metric, message);
}

} // namespace data_sdk::events
