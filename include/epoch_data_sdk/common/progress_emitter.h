#pragma once
#include <epoch_data_sdk/events/cancellation.h>
#include "event_dispatcher.h"

namespace data_sdk::events {

class TransformProgressEmitter {
public:
    TransformProgressEmitter(IEventDispatcherPtr dispatcher,
                             CancellationTokenPtr cancellationToken,
                             std::string node_id,
                             std::string operation_name);

    TransformProgressEmitter(const TransformProgressEmitter&) = delete;
    TransformProgressEmitter& operator=(const TransformProgressEmitter&) = delete;
    TransformProgressEmitter(TransformProgressEmitter&&) = default;
    TransformProgressEmitter& operator=(TransformProgressEmitter&&) = default;

    void SetAssetId(const std::string& asset_id);
    void ClearAssetId();
    [[nodiscard]] std::optional<std::string> GetAssetId() const;

    [[nodiscard]] bool IsCancelled() const noexcept;
    void ThrowIfCancelled() const;
    void ThrowIfCancelled(const std::string& context) const;

    void EmitProgress(std::size_t current, std::size_t total,
                      const std::string& message = "");
    void EmitEpoch(std::size_t epoch, std::size_t total_epochs,
                   std::optional<double> loss = std::nullopt,
                   std::optional<double> accuracy = std::nullopt,
                   std::optional<double> learning_rate = std::nullopt);
    void EmitIteration(std::size_t iteration,
                       std::optional<double> metric = std::nullopt,
                       const std::string& message = "");
    void EmitCustomProgress(const JsonMetadata& metadata,
                            const std::string& message = "");
    void Emit(TransformProgressEvent event);

    void EmitEpochOrCancel(std::size_t epoch, std::size_t total_epochs,
                           std::optional<double> loss = std::nullopt,
                           std::optional<double> accuracy = std::nullopt);
    void EmitIterationOrCancel(std::size_t iteration,
                               std::optional<double> metric = std::nullopt,
                               const std::string& message = "");

    [[nodiscard]] const std::string& GetNodeId() const { return m_nodeId; }
    [[nodiscard]] const std::string& GetOperationName() const { return m_operationName; }

private:
    IEventDispatcherPtr m_dispatcher;
    CancellationTokenPtr m_cancellationToken;
    std::string m_nodeId;
    std::string m_operationName;
    std::optional<std::string> m_assetId;

    TransformProgressEvent MakeBaseEvent() const;
};

using TransformProgressEmitterPtr = std::shared_ptr<TransformProgressEmitter>;

inline TransformProgressEmitterPtr MakeProgressEmitter(
    IEventDispatcherPtr dispatcher,
    CancellationTokenPtr cancellationToken,
    const std::string& node_id,
    const std::string& operation_name) {

    return std::make_shared<TransformProgressEmitter>(
        std::move(dispatcher),
        std::move(cancellationToken),
        node_id,
        operation_name
    );
}

class AssetContextGuard {
public:
    AssetContextGuard(TransformProgressEmitter& emitter, const std::string& asset_id)
        : m_emitter(emitter) {
        m_emitter.SetAssetId(asset_id);
    }

    ~AssetContextGuard() {
        m_emitter.ClearAssetId();
    }

    AssetContextGuard(const AssetContextGuard&) = delete;
    AssetContextGuard& operator=(const AssetContextGuard&) = delete;

private:
    TransformProgressEmitter& m_emitter;
};

} // namespace data_sdk::events
