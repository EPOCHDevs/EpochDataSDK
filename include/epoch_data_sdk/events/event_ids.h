#pragma once
#include <string_view>

namespace data_sdk::events {

/**
 * @file event_ids.h
 * @brief Constexpr string IDs for the event system
 *
 * This file centralizes all operation_type and operation_name identifiers
 * used throughout the event system for consistency and type safety.
 */

// =============================================================================
// Operation Types (operation_type parameter in EmitStarted/Completed/Failed)
// =============================================================================

namespace OperationType {

/// Pipeline-level operations (top-level orchestration)
/// Used in: src/transforms/runtime/orchestrator.cpp:186,246,262
constexpr std::string_view Pipeline = "pipeline";

/// Stage-level operations (major phases of pipeline)
/// Used in: src/data/database/database_impl.cpp:180,182,247,249,253,255
constexpr std::string_view Stage = "stage";

/// Node-level operations (individual transform nodes)
/// Used in: src/transforms/runtime/execution/execution_context.h:49,58,67
constexpr std::string_view Node = "node";

/// Transform-level operations (within a transform)
/// Used in: src/EVENT_SYSTEM.md:469,482 (example usage)
constexpr std::string_view Transform = "transform";

/// Asset-level operations (per-asset processing)
constexpr std::string_view Asset = "asset";

} // namespace OperationType

// =============================================================================
// Stage Names (operation_name for Stage operations)
// =============================================================================

namespace StageName {

/// Data loading stage - fetches data from APIs/cache
/// Used in: src/data/database/database_impl.cpp:180,182
constexpr std::string_view DataLoading = "DataLoading";

/// Resampling stage - converts timeframes (1min -> 5min, etc.)
/// Used in: src/data/database/database_impl.cpp:247,249
constexpr std::string_view Resampling = "Resampling";

/// Transformation stage - executes transform DAG
/// Used in: src/data/database/database_impl.cpp:253,255
constexpr std::string_view Transformation = "Transformation";

} // namespace StageName

// =============================================================================
// Pipeline Names (operation_name for Pipeline operations)
// =============================================================================

namespace PipelineName {

/// Transform pipeline execution
/// Used in: src/transforms/runtime/orchestrator.cpp:186,246,262
constexpr std::string_view Transform = "Transform";

/// Full database pipeline (load + resample + transform)
constexpr std::string_view Database = "Database";

} // namespace PipelineName

// =============================================================================
// Context Keys (keys used in SetContext() calls)
// =============================================================================

namespace ContextKey {

/// Total number of nodes in pipeline
/// Used in: src/transforms/runtime/orchestrator.cpp:187
constexpr std::string_view TotalNodes = "total_nodes";

/// Total number of assets being processed
/// Used in: src/transforms/runtime/orchestrator.cpp:188
constexpr std::string_view TotalAssets = "total_assets";

/// Duration in milliseconds
/// Used in: src/transforms/runtime/orchestrator.cpp:263
constexpr std::string_view DurationMs = "duration_ms";

/// Number of nodes that succeeded
/// Used in: src/transforms/runtime/orchestrator.cpp:264
constexpr std::string_view NodesSucceeded = "nodes_succeeded";

/// Number of nodes that failed
/// Used in: src/transforms/runtime/orchestrator.cpp:265
constexpr std::string_view NodesFailed = "nodes_failed";

/// Number of nodes that were skipped
/// Used in: src/transforms/runtime/orchestrator.cpp:266
constexpr std::string_view NodesSkipped = "nodes_skipped";

/// Whether transform is cross-sectional
/// Used in: src/transforms/runtime/execution/execution_context.h:50
constexpr std::string_view IsCrossSectional = "is_cross_sectional";

/// Number of assets in operation
/// Used in: src/transforms/runtime/execution/execution_context.h:51
constexpr std::string_view AssetCount = "asset_count";

/// Number of assets processed
/// Used in: src/transforms/runtime/execution/execution_context.h:59
constexpr std::string_view AssetsProcessed = "assets_processed";

/// Number of assets that failed
/// Used in: src/transforms/runtime/execution/execution_context.h:60
constexpr std::string_view AssetsFailed = "assets_failed";

// ML Training context keys
constexpr std::string_view Loss = "loss";
constexpr std::string_view Accuracy = "accuracy";
constexpr std::string_view Epoch = "epoch";
constexpr std::string_view LearningRate = "lr";
constexpr std::string_view ValidationLoss = "val_loss";
constexpr std::string_view ValidationAccuracy = "val_accuracy";

} // namespace ContextKey

} // namespace data_sdk::events
