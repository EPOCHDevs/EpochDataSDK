# Event System

A hierarchical, domain-agnostic event system for real-time progress tracking across
EpochDataSDK → EpochScript → StratifyX → Frontend.

## UI Visualization

The event system maps directly to what users see in the frontend:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Job: momentum_backtest_2024                                    [75% ████▓░]│
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─ Stage: DataLoading ─────────────────────────────── ✓ Completed (2.3s) ─┐│
│  │                                                                         ││
│  │  Asset: AAPL ──────────────────────────────────── ✓ Completed           ││
│  │  Asset: GOOGL ─────────────────────────────────── ✓ Completed           ││
│  │  Asset: MSFT ──────────────────────────────────── ✓ Completed           ││
│  │                                                                         ││
│  │  context: { total_assets: 50, categories: 3 }                           ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─ Stage: Transform ───────────────────────────────────── ◐ Running ──────┐│
│  │                                                                         ││
│  │  Node: SMA_20 ─────────────────────────────────── ✓ Completed           ││
│  │  Node: RSI_14 ─────────────────────────────────── ◐ Running [60%]       ││
│  │    └─ Asset: AAPL ─────────────────────────────── ✓ Completed           ││
│  │    └─ Asset: GOOGL ────────────────────────────── ◐ Running             ││
│  │    └─ Asset: MSFT ─────────────────────────────── ○ Pending             ││
│  │  Node: MACD ───────────────────────────────────── ○ Pending             ││
│  │                                                                         ││
│  │  context: { nodes_completed: 1, nodes_total: 3 }                        ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
│  ┌─ Stage: Backtest ────────────────────────────────────── ○ Pending ──────┐│
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## EventPath - The Hierarchy

Every event has a **path** that describes WHERE in the job hierarchy it occurred:

```
Job:momentum_2024/Stage:Transform/Node:RSI_14/Asset:AAPL
│                 │               │            │
└─ Job ID         └─ Stage name   └─ Node ID   └─ Asset being processed
```

**ScopeType enum** defines the hierarchy levels:
- `Job` - Top-level job/campaign
- `Stage` - Major phase (DataLoading, Transform, Backtest, etc.)
- `Node` - Transform node in the DAG
- `Asset` - Individual asset being processed
- `Category` - Data category (DailyBars, Dividends, etc.)
- `Batch` - Batch of items
- `Custom` - User-defined scope

## Context - Metadata That Travels With Events

**Context** is a key-value map attached to every event. It provides additional
information that helps the UI display meaningful data.

```cpp
// In DataLoader:
emitter.SetContext("total_assets", 50);        // How many assets total
emitter.SetContext("categories", 3);           // How many categories
emitter.EmitStarted("dataloader", "LoadData"); // Event carries context

// In ML Training:
emitter.SetContext("batch_size", 32);
emitter.SetContext("model", "LightGBM");
emitter.EmitProgress(5, 100, "Training");      // Event carries context
```

**Why context matters for UI:**

```
┌─ Node: RollingPCA ─────────────────────────────────── ◐ Training ──────────┐o
│                                                                            │
│  Epoch 45/100                                                              │
│  ████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 45%     │
│                                                                            │
│  context:                                                                  │
│    loss: 0.0234        ← ML metrics from context                           │
│    accuracy: 0.891                                                         │
│    learning_rate: 0.001                                                    │
│    batch_size: 32                                                          │
│    model: "LightGBM"                                                       │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

Context propagates from parent to child emitters, so child events inherit
parent context automatically.

## Event Types

### LifecycleEvent - Operation state changes
```
Status: Started → Running → Completed/Failed/Cancelled/Skipped
```

Used to show operation state in UI with icons: ○ ◐ ✓ ✗ ⊘

### ProgressEvent - Progress updates
```cpp
emitter.EmitProgress(45, 100, "Processing rows", "rows");
// UI shows: [45/100 rows] ████████░░░░░░░░░░░░ 45%
```

### MetricEvent - Single metric values
```cpp
emitter.EmitMetric("throughput", 1234.5, "rows/sec");
// UI shows: throughput: 1234.5 rows/sec
```

### LogEvent - Log messages
```cpp
emitter.EmitWarning("Asset AAPL has gaps in data");
// UI shows in log panel with warning icon
```

### SummaryEvent - Aggregate status
```cpp
emitter.EmitSummary(75.0, 15, 20, 2, {"RSI_14", "MACD"});
// UI shows: 75% complete, 15/20 done, 2 failed, 2 running
```

## ScopedProgressEmitter - The Main API

```cpp
// Create root emitter for a job
auto dispatcher = MakeGenericEventDispatcher();
auto token = MakeCancellationToken();
auto jobEmitter = ScopedProgressEmitter(dispatcher, token, MakeJobPath("job-123"));

// Add context that all child events will inherit
jobEmitter.SetContext("total_assets", 50);

// Emit job started
jobEmitter.EmitStarted("job", "MomentumBacktest");

// Create child for DataLoading stage
auto loadEmitter = jobEmitter.ChildScope(ScopeType::Stage, "DataLoading");
loadEmitter.EmitStarted("stage", "DataLoading");

// Create grandchild for each asset
auto assetEmitter = loadEmitter.ChildScope(ScopeType::Asset, "AAPL");
assetEmitter.EmitStarted("asset", "AAPL");
assetEmitter.EmitProgress(1, 5, "Loading categories");
assetEmitter.EmitCompleted("asset", "AAPL");

// Mark stage complete
loadEmitter.EmitCompleted("stage", "DataLoading");
```

Events emitted:
```
Path                                  | Type      | Status
--------------------------------------|-----------|----------
Job:job-123                           | Lifecycle | Started
Job:job-123/Stage:DataLoading         | Lifecycle | Started
Job:job-123/Stage:DataLoading/Asset:AAPL | Lifecycle | Started
Job:job-123/Stage:DataLoading/Asset:AAPL | Progress  | 1/5
Job:job-123/Stage:DataLoading/Asset:AAPL | Lifecycle | Completed
Job:job-123/Stage:DataLoading         | Lifecycle | Completed
```

## ScopedOperation - RAII Lifecycle

Automatically emits Started on construction, Completed/Failed on destruction:

```cpp
void ProcessAsset(ScopedProgressEmitter& emitter, const Asset& asset) {
    ScopedOperation op(emitter, "asset", asset.GetID());  // Emits Started

    try {
        // Do work...
    } catch (const std::exception& e) {
        op.SetFailed(e.what());  // Will emit Failed on destruction
        throw;
    }
    // Emits Completed on destruction if no failure
}
```

## Cancellation

The emitter respects cancellation tokens:

```cpp
for (size_t i = 0; i < total; ++i) {
    emitter.ThrowIfCancelled();  // Throws OperationCancelledException
    // or
    if (emitter.IsCancelled()) break;

    ProcessItem(i);
    emitter.EmitProgress(i + 1, total);
}
```

## Data Flow: Backend → Frontend

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  EpochDataSDK    │    │   EpochScript    │    │    StratifyX     │
│  (DataLoader)    │───▶│  (Orchestrator)  │───▶│  (SSE Endpoint)  │
└──────────────────┘    └──────────────────┘    └──────────────────┘
                                                         │
                                                         ▼ SSE
                                                ┌──────────────────┐
                                                │    Frontend      │
                                                │  (React/Vue)     │
                                                └──────────────────┘
```

1. **DataLoader** emits events as it loads each asset
2. **Orchestrator** emits events as each transform node runs
3. **StratifyX** aggregates events and streams via SSE
4. **Frontend** receives events and updates UI in real-time

## File Structure

```
include/epoch_data_sdk/events/
├── types.h                    # Event types (LifecycleEvent, ProgressEvent, etc.)
├── path.h                     # EventPath and ScopeType
├── dispatcher.h               # IGenericEventDispatcher interface
├── emitter.h                  # ScopedProgressEmitter (main API)
├── filter.h                   # Event filtering for subscriptions
├── cancellation.h             # CancellationToken
└── all.h                      # Convenience header

src/events/
├── dispatcher.cpp             # Dispatcher implementation
├── filter.cpp                 # Filter implementation
└── CMakeLists.txt

test/events/
├── dispatcher_test.cpp
├── emitter_test.cpp
├── path_test.cpp
└── CMakeLists.txt
```
