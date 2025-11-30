// Demo: Console Event Viewer - Full Coverage Demo
// Demonstrates all event types: Lifecycle, Progress, Metric, Summary, Log
#include "console_event_viewer.h"
#include <iostream>
#include <thread>
#include <random>

using namespace data_sdk::events;
using namespace data_sdk::tools;

void SimulateFullPipeline(IGenericEventDispatcherPtr dispatcher, CancellationTokenPtr token) {
    auto jobEmitter = ScopedProgressEmitter(dispatcher, token, MakeJobPath("momentum-backtest-2024"));

    // === JOB START ===
    jobEmitter.EmitStarted("job", "MomentumBacktest");
    jobEmitter.EmitLog(LogEvent::Level::Info, "Starting momentum backtest pipeline");

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> noise(0.0, 0.05);

    // === STAGE 1: DATA LOADING ===
    {
        auto loadStage = jobEmitter.ChildScope(ScopeType::Stage, "DataLoading");
        loadStage.EmitStarted("stage", "DataLoading");
        loadStage.EmitLog(LogEvent::Level::Info, "Loading market data for 5 assets");

        std::vector<std::string> assets = {"AAPL", "GOOGL", "MSFT", "AMZN", "META"};
        std::vector<std::string> categories = {"DailyBars", "Dividends", "Splits"};

        for (size_t i = 0; i < assets.size(); ++i) {
            if (token->IsCancelled()) {
                loadStage.EmitCancelled("stage", "DataLoading");
                return;
            }

            auto assetEmitter = loadStage.ChildScope(ScopeType::Asset, assets[i]);
            assetEmitter.EmitStarted("asset", assets[i]);

            // Load each category
            for (size_t c = 0; c < categories.size(); ++c) {
                auto catEmitter = assetEmitter.ChildScope(ScopeType::Transform, categories[c]);
                catEmitter.EmitStarted("category", categories[c]);

                // Simulate loading with progress
                size_t rows = 1000 + (i * 100) + (c * 50);
                for (size_t j = 0; j <= 10; ++j) {
                    catEmitter.SetContext("rows_loaded", static_cast<int64_t>(j * rows / 10));
                    catEmitter.EmitProgress(j, 10, "Loading " + categories[c]);
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }

                // Emit metric for load time
                catEmitter.EmitMetric("load_time_ms", 45.0 + noise(gen) * 100, "ms");
                catEmitter.EmitCompleted("category", categories[c]);
            }

            assetEmitter.EmitCompleted("asset", assets[i]);

            // Summary update
            loadStage.EmitSummary(
                static_cast<double>(i + 1) / assets.size() * 100.0,
                i + 1, assets.size(), 0, {assets[i]}
            );
        }

        loadStage.EmitLog(LogEvent::Level::Info, "Data loading complete: 5 assets, 3 categories each");
        loadStage.EmitCompleted("stage", "DataLoading");
    }

    // === STAGE 2: TRANSFORMS ===
    {
        auto transformStage = jobEmitter.ChildScope(ScopeType::Stage, "Transform");
        transformStage.EmitStarted("stage", "Transform");

        struct TransformConfig {
            std::string name;
            bool isML;
            size_t iterations;
        };

        std::vector<TransformConfig> transforms = {
            {"SMA_20", false, 100},
            {"RSI_14", false, 100},
            {"MACD", false, 100},
            {"RollingPCA", true, 50},
            {"RollingLightGBM", true, 30}
        };

        for (size_t t = 0; t < transforms.size(); ++t) {
            if (token->IsCancelled()) {
                transformStage.EmitCancelled("stage", "Transform");
                return;
            }

            const auto& config = transforms[t];
            auto nodeEmitter = transformStage.ChildScope(ScopeType::Node, config.name);
            nodeEmitter.EmitStarted("node", config.name);

            if (config.isML) {
                // ML Transform - emit training progress with metrics
                nodeEmitter.SetContext("model", config.name);
                nodeEmitter.SetContext("window_size", int64_t{252});
                nodeEmitter.EmitLog(LogEvent::Level::Info, "Starting ML training: " + config.name);

                double loss = 1.0;
                double accuracy = 0.5;

                for (size_t epoch = 0; epoch <= config.iterations; ++epoch) {
                    loss = 1.0 / (1 + epoch * 0.1) + noise(gen);
                    accuracy = 0.5 + 0.45 * (1 - 1.0 / (1 + epoch * 0.05));
                    double lr = 0.01 * std::pow(0.95, epoch / 10.0);

                    nodeEmitter.SetContext("loss", loss);
                    nodeEmitter.SetContext("accuracy", accuracy);
                    nodeEmitter.SetContext("lr", lr);
                    nodeEmitter.SetContext("epoch", static_cast<int64_t>(epoch));
                    nodeEmitter.EmitProgress(epoch, config.iterations, "Training epoch");

                    // Emit metrics periodically
                    if (epoch % 10 == 0) {
                        nodeEmitter.EmitMetric("loss", loss);
                        nodeEmitter.EmitMetric("accuracy", accuracy, "%");
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }

                nodeEmitter.EmitMetric("final_loss", loss);
                nodeEmitter.EmitMetric("final_accuracy", accuracy * 100, "%");
                nodeEmitter.EmitLog(LogEvent::Level::Info,
                    "Training complete: loss=" + std::to_string(loss) +
                    " accuracy=" + std::to_string(accuracy * 100) + "%");
            } else {
                // Regular transform - process each asset
                std::vector<std::string> assets = {"AAPL", "GOOGL", "MSFT", "AMZN", "META"};

                for (size_t a = 0; a < assets.size(); ++a) {
                    auto assetEmitter = nodeEmitter.ChildScope(ScopeType::Asset, assets[a]);
                    assetEmitter.EmitStarted("asset", assets[a]);

                    for (size_t i = 0; i <= 20; ++i) {
                        assetEmitter.EmitProgress(i, 20, "Computing " + config.name);
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }

                    assetEmitter.EmitCompleted("asset", assets[a]);
                }
            }

            nodeEmitter.EmitCompleted("node", config.name);

            // Summary for transform stage
            std::vector<std::string> running;
            if (t < transforms.size() - 1) {
                running.push_back(transforms[t + 1].name);
            }
            transformStage.EmitSummary(
                static_cast<double>(t + 1) / transforms.size() * 100.0,
                t + 1, transforms.size(), 0, running
            );
        }

        transformStage.EmitLog(LogEvent::Level::Info, "All transforms complete");
        transformStage.EmitCompleted("stage", "Transform");
    }

    // === STAGE 3: BACKTEST (simulated failure) ===
    {
        auto backtestStage = jobEmitter.ChildScope(ScopeType::Stage, "Backtest");
        backtestStage.EmitStarted("stage", "Backtest");
        backtestStage.EmitLog(LogEvent::Level::Info, "Starting backtest simulation");

        for (size_t day = 0; day < 100; ++day) {
            if (token->IsCancelled()) {
                backtestStage.EmitCancelled("stage", "Backtest");
                return;
            }

            backtestStage.SetContext("pnl", noise(gen) * 1000 - 250);
            backtestStage.SetContext("sharpe", 0.5 + noise(gen) * 2);
            backtestStage.EmitProgress(day, 252, "Simulating day " + std::to_string(day));

            if (day % 20 == 0) {
                backtestStage.EmitMetric("daily_pnl", noise(gen) * 1000 - 250, "USD");
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }

        // Simulate a warning
        backtestStage.EmitLog(LogEvent::Level::Warning, "Low liquidity detected on some assets");

        // Complete backtest
        backtestStage.EmitMetric("total_pnl", 15234.56, "USD");
        backtestStage.EmitMetric("sharpe_ratio", 1.85);
        backtestStage.EmitMetric("max_drawdown", -8.5, "%");
        backtestStage.EmitLog(LogEvent::Level::Info, "Backtest complete - Sharpe: 1.85");
        backtestStage.EmitCompleted("stage", "Backtest");
    }

    // === JOB COMPLETE ===
    jobEmitter.EmitSummary(100.0, 3, 3, 0, {});
    jobEmitter.EmitLog(LogEvent::Level::Info, "Pipeline complete - Total runtime: ~30s");
    jobEmitter.EmitCompleted("job", "MomentumBacktest");
}

int main(int argc, char* argv[]) {
    std::cout << "╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║        EpochDataSDK Event System - Console Viewer          ║\n";
    std::cout << "║                    Full Coverage Demo                      ║\n";
    std::cout << "╠════════════════════════════════════════════════════════════╣\n";
    std::cout << "║  Event Types Demonstrated:                                 ║\n";
    std::cout << "║    - LifecycleEvent (Started, Completed, Failed, etc.)     ║\n";
    std::cout << "║    - ProgressEvent (progress bars, ML training epochs)     ║\n";
    std::cout << "║    - MetricEvent (loss, accuracy, PnL, Sharpe)             ║\n";
    std::cout << "║    - SummaryEvent (overall progress aggregation)           ║\n";
    std::cout << "║    - LogEvent (Info, Warning, Error messages)              ║\n";
    std::cout << "╠════════════════════════════════════════════════════════════╣\n";
    std::cout << "║  Hierarchy:                                                ║\n";
    std::cout << "║    Job -> Stage -> Node -> Asset -> Category               ║\n";
    std::cout << "╠════════════════════════════════════════════════════════════╣\n";
    std::cout << "║  Press 'q' or ESC to quit at any time                      ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n\n";
    std::cout << "Starting in 3 seconds...\n";
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Create dispatcher and cancellation token
    auto dispatcher = MakeGenericEventDispatcher();
    auto token = MakeCancellationToken();

    // Create and start viewer
    auto viewer = MakeConsoleEventViewer(dispatcher);
    viewer->Start();

    // Run pipeline simulation
    SimulateFullPipeline(dispatcher, token);

    // Keep viewer running to show final state
    std::this_thread::sleep_for(std::chrono::seconds(5));

    viewer->Stop();

    std::cout << "\n╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                    Demo Complete!                          ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n";

    return 0;
}
