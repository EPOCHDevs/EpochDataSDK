#include <catch2/catch_test_macros.hpp>
#include <epoch_data_sdk/events/all.h>

using namespace data_sdk::events;

TEST_CASE("ScopedProgressEmitter basic lifecycle", "[events][scoped_emitter]") {
    auto dispatcher = MakeGenericEventDispatcher();
    auto token = MakeCancellationToken();
    auto emitter = ScopedProgressEmitter(dispatcher, token, MakeJobPath("job-1"));

    std::vector<LifecycleEvent> events;
    dispatcher->SubscribeTo<LifecycleEvent>([&](const auto& e) { events.push_back(e); });

    emitter.EmitStarted("stage", "Load");
    emitter.EmitCompleted("stage", "Load");

    REQUIRE(events.size() == 2);
    REQUIRE(events[0].status == OperationStatus::Started);
    REQUIRE(events[1].status == OperationStatus::Completed);
    REQUIRE(events[0].path.ToString() == "Job:job-1");
}

TEST_CASE("ScopedProgressEmitter child scope extends path", "[events][scoped_emitter]") {
    auto dispatcher = MakeGenericEventDispatcher();
    auto token = MakeCancellationToken();
    auto parent = ScopedProgressEmitter(dispatcher, token, MakeJobPath("job-2"));

    LifecycleEvent received{};
    dispatcher->SubscribeTo<LifecycleEvent>([&](const auto& e) { received = e; });

    auto child = parent.ChildScope(ScopeType::Asset, "AAPL");
    child.EmitStarted("asset", "Processing");

    REQUIRE(received.path.ToString() == "Job:job-2/Asset:AAPL");
}

TEST_CASE("ScopedProgressEmitter context propagates to events", "[events][scoped_emitter]") {
    auto dispatcher = MakeGenericEventDispatcher();
    auto token = MakeCancellationToken();
    auto emitter = ScopedProgressEmitter(dispatcher, token, MakeJobPath("job-3"));

    LifecycleEvent received{};
    dispatcher->SubscribeTo<LifecycleEvent>([&](const auto& e) { received = e; });

    emitter.SetContext("total_assets", int64_t{50});
    emitter.EmitStarted("dataloader", "LoadData");

    REQUIRE(received.context.contains("total_assets"));
}

TEST_CASE("ScopedProgressEmitter cancellation", "[events][scoped_emitter]") {
    auto dispatcher = MakeGenericEventDispatcher();
    auto token = MakeCancellationToken();
    auto emitter = ScopedProgressEmitter(dispatcher, token, MakeJobPath("job-4"));

    REQUIRE_FALSE(emitter.IsCancelled());
    token->Cancel();
    REQUIRE(emitter.IsCancelled());
    REQUIRE_THROWS_AS(emitter.ThrowIfCancelled(), OperationCancelledException);
}

TEST_CASE("ScopedProgressEmitter EmitFailed includes error", "[events][scoped_emitter]") {
    auto dispatcher = MakeGenericEventDispatcher();
    auto token = MakeCancellationToken();
    auto emitter = ScopedProgressEmitter(dispatcher, token, MakeJobPath("job-5"));

    LifecycleEvent received{};
    dispatcher->SubscribeTo<LifecycleEvent>([&](const auto& e) { received = e; });

    emitter.EmitFailed("asset", "AAPL", "Merge failed");

    REQUIRE(received.status == OperationStatus::Failed);
    REQUIRE(received.error_message == "Merge failed");
}

TEST_CASE("ScopedProgressEmitter progress events", "[events][scoped_emitter]") {
    auto dispatcher = MakeGenericEventDispatcher();
    auto token = MakeCancellationToken();
    auto emitter = ScopedProgressEmitter(dispatcher, token, MakeJobPath("job-6"));

    ProgressEvent received{};
    dispatcher->SubscribeTo<ProgressEvent>([&](const auto& e) { received = e; });

    emitter.EmitProgress(25, 100, "Loading", "rows");

    REQUIRE(received.current == 25);
    REQUIRE(received.total == 100);
    REQUIRE(received.message == "Loading");
}

TEST_CASE("ScopedOperation RAII emits Started and Completed", "[events][scoped_emitter]") {
    auto dispatcher = MakeGenericEventDispatcher();
    auto token = MakeCancellationToken();
    auto emitter = ScopedProgressEmitter(dispatcher, token, MakeJobPath("job-7"));

    std::vector<LifecycleEvent> events;
    dispatcher->SubscribeTo<LifecycleEvent>([&](const auto& e) { events.push_back(e); });

    {
        ScopedOperation op(emitter, "stage", "Process");
        REQUIRE(events.size() == 1);
    }

    REQUIRE(events.size() == 2);
    REQUIRE(events[0].status == OperationStatus::Started);
    REQUIRE(events[1].status == OperationStatus::Completed);
}

TEST_CASE("Null emitter does not crash", "[events][scoped_emitter]") {
    auto emitter = MakeNullScopedProgressEmitter();

    // Should not crash
    emitter->EmitStarted("test", "op");
    emitter->EmitProgress(1, 10);
    emitter->EmitFailed("test", "op", "error");
    emitter->EmitCompleted("test", "op");
}