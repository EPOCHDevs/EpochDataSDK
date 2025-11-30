#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <epoch_data_sdk/common/event_dispatcher.h>
#include <epoch_data_sdk/common/progress_emitter.h>

using namespace data_sdk::events;

TEST_CASE("TransformProgressEmitter emits progress with context", "[events][emitter]") {
    auto dispatcher = MakeEventDispatcher();
    auto token = MakeCancellationToken();
    TransformProgressEmitter emitter(dispatcher, token, "node-42", "load");

    TransformProgressEvent received{};
    dispatcher->SubscribeTo<TransformProgressEvent>([&](const auto& e) { received = e; });

    emitter.SetAssetId("AST");
    emitter.EmitProgress(5, 20, "halfway");

    REQUIRE(received.node_id == "node-42");
    REQUIRE(received.operation_name == "load");
    REQUIRE(received.asset_id.has_value());
    REQUIRE(*received.asset_id == "AST");
    REQUIRE(received.current_step.has_value());
    REQUIRE(*received.current_step == 5);
    REQUIRE(received.total_steps.has_value());
    REQUIRE(*received.total_steps == 20);
    REQUIRE(received.message == "halfway");
    REQUIRE(received.progress_percent.has_value());
    REQUIRE_THAT(*received.progress_percent, Catch::Matchers::WithinAbs(25.0, 0.001));
}

TEST_CASE("TransformProgressEmitter epoch and iteration helpers", "[events][emitter]") {
    auto dispatcher = MakeEventDispatcher();
    auto token = MakeCancellationToken();
    TransformProgressEmitter emitter(dispatcher, token, "node-epoch", "train");

    SECTION("EmitEpoch populates loss/accuracy and message") {
        TransformProgressEvent received{};
        dispatcher->SubscribeTo<TransformProgressEvent>([&](const auto& e) { received = e; });

        emitter.EmitEpoch(2, 4, 0.5, 0.9, 0.01);

        REQUIRE(received.current_step == 2);
        REQUIRE(received.total_steps == 4);
        REQUIRE(received.loss.has_value());
        REQUIRE(received.accuracy.has_value());
        REQUIRE(received.learning_rate.has_value());
        REQUIRE(received.message.find("Epoch 2/4") != std::string::npos);
    }

    SECTION("EmitIteration attaches metric metadata") {
        TransformProgressEvent received{};
        dispatcher->SubscribeTo<TransformProgressEvent>([&](const auto& e) { received = e; });

        emitter.EmitIteration(7, 0.123, "");

        REQUIRE(received.iteration == 7);
        REQUIRE(received.metadata.contains("metric"));
    }
}

TEST_CASE("TransformProgressEmitter honors cancellation", "[events][emitter][cancel]") {
    auto dispatcher = MakeEventDispatcher();
    auto token = MakeCancellationToken();
    TransformProgressEmitter emitter(dispatcher, token, "node-cancel", "op");

    token->Cancel();
    REQUIRE(emitter.IsCancelled());
    REQUIRE_THROWS_AS(emitter.ThrowIfCancelled(), OperationCancelledException);
    REQUIRE_THROWS_AS(emitter.EmitEpochOrCancel(1, 2), OperationCancelledException);
    REQUIRE_THROWS_AS(emitter.EmitIterationOrCancel(3), OperationCancelledException);
}
