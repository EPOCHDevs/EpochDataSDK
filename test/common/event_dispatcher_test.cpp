#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <vector>

#include <epoch_data_sdk/common/event_dispatcher.h>
#include <epoch_data_sdk/common/event_filter.h>

using namespace data_sdk::events;

TEST_CASE("EventDispatcher routes events to subscribers", "[events][dispatcher]") {
    EventDispatcher dispatcher;
    std::vector<EventType> received;

    dispatcher.Subscribe([&](const OrchestratorEvent& e) {
        received.push_back(GetEventType(e));
    });

    dispatcher.Emit(PipelineStartedEvent{.timestamp = Now(), .total_nodes = 2});
    dispatcher.Emit(NodeStartedEvent{
        .timestamp = Now(),
        .node_id = "node-1",
        .operation_name = "load",
        .is_cross_sectional = false,
        .node_index = 0,
        .total_nodes = 2,
        .asset_count = 1});

    REQUIRE(received.size() == 2);
    REQUIRE(received[0] == EventType::PipelineStarted);
    REQUIRE(received[1] == EventType::NodeStarted);
}

TEST_CASE("EventDispatcher filtering and typed subscriptions", "[events][dispatcher]") {
    EventDispatcher dispatcher;

    SECTION("Filters by EventFilter") {
        std::vector<EventType> nodeEvents;

        dispatcher.Subscribe(
            [&](const OrchestratorEvent& e) { nodeEvents.push_back(GetEventType(e)); },
            EventFilter::NodesOnly());

        dispatcher.Emit(PipelineStartedEvent{});
        dispatcher.Emit(NodeStartedEvent{});
        dispatcher.Emit(NodeCompletedEvent{});
        dispatcher.Emit(TransformProgressEvent{});

        REQUIRE(nodeEvents.size() == 2);
        REQUIRE(nodeEvents[0] == EventType::NodeStarted);
        REQUIRE(nodeEvents[1] == EventType::NodeCompleted);
    }

    SECTION("Typed subscription receives matching payload") {
        NodeFailedEvent captured{};
        dispatcher.SubscribeTo<NodeFailedEvent>([&](const NodeFailedEvent& e) {
            captured = e;
        });

        dispatcher.Emit(NodeFailedEvent{
            .timestamp = Now(),
            .node_id = "alpha",
            .operation_name = "load",
            .error_message = "boom",
            .asset_id = "AAPL"});

        REQUIRE(captured.node_id == "alpha");
        REQUIRE(captured.operation_name == "load");
        REQUIRE(captured.error_message == "boom");
        REQUIRE(captured.asset_id.has_value());
        REQUIRE(*captured.asset_id == "AAPL");
    }

    SECTION("SubscriberCount reflects active slots and disconnects") {
        auto c1 = dispatcher.Subscribe([](const auto&) {});
        auto c2 = dispatcher.Subscribe([](const auto&) {});

        REQUIRE(dispatcher.SubscriberCount() == 2);

        c1.disconnect();
        REQUIRE(dispatcher.SubscriberCount() == 1);

        c2.disconnect();
        REQUIRE(dispatcher.SubscriberCount() == 0);
    }
}
