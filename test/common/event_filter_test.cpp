#include <catch2/catch_test_macros.hpp>
#include <vector>

#include <epoch_data_sdk/common/event_filter.h>
#include <epoch_data_sdk/common/event_types.h>

using namespace data_sdk::events;

TEST_CASE("EventFilter basic sets", "[events][filter]") {
    const auto all = EventFilter::All();
    const auto none = EventFilter::None();
    const auto pipeline = EventFilter::PipelineOnly();
    const auto nodes = EventFilter::NodesOnly();

    REQUIRE(all.Accepts(EventType::PipelineStarted));
    REQUIRE_FALSE(none.Accepts(EventType::PipelineStarted));

    REQUIRE(pipeline.Accepts(EventType::PipelineCompleted));
    REQUIRE_FALSE(pipeline.Accepts(EventType::NodeStarted));

    REQUIRE(nodes.Accepts(EventType::NodeStarted));
    REQUIRE(nodes.Accepts(EventType::NodeCompleted));
    REQUIRE_FALSE(nodes.Accepts(EventType::PipelineCompleted));
}

TEST_CASE("EventFilter Only and Except combinations", "[events][filter]") {
    auto onlyPipeline = EventFilter::Only({EventType::PipelineStarted, EventType::PipelineFailed});
    REQUIRE(onlyPipeline.Accepts(EventType::PipelineStarted));
    REQUIRE_FALSE(onlyPipeline.Accepts(EventType::NodeStarted));

    auto exceptProgress = EventFilter::Except({EventType::TransformProgress, EventType::ProgressSummary});
    REQUIRE(exceptProgress.Accepts(EventType::PipelineCompleted));
    REQUIRE_FALSE(exceptProgress.Accepts(EventType::TransformProgress));

    auto combined = EventFilter::PipelineOnly() | EventFilter::ProgressOnly();
    REQUIRE(combined.Accepts(EventType::PipelineStarted));
    REQUIRE(combined.Accepts(EventType::TransformProgress));
    REQUIRE_FALSE(combined.Accepts(EventType::NodeStarted));
}
