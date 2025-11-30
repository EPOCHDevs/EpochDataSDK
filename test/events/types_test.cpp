#include <catch2/catch_test_macros.hpp>
#include <epoch_data_sdk/events/types.h>

using namespace data_sdk::events;
using epoch_core::GenericEventType;  // Only GenericEventType uses CREATE_ENUM

TEST_CASE("GenericEvent type traits", "[events][types]") {
    SECTION("LifecycleEvent has correct type") {
        LifecycleEvent event;
        GenericEvent generic = event;
        REQUIRE(GetEventType(generic) == GenericEventType::Lifecycle);
    }

    SECTION("ProgressEvent has correct type") {
        ProgressEvent event;
        GenericEvent generic = event;
        REQUIRE(GetEventType(generic) == GenericEventType::Progress);
    }

    SECTION("MetricEvent has correct type") {
        MetricEvent event;
        GenericEvent generic = event;
        REQUIRE(GetEventType(generic) == GenericEventType::Metric);
    }

    SECTION("SummaryEvent has correct type") {
        SummaryEvent event;
        GenericEvent generic = event;
        REQUIRE(GetEventType(generic) == GenericEventType::Summary);
    }

    SECTION("LogEvent has correct type") {
        LogEvent event;
        GenericEvent generic = event;
        REQUIRE(GetEventType(generic) == GenericEventType::Log);
    }
}

TEST_CASE("LifecycleEvent properties", "[events][types]") {
    SECTION("Default values") {
        LifecycleEvent event;
        REQUIRE(event.status == OperationStatus::Pending);
        REQUIRE(event.operation_type.empty());
        REQUIRE(event.operation_name.empty());
        REQUIRE_FALSE(event.duration.has_value());
        REQUIRE_FALSE(event.error_message.has_value());
    }

    SECTION("Setting all fields") {
        LifecycleEvent event;
        event.timestamp = Now();
        event.path = MakeJobPath("abc");
        event.status = OperationStatus::Completed;
        event.operation_type = "stage";
        event.operation_name = "DataLoading";
        event.duration = std::chrono::milliseconds{500};
        event.items_succeeded = 10;
        event.items_failed = 2;
        event.items_total = 12;

        REQUIRE(event.status == OperationStatus::Completed);
        REQUIRE(event.operation_type == "stage");
        REQUIRE(event.duration.value().count() == 500);
        REQUIRE(event.items_succeeded == 10);
        REQUIRE(event.items_failed == 2);
    }
}

TEST_CASE("ProgressEvent properties", "[events][types]") {
    SECTION("GetPercentOrCompute with explicit percent") {
        ProgressEvent event;
        event.progress_percent = 75.5;
        REQUIRE(event.GetPercentOrCompute() == 75.5);
    }

    SECTION("GetPercentOrCompute calculates from current/total") {
        ProgressEvent event;
        event.current = 50;
        event.total = 100;
        REQUIRE(event.GetPercentOrCompute() == 50.0);
    }

    SECTION("GetPercentOrCompute returns 0 for empty event") {
        ProgressEvent event;
        REQUIRE(event.GetPercentOrCompute() == 0.0);
    }

    SECTION("GetPercentOrCompute handles zero total") {
        ProgressEvent event;
        event.current = 50;
        event.total = 0;
        REQUIRE(event.GetPercentOrCompute() == 0.0);
    }

    SECTION("ML metrics in context") {
        ProgressEvent event;
        event.current = 5;
        event.total = 100;
        event.unit = "epochs";
        event.context["loss"] = glz::generic(0.123);
        event.context["accuracy"] = glz::generic(0.95);

        REQUIRE(event.unit == "epochs");
        // Verify context contains the metrics
        REQUIRE(event.context.count("loss") == 1);
        REQUIRE(event.context.count("accuracy") == 1);
    }
}

TEST_CASE("MetricEvent properties", "[events][types]") {
    MetricEvent event;
    event.path = MakeNodePath("j", "s", "p", "node1");
    event.metric_name = "processing_time";
    event.value = 123.45;
    event.unit = "ms";
    event.min_value = 50.0;
    event.max_value = 200.0;

    REQUIRE(event.metric_name == "processing_time");
    REQUIRE(event.value == 123.45);
    REQUIRE(event.unit == "ms");
    REQUIRE(event.min_value == 50.0);
    REQUIRE(event.max_value == 200.0);
}

TEST_CASE("SummaryEvent properties", "[events][types]") {
    SummaryEvent event;
    event.overall_progress_percent = 67.5;
    event.operations_completed = 5;
    event.operations_total = 10;
    event.operations_failed = 1;
    event.operations_running = 2;
    event.currently_running = {"SMA_20", "EMA_50"};

    REQUIRE(event.overall_progress_percent == 67.5);
    REQUIRE(event.operations_completed == 5);
    REQUIRE(event.operations_running == 2);
    REQUIRE(event.currently_running.size() == 2);
    REQUIRE(event.currently_running[0] == "SMA_20");
}

TEST_CASE("LogEvent properties", "[events][types]") {
    SECTION("Default level is Info") {
        LogEvent event;
        REQUIRE(event.level == LogEvent::Level::Info);
    }

    SECTION("All log levels") {
        LogEvent debug, info, warning, error;
        debug.level = LogEvent::Level::Debug;
        info.level = LogEvent::Level::Info;
        warning.level = LogEvent::Level::Warning;
        error.level = LogEvent::Level::Error;

        REQUIRE(debug.level == LogEvent::Level::Debug);
        REQUIRE(info.level == LogEvent::Level::Info);
        REQUIRE(warning.level == LogEvent::Level::Warning);
        REQUIRE(error.level == LogEvent::Level::Error);
    }

    SECTION("With source") {
        LogEvent event;
        event.message = "Processing complete";
        event.source = "rolling_lightgbm";

        REQUIRE(event.source == "rolling_lightgbm");
    }
}

TEST_CASE("GenericEvent path access", "[events][types]") {
    auto path = MakeNodePath("j", "s", "p", "SMA");

    SECTION("GetEventPath retrieves path from LifecycleEvent") {
        LifecycleEvent event;
        event.path = path;
        GenericEvent generic = event;
        REQUIRE(GetEventPath(generic) == path);
    }

    SECTION("GetEventPath retrieves path from ProgressEvent") {
        ProgressEvent event;
        event.path = path;
        GenericEvent generic = event;
        REQUIRE(GetEventPath(generic) == path);
    }

    SECTION("SetEventPath modifies path in variant") {
        LifecycleEvent event;
        event.path = MakeJobPath("old");
        GenericEvent generic = event;

        auto newPath = MakeJobPath("new");
        SetEventPath(generic, newPath);

        REQUIRE(GetEventPath(generic) == newPath);
    }
}

TEST_CASE("GetEventTimestamp", "[events][types]") {
    auto before = Now();

    LifecycleEvent event;
    event.timestamp = Now();
    GenericEvent generic = event;

    auto after = Now();

    auto ts = GetEventTimestamp(generic);
    REQUIRE(ts >= before);
    REQUIRE(ts <= after);
}

TEST_CASE("ToString conversions", "[events][types]") {
    SECTION("OperationStatus to string") {
        // Regular enum uses lowercase snake_case
        REQUIRE(ToString(OperationStatus::Pending) == "pending");
        REQUIRE(ToString(OperationStatus::Started) == "started");
        REQUIRE(ToString(OperationStatus::InProgress) == "in_progress");
        REQUIRE(ToString(OperationStatus::Completed) == "completed");
        REQUIRE(ToString(OperationStatus::Failed) == "failed");
        REQUIRE(ToString(OperationStatus::Cancelled) == "cancelled");
        REQUIRE(ToString(OperationStatus::Skipped) == "skipped");
    }

    SECTION("GenericEventType to string") {
        // CREATE_ENUM generates Capitalized names by default
        REQUIRE(ToString(GenericEventType::Lifecycle) == "Lifecycle");
        REQUIRE(ToString(GenericEventType::Progress) == "Progress");
        REQUIRE(ToString(GenericEventType::Metric) == "Metric");
        REQUIRE(ToString(GenericEventType::Summary) == "Summary");
        REQUIRE(ToString(GenericEventType::Log) == "Log");
    }

    SECTION("LogEvent::Level to string") {
        // Regular enum uses lowercase
        REQUIRE(ToString(LogEvent::Level::Debug) == "debug");
        REQUIRE(ToString(LogEvent::Level::Info) == "info");
        REQUIRE(ToString(LogEvent::Level::Warning) == "warn");
        REQUIRE(ToString(LogEvent::Level::Error) == "error");
    }
}

TEST_CASE("ToMillis conversion", "[events][types]") {
    auto seconds = std::chrono::seconds{5};
    auto millis = ToMillis(seconds);
    REQUIRE(millis.count() == 5000);

    auto microseconds = std::chrono::microseconds{1500};
    millis = ToMillis(microseconds);
    REQUIRE(millis.count() == 1);
}