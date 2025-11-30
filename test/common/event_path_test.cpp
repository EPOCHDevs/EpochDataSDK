#include <catch2/catch_test_macros.hpp>
#include <epoch_data_sdk/common/event_path.h>
#include <unordered_set>

using namespace data_sdk::events;

TEST_CASE("EventPath construction and basic operations", "[events][path]") {
    SECTION("Default constructor creates empty path") {
        EventPath path;
        REQUIRE(path.IsEmpty());
        REQUIRE(path.Depth() == 0);
        REQUIRE(path.ToString().empty());
    }

    SECTION("Single segment constructor") {
        EventPath path("job", "abc123");
        REQUIRE_FALSE(path.IsEmpty());
        REQUIRE(path.Depth() == 1);
        REQUIRE(path.ToString() == "job:abc123");
    }

    SECTION("Initializer list constructor") {
        EventPath path{{"job", "abc"}, {"stage", "Run"}};
        REQUIRE(path.Depth() == 2);
        REQUIRE(path.ToString() == "job:abc/stage:Run");
    }
}

TEST_CASE("EventPath parsing", "[events][path]") {
    SECTION("Parse empty string returns empty path") {
        auto path = EventPath::Parse("");
        REQUIRE(path.IsEmpty());
    }

    SECTION("Parse single segment") {
        auto path = EventPath::Parse("job:abc123");
        REQUIRE(path.Depth() == 1);
        REQUIRE(path.GetSegment("job") == "abc123");
    }

    SECTION("Parse multiple segments") {
        auto path = EventPath::Parse("job:abc/stage:Run/node:SMA_20");
        REQUIRE(path.Depth() == 3);
        REQUIRE(path.GetSegment("job") == "abc");
        REQUIRE(path.GetSegment("stage") == "Run");
        REQUIRE(path.GetSegment("node") == "SMA_20");
    }

    SECTION("Parse throws on invalid segment without colon") {
        REQUIRE_THROWS_AS(EventPath::Parse("job:abc/invalid"), std::invalid_argument);
    }

    SECTION("Parse handles trailing slash") {
        auto path = EventPath::Parse("job:abc/");
        REQUIRE(path.Depth() == 1);
    }
}

TEST_CASE("EventPath hierarchy operations", "[events][path]") {
    EventPath root("job", "abc");
    EventPath child = root.Child("stage", "Run");
    EventPath grandchild = child.Child("node", "SMA");

    SECTION("Child creates extended path") {
        REQUIRE(child.Depth() == 2);
        REQUIRE(child.ToString() == "job:abc/stage:Run");
    }

    SECTION("Parent returns path without last segment") {
        auto parent = grandchild.Parent();
        REQUIRE(parent.Depth() == 2);
        REQUIRE(parent == child);
    }

    SECTION("Parent of root returns empty path") {
        auto parent = root.Parent();
        REQUIRE(parent.IsEmpty());
    }

    SECTION("Parent of empty returns empty") {
        EventPath empty;
        auto parent = empty.Parent();
        REQUIRE(parent.IsEmpty());
    }

    SECTION("IsDescendantOf returns true for descendants") {
        REQUIRE(child.IsDescendantOf(root));
        REQUIRE(grandchild.IsDescendantOf(root));
        REQUIRE(grandchild.IsDescendantOf(child));
    }

    SECTION("IsDescendantOf returns true for self") {
        REQUIRE(root.IsDescendantOf(root));
    }

    SECTION("IsDescendantOf returns false for non-ancestors") {
        EventPath other("job", "xyz");
        REQUIRE_FALSE(child.IsDescendantOf(other));
        REQUIRE_FALSE(root.IsDescendantOf(child));
    }

    SECTION("IsAncestorOf is the inverse of IsDescendantOf") {
        REQUIRE(root.IsAncestorOf(child));
        REQUIRE(root.IsAncestorOf(grandchild));
        REQUIRE(child.IsAncestorOf(grandchild));
        REQUIRE_FALSE(child.IsAncestorOf(root));
    }
}

TEST_CASE("EventPath segment access", "[events][path]") {
    auto path = EventPath::Parse("job:abc/stage:Run/node:SMA_20/asset:AAPL");

    SECTION("GetSegment returns value for existing scope") {
        REQUIRE(path.GetSegment("job") == "abc");
        REQUIRE(path.GetSegment("asset") == "AAPL");
    }

    SECTION("GetSegment returns nullopt for missing scope") {
        REQUIRE_FALSE(path.GetSegment("pipeline").has_value());
    }

    SECTION("GetLastScope and GetLastId") {
        REQUIRE(path.GetLastScope() == "asset");
        REQUIRE(path.GetLastId() == "AAPL");
    }

    SECTION("GetRoot returns first segment") {
        auto root = path.GetRoot();
        REQUIRE(root.has_value());
        REQUIRE(root->scope == "job");
        REQUIRE(root->id == "abc");
    }

    SECTION("GetRoot on empty returns nullopt") {
        EventPath empty;
        REQUIRE_FALSE(empty.GetRoot().has_value());
    }
}

TEST_CASE("EventPath equality and comparison", "[events][path]") {
    EventPath path1 = EventPath::Parse("job:abc/stage:Run");
    EventPath path2 = EventPath::Parse("job:abc/stage:Run");
    EventPath path3 = EventPath::Parse("job:xyz/stage:Run");

    SECTION("Equality operator") {
        REQUIRE(path1 == path2);
        REQUIRE_FALSE(path1 == path3);
    }

    SECTION("Inequality operator") {
        REQUIRE_FALSE(path1 != path2);
        REQUIRE(path1 != path3);
    }

    SECTION("Less-than for ordering") {
        REQUIRE((path1 < path3) != (path3 < path1));
    }

    SECTION("Hash works for unordered containers") {
        std::unordered_set<EventPath, EventPath::Hash> pathSet;
        pathSet.insert(path1);
        pathSet.insert(path2);  // Duplicate
        pathSet.insert(path3);

        REQUIRE(pathSet.size() == 2);
        REQUIRE(pathSet.count(path1) == 1);
        REQUIRE(pathSet.count(path3) == 1);
    }
}

TEST_CASE("EventPath factory functions", "[events][path]") {
    SECTION("MakeJobPath creates job-level path") {
        auto path = MakeJobPath("job123");
        REQUIRE(path.ToString() == "job:job123");
    }

    SECTION("MakeStagePath creates job/stage path") {
        auto path = MakeStagePath("job123", "RunCampaign");
        REQUIRE(path.ToString() == "job:job123/stage:RunCampaign");
    }

    SECTION("MakePipelinePath creates full pipeline path") {
        auto path = MakePipelinePath("job123", "Run", "transforms");
        REQUIRE(path.ToString() == "job:job123/stage:Run/pipeline:transforms");
    }

    SECTION("MakeNodePath creates full node path") {
        auto path = MakeNodePath("j", "s", "p", "SMA_20");
        REQUIRE(path.ToString() == "job:j/stage:s/pipeline:p/node:SMA_20");
    }

    SECTION("MakeAssetPath creates full asset path") {
        auto path = MakeAssetPath("j", "s", "p", "n", "AAPL");
        REQUIRE(path.ToString() == "job:j/stage:s/pipeline:p/node:n/asset:AAPL");
    }
}