#include "epoch_data_sdk/polygon/polygon_client.hpp"
#include "epoch_data_sdk/polygon/options.hpp"
#include <catch2/catch_all.hpp>
#include <drogon/drogon.h>
#include <thread>
#include <chrono>

using namespace data_sdk::polygon;
using namespace std::chrono_literals;

TEST_CASE("PolygonClient with use_drogon_main_loop flag", "[polygon][drogon-loop]") {
  SECTION("Create client with flag but no Drogon app running") {
    Options opts;
    opts.api_key = "test_key";
    opts.use_drogon_main_loop = true;

    // This should NOT crash - should fall back to own thread
    REQUIRE_NOTHROW([&opts]() {
      PolygonClient client(opts);
      // Client created successfully with fallback
    }());
  }

  SECTION("Create client with Drogon app running") {
    // Start Drogon in background
    std::atomic<bool> drogon_started{false};
    std::thread drogon_thread([&drogon_started]() {
      drogon::app()
        .setThreadNum(2)
        .setLogLevel(trantor::Logger::kError)
        .registerSyncAdvice([&drogon_started](const drogon::HttpRequestPtr&) {
          drogon_started = true;
          return drogon::HttpResponse::newHttpResponse();
        })
        .addListener("127.0.0.1", 18080)
        .run();
    });

    // Wait for Drogon to start
    std::this_thread::sleep_for(200ms);

    Options opts;
    opts.api_key = "test_key";
    opts.use_drogon_main_loop = true;

    // This should work with Drogon's loop
    REQUIRE_NOTHROW([&opts]() {
      PolygonClient client(opts);

      // Try to make a request (will fail with test key, but shouldn't crash)
      auto result = client.getAggregates("AAPL", "2024-01-01", "2024-01-02", true);

      // We expect it to fail due to invalid API key
      REQUIRE(!result.has_value());
    }());

    // Cleanup
    drogon::app().quit();
    drogon_thread.join();
  }

  SECTION("Multiple clients with same flag") {
    Options opts;
    opts.api_key = "test_key";
    opts.use_drogon_main_loop = true;

    // Create multiple clients - should not crash
    REQUIRE_NOTHROW([&opts]() {
      std::vector<std::unique_ptr<PolygonClient>> clients;
      for (int i = 0; i < 5; ++i) {
        clients.push_back(std::make_unique<PolygonClient>(opts));
      }

      // All clients created successfully
      REQUIRE(clients.size() == 5);
    }());
  }
}