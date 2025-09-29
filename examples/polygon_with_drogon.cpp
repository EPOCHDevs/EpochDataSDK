/**
 * Example: Using PolygonClient with Drogon's event loop
 *
 * This shows how to efficiently integrate PolygonClient with a Drogon
 * web server by reusing Drogon's main event loop.
 */

#include <epoch_data_sdk/polygon/polygon_client.hpp>
#include <drogon/drogon.h>
#include <memory>

int main() {
    // Configure polygon client to use Drogon's main loop
    data_sdk::polygon::Options opts;
    opts.api_key = std::getenv("POLYGON_API_KEY") ?: "demo";
    opts.use_drogon_main_loop = true;  // <-- This is the key flag

    // Create client - will use Drogon's loop once app.run() is called
    auto polygon_client = std::make_shared<data_sdk::polygon::PolygonClient>(opts);

    // Set up a simple Drogon HTTP server
    drogon::app()
        .addListener("0.0.0.0", 8080)
        .setThreadNum(4)  // 4 I/O threads

        // API endpoint that fetches data from Polygon
        .registerHandler("/api/stock/{ticker}",
            [polygon_client](const drogon::HttpRequestPtr& req,
                           std::function<void(const drogon::HttpResponsePtr&)>&& callback,
                           const std::string& ticker) {

                // Fetch aggregates from Polygon
                auto result = polygon_client->getAggregates(
                    ticker, "2024-01-01", "2024-01-31", true
                );

                auto resp = drogon::HttpResponse::newHttpJsonResponse();

                if (result.has_value()) {
                    // Convert DataFrame to JSON (simplified)
                    Json::Value json;
                    json["ticker"] = ticker;
                    json["rows"] = static_cast<int>(result->num_rows());
                    json["status"] = "success";
                    resp->setJsonObject(json);
                } else {
                    Json::Value json;
                    json["error"] = result.error().message;
                    json["status"] = "error";
                    resp->setStatusCode(drogon::k500InternalServerError);
                    resp->setJsonObject(json);
                }

                callback(resp);
            })

        .run();

    return 0;
}

/**
 * Benefits of use_drogon_main_loop = true:
 *
 * 1. No extra thread created - uses existing Drogon event loop
 * 2. Better resource utilization
 * 3. Simpler async coordination
 * 4. Reduced context switching
 *
 * When NOT to use:
 * - If you need isolation from the main loop
 * - If you're not running a Drogon server
 * - If you need guaranteed dedicated resources
 */