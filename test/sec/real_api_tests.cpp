#include <catch2/catch_test_macros.hpp>
#include <epoch_data_sdk/sec/client_factory.hpp>
#include <spdlog/spdlog.h>

using namespace data_sdk::sec;

TEST_CASE("SEC API - Real API Integration Tests", "[sec][real][.]") {
  spdlog::set_level(spdlog::level::debug); // Enable debug logging for troubleshooting
  try {
    // Load API key from .env file
    auto factory = ClientFactory::fromEnv();

    SECTION("QueryClient - Real API call") {
      auto client = factory.createQueryClient();
      auto task = client->getFilingsByTicker("AAPL", epoch_core::FormType::TenK, 5);
      auto result = drogon::sync_wait(task);

      if (!result.has_value()) {
        spdlog::error("Query API failed: {}", result.error().message);
      }
      REQUIRE(result.has_value());
      auto response = result.value();
      spdlog::info("Query API returned {} total filings", response.total.value);
      REQUIRE(response.filings.size() > 0);

      // Log first filing details
      if (!response.filings.empty()) {
        const auto &filing = response.filings[0];
        spdlog::info("First filing: {} - {} filed at {}",
                    filing.ticker, filing.formType, filing.filedAt);
      }
    }

    SECTION("MappingClient - Real API call") {
      auto client = factory.createMappingClient();
      auto task = client->resolveByTicker("AAPL");
      auto result = drogon::sync_wait(task);

      if (!result.has_value()) {
        spdlog::error("Mapping API failed: {}", result.error().message);
      }
      REQUIRE(result.has_value());
      auto mapping = result.value();
      spdlog::info("Mapping: {} -> CIK {}, CUSIP {}",
                  mapping.ticker, mapping.cik, mapping.cusip);
      REQUIRE(mapping.ticker == "AAPL");
      REQUIRE(!mapping.cik.empty());
    }

    SECTION("ExtractorClient - Real API call") {
      // First get a filing URL
      auto query_client = factory.createQueryClient();
      auto query_task = query_client->getFilingsByTicker("AAPL", "10-K", 1);
      auto query_result = drogon::sync_wait(query_task);

      if (query_result.has_value() && !query_result->filings.empty()) {
        const auto &filing = query_result->filings[0];
        // Use linkToFilingDetails instead of deprecated filingUrl
        std::string filing_url = filing.linkToFilingDetails.empty() ? filing.filingUrl : filing.linkToFilingDetails;
        spdlog::info("Testing extractor with filing: {}", filing_url);

        // Extract Risk Factors section
        auto extractor = factory.createExtractorClient();
        auto extract_task = extractor->getSection(filing_url, "1A", "text");
        auto extract_result = drogon::sync_wait(extract_task);

        if (extract_result.has_value()) {
          auto section_text = extract_result.value();
          spdlog::info("Extracted section length: {} characters", section_text.length());
          REQUIRE(section_text.length() > 0);
        } else {
          spdlog::warn("Failed to extract section: {}", extract_result.error().message);
        }
      }
    }

  } catch (const std::exception &e) {
    spdlog::error("Test setup failed: {}", e.what());
    FAIL("Failed to initialize SEC API client: " + std::string(e.what()));
  }
}
