/**
 * @file sec_api_example.cpp
 * @brief Example demonstrating SEC API usage
 *
 * This example shows how to use the SEC API clients to:
 * 1. Search for filings by ticker
 * 2. Resolve ticker to CIK using Mapping API
 * 3. Extract sections from filings
 */

#include <iostream>
#include <spdlog/spdlog.h>

#include <epoch_data_sdk/sec/client_factory.hpp>

using namespace data_sdk::sec;

int main() {
  try {
    // Create factory from .env file (loads SEC_API_KEY)
    auto factory = ClientFactory::fromEnv();

    spdlog::info("SEC API Example - Starting");

    // Example 1: Query filings by ticker
    {
      spdlog::info("\n=== Example 1: Query Filings by Ticker ===");
      auto query_client = factory.createQueryClient();

      auto task = query_client->getFilingsByTicker("AAPL", "10-K", 5);
      auto result = drogon::sync_wait(task);

      if (result.has_value()) {
        auto response = result.value();
        spdlog::info("Found {} total filings", response.total);

        for (const auto& filing : response.filings) {
          spdlog::info("  - {} {} filed on {}",
                      filing.ticker, filing.formType, filing.filedAt);
        }
      } else {
        spdlog::error("Query failed: {}", result.error().message);
      }
    }

    // Example 2: Mapping - Resolve ticker to CIK
    {
      spdlog::info("\n=== Example 2: Mapping API ===");
      auto mapping_client = factory.createMappingClient();

      auto task = mapping_client->resolveByTicker("AAPL");
      auto result = drogon::sync_wait(task);

      if (result.has_value()) {
        auto mapping = result.value();
        spdlog::info("Ticker: {} -> CIK: {}", mapping.ticker, mapping.cik);
        spdlog::info("Company: {}", mapping.name);
        spdlog::info("CUSIP: {}", mapping.cusip);
        spdlog::info("Exchange: {}", mapping.exchange);
      } else {
        spdlog::error("Mapping failed: {}", result.error().message);
      }
    }

    // Example 3: Extract section from filing
    {
      spdlog::info("\n=== Example 3: Extract Filing Section ===");

      // First get a filing URL
      auto query_client = factory.createQueryClient();
      auto query_task = query_client->getFilingsByTicker("AAPL", "10-K", 1);
      auto query_result = drogon::sync_wait(query_task);

      if (query_result.has_value() && !query_result->filings.empty()) {
        const auto& filing = query_result->filings[0];
        spdlog::info("Extracting Risk Factors from: {}", filing.filingUrl);

        // Extract section 1A (Risk Factors) from 10-K
        auto extractor = factory.createExtractorClient();
        auto extract_task = extractor->getSection(filing.filingUrl, "1A", "text");
        auto extract_result = drogon::sync_wait(extract_task);

        if (extract_result.has_value()) {
          auto section_text = extract_result.value();
          spdlog::info("Extracted {} characters", section_text.length());
          // Print first 500 characters
          spdlog::info("Preview: {}...",
                      section_text.substr(0, std::min(size_t(500), section_text.length())));
        } else {
          spdlog::error("Extraction failed: {}", extract_result.error().message);
        }
      }
    }

    spdlog::info("\n=== Example Complete ===");

  } catch (const std::exception& e) {
    spdlog::error("Error: {}", e.what());
    return 1;
  }

  return 0;
}
