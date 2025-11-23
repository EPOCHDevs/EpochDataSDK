#pragma once

#include <memory>
#include <cstdlib>
#include <spdlog/spdlog.h>

#include <epoch_data_sdk/dataloader/cross_sectional_fetcher.hpp>
#include "../fred/alfred_client.hpp"
#include "../fred/cross_sectional_series_map.hpp"
#include "../fred/options.hpp"

namespace data_sdk::dataloader {

// FRED implementation of cross-sectional economic data fetcher
// Uses AlfredClient for point-in-time data with revision tracking
// Note: Some series (e.g., daily interest rates) do not exist in ALFRED and will fail
class FredCrossSectionalFetcher : public ICrossSectionalFetcher {
public:
  FredCrossSectionalFetcher() : m_options(MakeDefaultOptions()) {
    InitializeClient();
  }

  explicit FredCrossSectionalFetcher(fred::Options options)
      : m_options(std::move(options)) {
    InitializeClient();
  }

  ~FredCrossSectionalFetcher() override = default;

  // Synchronous fetch
  FetchResult Fetch(CrossSectionalDataCategory category,
                   const epoch_frame::Date &fromDate,
                   const epoch_frame::Date &toDate) const override {
    SPDLOG_DEBUG("FredCrossSectionalFetcher::Fetch - category: {}, from: {}, to: {}",
                 CrossSectionalDataCategoryWrapper::ToString(category),
                 fromDate.repr(), toDate.repr());

    // Get FRED series ID for this category
    std::string series_id;
    try {
      series_id = fred::getSeriesId(category);
    } catch (const std::exception &e) {
      return std::unexpected(std::string("Failed to get series ID: ") + e.what());
    }

    // Convert dates to FRED API format (YYYY-MM-DD)
    std::string from_str = fromDate.repr();
    std::string to_str = toDate.repr();

    SPDLOG_DEBUG("Fetching FRED series: {} from {} to {}", series_id, from_str, to_str);

    // Fetch using AlfredClient for point-in-time data
    auto result = m_alfred_client->getSeries(series_id, from_str, to_str);

    if (!result) {
      SPDLOG_ERROR("FRED API error for series {}: {}", series_id, result.error().message);
      return std::unexpected("FRED API error: " + result.error().message);
    }

    SPDLOG_DEBUG("Successfully fetched {} rows for series {}", result->num_rows(), series_id);
    return std::expected<epoch_frame::DataFrame, std::string>(std::move(*result));
  }

  // Async fetch for concurrent operations
  drogon::Task<FetchResult> FetchAsync(CrossSectionalDataCategory category,
                                      const epoch_frame::Date &fromDate,
                                      const epoch_frame::Date &toDate) const override {
    SPDLOG_DEBUG("FredCrossSectionalFetcher::FetchAsync - category: {}, from: {}, to: {}",
                 CrossSectionalDataCategoryWrapper::ToString(category),
                 fromDate.repr(), toDate.repr());

    // Get FRED series ID for this category
    std::string series_id;
    try {
      series_id = fred::getSeriesId(category);
    } catch (const std::exception &e) {
      co_return std::unexpected(std::string("Failed to get series ID: ") + e.what());
    }

    // Convert dates to FRED API format (YYYY-MM-DD)
    std::string from_str = fromDate.repr();
    std::string to_str = toDate.repr();

    SPDLOG_DEBUG("Fetching FRED series async: {} from {} to {}", series_id, from_str, to_str);

    // Fetch using AlfredClient for point-in-time data
    auto result = co_await m_alfred_client->getSeriesAsync(series_id, from_str, to_str);

    if (!result) {
      SPDLOG_ERROR("FRED API error for series {}: {}", series_id, result.error().message);
      co_return std::unexpected("FRED API error: " + result.error().message);
    }

    SPDLOG_DEBUG("Successfully fetched {} rows for series {}", result->num_rows(), series_id);
    co_return std::expected<epoch_frame::DataFrame, std::string>(std::move(*result));
  }

private:
  void InitializeClient() {
    m_alfred_client = std::make_unique<fred::AlfredClient>(m_options);
    SPDLOG_INFO("FredCrossSectionalFetcher initialized with API key: {}",
                m_options.api_key.empty() ? "NOT SET" : "SET");
  }

  static fred::Options MakeDefaultOptions() {
    fred::Options opt;

    // Get API key from environment
    const char* api_key_env = std::getenv("FRED_API_KEY");
    if (api_key_env) {
      opt.api_key = api_key_env;
    } else {
      SPDLOG_WARN("FRED_API_KEY environment variable not set");
    }

    opt.use_drogon_main_loop = true;
    return opt;
  }

  fred::Options m_options;
  std::unique_ptr<fred::AlfredClient> m_alfred_client;
};

} // namespace data_sdk::dataloader
