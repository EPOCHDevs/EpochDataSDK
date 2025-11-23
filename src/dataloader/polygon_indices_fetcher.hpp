#pragma once

#include <memory>
#include <cstdlib>
#include <spdlog/spdlog.h>

#include <epoch_data_sdk/dataloader/indices_fetcher.hpp>
#include "../polygon/aggs_client.hpp"
#include "../polygon/options.hpp"

namespace data_sdk::dataloader {

// Polygon implementation of indices data fetcher
// Uses AggsClient to fetch index data with "I:" prefix (e.g., "I:SPX", "I:VIX")
class PolygonIndicesFetcher : public IIndicesFetcher {
public:
  PolygonIndicesFetcher() : m_options(MakeDefaultOptions()) {
    InitializeClient();
  }

  explicit PolygonIndicesFetcher(polygon::Options options)
      : m_options(std::move(options)) {
    InitializeClient();
  }

  ~PolygonIndicesFetcher() override = default;

  // Synchronous fetch
  FetchResult Fetch(const std::string& indexTicker,
                   const epoch_frame::Date &fromDate,
                   const epoch_frame::Date &toDate,
                   bool is_eod = true) const override {
    SPDLOG_DEBUG("PolygonIndicesFetcher::Fetch - ticker: {}, from: {}, to: {}, is_eod: {}",
                 indexTicker, fromDate.repr(), toDate.repr(), is_eod);

    // Add "I:" prefix to ticker for Polygon indices API
    std::string full_ticker = "I:" + indexTicker;

    // Convert dates to Polygon API format (YYYY-MM-DD)
    std::string from_str = fromDate.repr();
    std::string to_str = toDate.repr();

    SPDLOG_DEBUG("Fetching Polygon index: {} from {} to {} ({})",
                 full_ticker, from_str, to_str, is_eod ? "daily" : "minute");

    // Fetch using AggsClient for OHLCV data
    auto result = m_aggs_client->getAggregates(full_ticker, from_str, to_str, is_eod);

    if (!result) {
      SPDLOG_ERROR("Polygon API error for index {}: {}", full_ticker, result.error().message);
      return std::unexpected("Polygon API error: " + result.error().message);
    }

    SPDLOG_DEBUG("Successfully fetched {} rows for index {}", result->num_rows(), full_ticker);
    return std::expected<epoch_frame::DataFrame, std::string>(std::move(*result));
  }

  // Async fetch for concurrent operations
  drogon::Task<FetchResult> FetchAsync(const std::string& indexTicker,
                                      const epoch_frame::Date &fromDate,
                                      const epoch_frame::Date &toDate,
                                      bool is_eod = true) const override {
    SPDLOG_DEBUG("PolygonIndicesFetcher::FetchAsync - ticker: {}, from: {}, to: {}, is_eod: {}",
                 indexTicker, fromDate.repr(), toDate.repr(), is_eod);

    // Add "I:" prefix to ticker for Polygon indices API
    std::string full_ticker = "I:" + indexTicker;

    // Convert dates to Polygon API format (YYYY-MM-DD)
    std::string from_str = fromDate.repr();
    std::string to_str = toDate.repr();

    SPDLOG_DEBUG("Fetching Polygon index async: {} from {} to {} ({})",
                 full_ticker, from_str, to_str, is_eod ? "daily" : "minute");

    // Fetch using AggsClient for OHLCV data
    auto result = co_await m_aggs_client->getAggregatesAsync(full_ticker, from_str, to_str, is_eod);

    if (!result) {
      SPDLOG_ERROR("Polygon API error for index {}: {}", full_ticker, result.error().message);
      co_return std::unexpected("Polygon API error: " + result.error().message);
    }

    SPDLOG_DEBUG("Successfully fetched {} rows for index {}", result->num_rows(), full_ticker);
    co_return std::expected<epoch_frame::DataFrame, std::string>(std::move(*result));
  }

private:
  void InitializeClient() {
    m_aggs_client = std::make_unique<polygon::AggsClient>(m_options);
    SPDLOG_INFO("PolygonIndicesFetcher initialized with API key: {}",
                m_options.api_key.empty() ? "NOT SET" : "SET");
  }

  static polygon::Options MakeDefaultOptions() {
    polygon::Options opt;

    // Get API key from environment
    const char* api_key_env = std::getenv("POLYGON_API_KEY");
    if (api_key_env) {
      opt.api_key = api_key_env;
    } else {
      SPDLOG_WARN("POLYGON_API_KEY environment variable not set");
    }

    opt.use_drogon_main_loop = true;
    return opt;
  }

  polygon::Options m_options;
  std::unique_ptr<polygon::AggsClient> m_aggs_client;
};

} // namespace data_sdk::dataloader
