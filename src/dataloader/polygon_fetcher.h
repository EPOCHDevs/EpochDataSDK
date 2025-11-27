#pragma once
#include <epoch_data_sdk/common/env_loader.hpp>
#include <epoch_data_sdk/dataloader/fetcher.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>
#include "polygon/options.hpp"
#include "polygon/client_factory.hpp"
#include "polygon/aggs_client.hpp"
#include "polygon/news_client.hpp"
#include "polygon/dividends_client.hpp"
#include "polygon/splits_client.hpp"
#include "polygon/financials_client.hpp"
#include "polygon/ticker_events_client.hpp"
#include "polygon/ratios_client.hpp"
#include "polygon/short_interest_client.hpp"
#include "polygon/short_volume_client.hpp"
#include "fred/alfred_client.hpp"
#include "fred/options.hpp"
#include <epoch_frame/common.h>
#include <spdlog/spdlog.h>

namespace data_sdk::dataloader {

/**
 * Multi-category Polygon data fetcher
 * Routes fetch requests to specialized Polygon SDK clients based on DataCategory
 * Supports both asset-based fetching and asset-less fetching (for indices/economic indicators)
 */
class PolygonDataFetcher : public IDataFetcher {
public:
  PolygonDataFetcher() : m_polygon_options(MakeDefaultPolygonOptions()),
                         m_fred_options(MakeDefaultFredOptions()) {
    InitializeClients();
  }

  explicit PolygonDataFetcher(data_sdk::polygon::Options polygon_options,
                              data_sdk::fred::Options fred_options = MakeDefaultFredOptions())
      : m_polygon_options(std::move(polygon_options)),
        m_fred_options(std::move(fred_options)) {
    InitializeClients();
  }

  // ============================================================
  // Asset-based fetching
  // ============================================================

  std::expected<epoch_frame::DataFrame, std::string>
  Fetch(const asset::Asset &asset, DataCategory category,
        const epoch_frame::Date &fromDate,
        const epoch_frame::Date &toDate,
        const FetchKwargs &kwargs = NoKwargs{}) const override {
    // Sync version calls async and waits - eliminates code duplication
    return drogon::sync_wait(FetchAsync(asset, category, fromDate, toDate, kwargs));
  }

  // Async fetch for concurrent operations
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  FetchAsync(const asset::Asset &asset, DataCategory category,
             const epoch_frame::Date &fromDate,
             const epoch_frame::Date &toDate,
             const FetchKwargs &kwargs = NoKwargs{}) const override {

    const auto mapped = asset.GetSpec().GetVendorSymbolForPolygon().value_or(
        asset.GetSymbolStr());

    const std::string from_str = fromDate.repr();
    const std::string to_str = toDate.repr();

    switch (category) {
      case DataCategory::MinuteBars: {
        auto result = co_await m_aggs_client->getAggregatesAsync(mapped, from_str, to_str, false);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::DailyBars: {
        auto result = co_await m_aggs_client->getAggregatesAsync(mapped, from_str, to_str, true);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::News: {
        auto result = m_news_client->getNews(mapped, from_str, to_str);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::Dividends: {
        auto result = m_dividends_client->getDividends(
            mapped, std::nullopt, from_str, to_str);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::Splits: {
        auto result = m_splits_client->getSplits(
            mapped, std::nullopt, from_str, to_str);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::ShortInterest: {
        auto result = m_short_interest_client->getShortInterest(
            mapped, from_str, to_str);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::ShortVolume: {
        auto result = m_short_volume_client->getShortVolume(
            mapped, from_str, to_str);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::TickerEvents: {
        // TickerEventsClient::getTickerEvents only takes ticker and optional types parameter
        // No date range parameters - returns all events for the ticker
        auto result = co_await m_ticker_events_client->getTickerEventsAsync(mapped);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::BalanceSheets: {
        // Extract timeframe from kwargs (default: Quarterly)
        auto fin_kwargs = getKwargsOr<DataCategory::BalanceSheets>(kwargs);
        auto result = m_financials_client->getBalanceSheets(
            mapped, from_str, to_str, std::nullopt, fin_kwargs.timeframe);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::CashFlowStatements: {
        auto fin_kwargs = getKwargsOr<DataCategory::CashFlowStatements>(kwargs);
        auto result = m_financials_client->getCashFlowStatements(
            mapped, from_str, to_str, std::nullopt, fin_kwargs.timeframe);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::IncomeStatements: {
        auto fin_kwargs = getKwargsOr<DataCategory::IncomeStatements>(kwargs);
        auto result = m_financials_client->getIncomeStatements(
            mapped, from_str, to_str, std::nullopt, fin_kwargs.timeframe);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::Ratios: {
        // RatiosClient::getRatios only takes ticker, limit, and sort parameters
        // No date range parameters - returns latest ratios for the ticker
        auto result = co_await m_ratios_client->getRatiosAsync(mapped);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::EconomicIndicator:
      case DataCategory::ReferenceAgg:
        // These categories require asset-less fetch - delegate to the overload
        co_return co_await FetchAsync(category, fromDate, toDate, kwargs);

      default:
        co_return std::unexpected(
            "Unsupported DataCategory: " +
            std::string(epoch_core::DataCategoryWrapper::ToString(category)));
    }
  }

  // ============================================================
  // Asset-less fetching (for cross-sectional data)
  // ============================================================

  FetchResult Fetch(DataCategory category,
                   const epoch_frame::Date &fromDate,
                   const epoch_frame::Date &toDate,
                   const FetchKwargs &kwargs) const override {
    return drogon::sync_wait(FetchAsync(category, fromDate, toDate, kwargs));
  }

  drogon::Task<FetchResult> FetchAsync(DataCategory category,
                                       const epoch_frame::Date &fromDate,
                                       const epoch_frame::Date &toDate,
                                       const FetchKwargs &kwargs) const override {
    const std::string from_str = fromDate.repr();
    const std::string to_str = toDate.repr();

    switch (category) {
      case DataCategory::EconomicIndicator: {
        // Extract indicator enum from kwargs
        auto econ_kwargs = getKwargsOr<DataCategory::EconomicIndicator>(kwargs);
        // Get series ID from enum mapping
        std::string series_id = econ_kwargs.getSeriesId();
        // AlfredClient handles ALFRED with fallback to FRED, chunking, pagination
        auto result = co_await m_alfred_client->getSeriesAsync(
            series_id, from_str, to_str);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::ReferenceAgg: {
        // Extract kwargs and use asset-less fetch
        auto ref_kwargs = getKwargsOr<DataCategory::ReferenceAgg>(kwargs);
        if (ref_kwargs.ticker.empty()) {
          co_return std::unexpected("ReferenceAgg requires ticker in kwargs");
        }
        // Add appropriate prefix based on asset class
        std::string full_ticker = ref_kwargs.getPolygonTicker();
        auto result = co_await m_aggs_client->getAggregatesAsync(
            full_ticker, from_str, to_str, ref_kwargs.is_eod);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      default:
        co_return std::unexpected(
            "Asset-less fetch not supported for category: " +
            std::string(epoch_core::DataCategoryWrapper::ToString(category)));
    }
  }

private:
  static data_sdk::polygon::Options MakeDefaultPolygonOptions() {
    data_sdk::polygon::Options opt;
    opt.api_key = ENV("POLYGON_API_KEY");
    if (opt.api_key.empty()) {
      SPDLOG_WARN("POLYGON_API_KEY not set in environment");
    }
    opt.use_drogon_main_loop = true;
    // Increase timeout to handle parallel request congestion
    // Polygon responds fast (~100ms) but parallel requests can cause queuing
    opt.request_timeout_sec = 30.0;  // 30 seconds for queuing delays
    opt.connect_timeout_sec = 15.0;   // Connection timeout

    // Rate limiting configuration
    opt.enable_rate_limiting = true;
    opt.max_requests_per_second = 100.0;  // Limit to 100 requests per second

    return opt;
  }

  static data_sdk::fred::Options MakeDefaultFredOptions() {
    data_sdk::fred::Options opt;
    opt.api_key = ENV("FRED_API_KEY");
    if (opt.api_key.empty()) {
      SPDLOG_WARN("FRED_API_KEY not set in environment");
    }
    return opt;
  }

  void InitializeClients() {
    using namespace data_sdk::polygon;
    m_aggs_client = ClientFactory::createAggsClient(m_polygon_options);
    m_news_client = ClientFactory::createNewsClient(m_polygon_options);
    m_dividends_client = ClientFactory::createDividendsClient(m_polygon_options);
    m_splits_client = ClientFactory::createSplitsClient(m_polygon_options);
    m_ticker_events_client = ClientFactory::createTickerEventsClient(m_polygon_options);
    m_financials_client = ClientFactory::createFinancialsClient(m_polygon_options);
    m_ratios_client = ClientFactory::createRatiosClient(m_polygon_options);
    m_short_interest_client = ClientFactory::createShortInterestClient(m_polygon_options);
    m_short_volume_client = ClientFactory::createShortVolumeClient(m_polygon_options);

    // ALFRED client for economic indicators (with fallback to FRED)
    m_alfred_client = std::make_unique<data_sdk::fred::AlfredClient>(m_fred_options);
  }

  data_sdk::polygon::Options m_polygon_options;
  data_sdk::fred::Options m_fred_options;
  std::unique_ptr<data_sdk::polygon::AggsClient> m_aggs_client;
  std::unique_ptr<data_sdk::polygon::NewsClient> m_news_client;
  std::unique_ptr<data_sdk::polygon::DividendsClient> m_dividends_client;
  std::unique_ptr<data_sdk::polygon::SplitsClient> m_splits_client;
  std::unique_ptr<data_sdk::polygon::TickerEventsClient> m_ticker_events_client;
  std::unique_ptr<data_sdk::polygon::FinancialsClient> m_financials_client;
  std::unique_ptr<data_sdk::polygon::RatiosClient> m_ratios_client;
  std::unique_ptr<data_sdk::polygon::ShortInterestClient> m_short_interest_client;
  std::unique_ptr<data_sdk::polygon::ShortVolumeClient> m_short_volume_client;
  std::unique_ptr<data_sdk::fred::AlfredClient> m_alfred_client;
};

} // namespace data_sdk::dataloader
