#pragma once
#include <epoch_data_sdk/common/env_loader.hpp>
#include <epoch_data_sdk/dataloader/fetcher.hpp>
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
#include <epoch_frame/common.h>
#include <spdlog/spdlog.h>

namespace data_sdk::dataloader {

/**
 * Multi-category Polygon data fetcher
 * Routes fetch requests to specialized Polygon SDK clients based on DataCategory
 */
class PolygonDataFetcher : public IDataFetcher {
public:
  PolygonDataFetcher() : m_options(MakeDefaultOptions()) {
    InitializeClients();
  }

  explicit PolygonDataFetcher(data_sdk::polygon::Options options)
      : m_options(std::move(options)) {
    InitializeClients();
  }

  std::expected<epoch_frame::DataFrame, std::string>
  Fetch(const asset::Asset &asset, DataCategory category,
        const epoch_frame::Date &fromDate,
        const epoch_frame::Date &toDate) const override {
    // Sync version calls async and waits - eliminates code duplication
    return drogon::sync_wait(FetchAsync(asset, category, fromDate, toDate));
  }

  // Async fetch for concurrent operations
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  FetchAsync(const asset::Asset &asset, DataCategory category,
             const epoch_frame::Date &fromDate,
             const epoch_frame::Date &toDate) const override {

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
        auto result = m_financials_client->getBalanceSheets(
            mapped, from_str, to_str);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::CashFlowStatements: {
        auto result = m_financials_client->getCashFlowStatements(
            mapped, from_str, to_str);
        if (!result) {
          co_return std::unexpected(result.error().message);
        }
        co_return *result;
      }

      case DataCategory::IncomeStatements: {
        auto result = m_financials_client->getIncomeStatements(
            mapped, from_str, to_str);
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

      default:
        co_return std::unexpected(
            "Unsupported DataCategory: " +
            std::string(epoch_core::DataCategoryWrapper::ToString(category)));
    }
  }

private:
  static data_sdk::polygon::Options MakeDefaultOptions() {
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

  void InitializeClients() {
    using namespace data_sdk::polygon;
    m_aggs_client = ClientFactory::createAggsClient(m_options);
    m_news_client = ClientFactory::createNewsClient(m_options);
    m_dividends_client = ClientFactory::createDividendsClient(m_options);
    m_splits_client = ClientFactory::createSplitsClient(m_options);
    m_ticker_events_client = ClientFactory::createTickerEventsClient(m_options);
    m_financials_client = ClientFactory::createFinancialsClient(m_options);
    m_ratios_client = ClientFactory::createRatiosClient(m_options);
    m_short_interest_client = ClientFactory::createShortInterestClient(m_options);
    m_short_volume_client = ClientFactory::createShortVolumeClient(m_options);
  }

  data_sdk::polygon::Options m_options;
  std::unique_ptr<data_sdk::polygon::AggsClient> m_aggs_client;
  std::unique_ptr<data_sdk::polygon::NewsClient> m_news_client;
  std::unique_ptr<data_sdk::polygon::DividendsClient> m_dividends_client;
  std::unique_ptr<data_sdk::polygon::SplitsClient> m_splits_client;
  std::unique_ptr<data_sdk::polygon::TickerEventsClient> m_ticker_events_client;
  std::unique_ptr<data_sdk::polygon::FinancialsClient> m_financials_client;
  std::unique_ptr<data_sdk::polygon::RatiosClient> m_ratios_client;
  std::unique_ptr<data_sdk::polygon::ShortInterestClient> m_short_interest_client;
  std::unique_ptr<data_sdk::polygon::ShortVolumeClient> m_short_volume_client;
};

} // namespace data_sdk::dataloader
