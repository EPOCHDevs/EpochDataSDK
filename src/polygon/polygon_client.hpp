#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <drogon/drogon.h>
#include <expected>
#include <trantor/net/EventLoopThread.h>

#include <epoch_frame/dataframe.h>

#include <epoch_data_sdk/polygon/error.hpp>
#include <epoch_data_sdk/polygon/options.hpp>

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

class PolygonClient final {
public:
  explicit PolygonClient(Options options);
  ~PolygonClient();

  Expected<epoch_frame::DataFrame>
  getAggregates(const std::string &ticker, const std::string &from_date,
                const std::string &to_date, bool is_eod,
                std::optional<bool> adjusted = true) const;

  // Convenience wrappers using from/to date → timestamp.gte/lte
  Expected<epoch_frame::DataFrame>
  getQuotes(const std::string &ticker, const std::string &from_date,
            const std::string &to_date, std::optional<int> limit = std::nullopt,
            std::optional<std::string> order = std::string("asc"),
            std::optional<std::string> sort = std::string("timestamp")) const;

  Expected<epoch_frame::DataFrame>
  getTrades(const std::string &ticker, const std::string &from_date,
            const std::string &to_date, std::optional<int> limit = std::nullopt,
            std::optional<std::string> order = std::string("asc"),
            std::optional<std::string> sort = std::string("timestamp")) const;

  Expected<epoch_frame::DataFrame>
  getBalanceSheets(std::optional<std::string> cik = std::nullopt,
                   std::optional<std::string> period_end = std::nullopt,
                   std::optional<std::string> tickers = std::nullopt,
                   std::optional<int> fiscal_year = std::nullopt,
                   std::optional<int> fiscal_quarter = std::nullopt,
                   std::optional<std::string> timeframe = std::nullopt,
                   std::optional<int> limit = std::nullopt,
                   std::optional<std::string> sort = std::string("period_end.desc")) const;

  Expected<epoch_frame::DataFrame>
  getCashFlowStatements(std::optional<std::string> cik = std::nullopt,
                        std::optional<std::string> period_end = std::nullopt,
                        std::optional<std::string> tickers = std::nullopt,
                        std::optional<int> fiscal_year = std::nullopt,
                        std::optional<int> fiscal_quarter = std::nullopt,
                        std::optional<std::string> timeframe = std::nullopt,
                        std::optional<int> limit = std::nullopt,
                        std::optional<std::string> sort = std::string("period_end.desc")) const;

  Expected<epoch_frame::DataFrame>
  getIncomeStatements(std::optional<std::string> cik = std::nullopt,
                      std::optional<std::string> period_end = std::nullopt,
                      std::optional<std::string> tickers = std::nullopt,
                      std::optional<int> fiscal_year = std::nullopt,
                      std::optional<int> fiscal_quarter = std::nullopt,
                      std::optional<std::string> timeframe = std::nullopt,
                      std::optional<int> limit = std::nullopt,
                      std::optional<std::string> sort = std::string("period_end.desc")) const;

  Expected<epoch_frame::DataFrame>
  getFinancialRatios(std::optional<std::string> ticker = std::nullopt,
                     std::optional<std::string> cik = std::nullopt,
                     std::optional<double> price = std::nullopt,
                     std::optional<double> average_volume = std::nullopt,
                     std::optional<double> market_cap = std::nullopt,
                     std::optional<double> earnings_per_share = std::nullopt,
                     std::optional<double> price_to_earnings = std::nullopt,
                     std::optional<double> price_to_book = std::nullopt,
                     std::optional<double> price_to_sales = std::nullopt,
                     std::optional<int> limit = std::nullopt,
                     std::optional<std::string> sort = std::nullopt) const;

private:
  drogon::Task<Expected<std::string>> httpAsyncGet(
      const std::string &path,
      const std::vector<std::pair<std::string, std::string>> &query) const;

  Expected<std::string>
  httpGet(const std::string &path,
          const std::vector<std::pair<std::string, std::string>> &query) const {
    return drogon::sync_wait(httpAsyncGet(path, query));
  }

  Expected<std::string> httpGetWithRetry(
      const std::string &path,
      const std::vector<std::pair<std::string, std::string>> &query,
      int max_retries) const;

  static std::string buildQueryString(
      const std::vector<std::pair<std::string, std::string>> &query);

  static std::optional<int> parseIntHeader(const drogon::HttpResponsePtr &resp,
                                           const std::string &key);

private:
  // v3 REST endpoints (private primitives)
  Expected<epoch_frame::DataFrame>
  getV3Quotes(const std::string &ticker,
              std::optional<int> limit = std::nullopt,
              std::optional<std::string> order = std::string("asc"),
              std::optional<std::string> sort = std::string("timestamp"),
              std::optional<std::string> timestamp_gte = std::nullopt,
              std::optional<std::string> timestamp_gt = std::nullopt,
              std::optional<std::string> timestamp_lte = std::nullopt,
              std::optional<std::string> timestamp_lt = std::nullopt) const;

  Expected<epoch_frame::DataFrame>
  getV3Trades(const std::string &ticker,
              std::optional<int> limit = std::nullopt,
              std::optional<std::string> order = std::string("asc"),
              std::optional<std::string> sort = std::string("timestamp"),
              std::optional<std::string> timestamp_gte = std::nullopt,
              std::optional<std::string> timestamp_gt = std::nullopt,
              std::optional<std::string> timestamp_lte = std::nullopt,
              std::optional<std::string> timestamp_lt = std::nullopt) const;

  Options options_;
  // Dedicated event loop and HTTP client bound to it
  std::unique_ptr<trantor::EventLoopThread> loopThread_;
  drogon::HttpClientPtr httpClient_;
};

} // namespace data_sdk::polygon
