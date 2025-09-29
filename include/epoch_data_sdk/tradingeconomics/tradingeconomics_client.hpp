#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <drogon/drogon.h>
#include <trantor/net/EventLoopThread.h>
#include <expected>

#include "epoch_data_sdk/tradingeconomics/error.hpp"
#include "epoch_data_sdk/tradingeconomics/models.hpp"
#include "epoch_data_sdk/tradingeconomics/options.hpp"

namespace data_sdk::tradingeconomics {

template <typename T> using Expected = std::expected<T, HttpError>;

class TradingEconomicsClient final {
public:
  explicit TradingEconomicsClient(Options options);
  ~TradingEconomicsClient();

  // GET /historical/series/{symbol}
  Expected<std::vector<HistoricalSeriesResponse>> getHistoricalSeries(
      const std::string &symbol,
      std::optional<std::string> start_date = std::nullopt,
      std::optional<std::string> end_date = std::nullopt,
      std::optional<std::string> output_type = std::nullopt) const;

  // GET /calendar
  Expected<CalendarResponse>
  getCalendar(std::optional<std::string> country = std::nullopt,
              std::optional<std::string> category = std::nullopt,
              std::optional<std::string> start_date = std::nullopt,
              std::optional<std::string> end_date = std::nullopt) const;

private:
  Expected<std::string>
  httpGet(const std::string &path,
          const std::vector<std::pair<std::string, std::string>> &query) const;

  static std::string buildQueryString(
      const std::vector<std::pair<std::string, std::string>> &query);

private:
  Options options_;
  std::unique_ptr<trantor::EventLoopThread> loopThread_;
  drogon::HttpClientPtr httpClient_;
};

} // namespace data_sdk::tradingeconomics
