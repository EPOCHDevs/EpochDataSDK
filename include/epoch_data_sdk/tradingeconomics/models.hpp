#pragma once

#include <optional>
#include <string>
#include <vector>

#include <glaze/glaze.hpp>

namespace data_sdk::tradingeconomics {

// Example response for TE historical series endpoint
// e.g., /historical/series/{symbol}?c={key}&format=json
struct HistoricalPoint {
  std::string Date;
  double Value{};
};

struct HistoricalSeriesResponse {
  std::string Symbol;
  std::string Category;
  std::string Country;
  std::vector<HistoricalPoint> HistoricalData;
};

// Example response for TE calendar indicators endpoint
struct IndicatorEvent {
  std::string Country;
  std::string Category;
  std::string Date;
  std::optional<double> Actual;
  std::optional<double> Previous;
  std::optional<double> Forecast;
  std::optional<std::string> Unit;
};

using CalendarResponse = std::vector<IndicatorEvent>;

} // namespace data_sdk::tradingeconomics
