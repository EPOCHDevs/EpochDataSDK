#include "epoch_data_sdk/trading_economics/historical_indicators_client.hpp"

#include "base_client.hpp"

namespace data_sdk::trading_economics {

// Private implementation using direct composition
class HistoricalIndicatorsClient::Impl {
public:
  explicit Impl(Options options) : base_client_(std::move(options)) {}

  BaseClient base_client_;
};

// Constructor
HistoricalIndicatorsClient::HistoricalIndicatorsClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

// Destructor
HistoricalIndicatorsClient::~HistoricalIndicatorsClient() = default;

// Get historical data for specific country and indicator
// API: GET /historical/country/{country}/indicator/{indicator}
Expected<epoch_frame::DataFrame>
HistoricalIndicatorsClient::getHistoricalData(
    const std::string& country, const std::string& indicator,
    std::optional<std::string> from_date,
    std::optional<std::string> to_date) const {

  std::string path = "/historical/country/" + country + "/indicator/" + indicator;

  std::map<std::string, std::string> params;
  if (from_date) {
    params["d1"] = *from_date;
  }
  if (to_date) {
    params["d2"] = *to_date;
  }

  return impl_->base_client_.httpGetDataFrame(path, params);
}

// Get historical credit ratings for countries
// API: GET /ratings/historical/country/{country}
Expected<epoch_frame::DataFrame>
HistoricalIndicatorsClient::getHistoricalRatings(
    const std::string& country, std::optional<std::string> rating,
    std::optional<std::string> from_date,
    std::optional<std::string> to_date) const {

  std::string path = "/ratings/historical/country/" + country;

  std::map<std::string, std::string> params;
  if (rating) {
    params["rating"] = *rating;
  }
  if (from_date) {
    params["d1"] = *from_date;
  }
  if (to_date) {
    params["d2"] = *to_date;
  }

  return impl_->base_client_.httpGetDataFrame(path, params);
}

// Get historical data by Trading Economics ticker symbol
// API: GET /historical/ticker/{ticker}
Expected<epoch_frame::DataFrame>
HistoricalIndicatorsClient::getHistoricalByTicker(
    const std::string& ticker,
    std::optional<std::string> start_date) const {

  std::string path = "/historical/ticker/" + ticker;

  std::map<std::string, std::string> params;
  if (start_date) {
    params["d1"] = *start_date;
  }

  return impl_->base_client_.httpGetDataFrame(path, params);
}

// Get latest historical data updates across all indicators
// API: GET /historical/updates
Expected<epoch_frame::DataFrame>
HistoricalIndicatorsClient::getHistoricalLatest() const {
  std::string path = "/historical/updates";
  std::map<std::string, std::string> params;

  return impl_->base_client_.httpGetDataFrame(path, params);
}

// Get historical data updates/changes
// API: GET /historical/updates
Expected<epoch_frame::DataFrame>
HistoricalIndicatorsClient::getHistoricalUpdates() const {
  std::string path = "/historical/updates";
  std::map<std::string, std::string> params;

  return impl_->base_client_.httpGetDataFrame(path, params);
}

} // namespace data_sdk::trading_economics
