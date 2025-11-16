#include "fred/fred_client.hpp"

#include <limits>
#include <chrono>
#include <iomanip>
#include <sstream>

#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>

#include "fred/base_client.hpp"
#include "fred/series_impl.hpp"

namespace data_sdk::fred {

namespace {
// Get today's date in YYYY-MM-DD format for realtime queries
std::string getTodayDate() {
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
  localtime_r(&time_t_now, &tm_now);

  std::ostringstream oss;
  oss << std::put_time(&tm_now, "%Y-%m-%d");
  return oss.str();
}
} // namespace

// PImpl implementation
class FredClient::Impl : public SeriesImpl {
public:
  explicit Impl(Options options) : SeriesImpl(std::move(options)) {}
};

FredClient::FredClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

FredClient::~FredClient() = default;
FredClient::FredClient(FredClient&&) noexcept = default;
FredClient& FredClient::operator=(FredClient&&) noexcept = default;

Expected<epoch_frame::DataFrame>
FredClient::getSeries(const std::string &series_id,
                      const std::string &from,
                      const std::string &to) const {

  // Fetch data: observation period = [from, to], realtime = today's date (latest vintage only)
  // Setting both realtime_start and realtime_end to today gets only the most recent revision
  std::string today = getTodayDate();
  auto response = impl_->fetchSeries(series_id, from, to, today, today);
  if (!response)
    return std::unexpected(response.error());

  // Process observations into simple DataFrame
  std::vector<std::string> dates;
  std::vector<double> values;

  dates.reserve(response->observations.size());
  values.reserve(response->observations.size());

  for (const auto &obs : response->observations) {
    dates.push_back(obs.date.value_or(""));

    // Parse value string to double
    double val = std::numeric_limits<double>::quiet_NaN();
    if (obs.value.has_value()) {
      const auto& val_str = *obs.value;
      if (val_str != "." && !val_str.empty()) {
        try {
          val = std::stod(val_str);
        } catch (...) {
          // Keep as NaN if parsing fails
        }
      }
    }
    values.push_back(val);
  }

  // Create DataFrame with observation_date as UTC NANO datetime index
  auto date_timestamps = parseDateStringsToNanoseconds(dates);
  auto index = epoch_frame::factory::index::make_datetime_index(
      date_timestamps, "observation_date", "UTC");

  std::vector<std::string> columns = {"value"};
  std::vector<arrow::ChunkedArrayPtr> arrays = {
      epoch_frame::factory::array::make_array(values)
  };

  return epoch_frame::make_dataframe(index, arrays, columns);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
FredClient::getSeriesAsync(std::string series_id,
                           std::string from,
                           std::string to) const {

  // Fetch data: observation period = [from, to], realtime = today's date (latest vintage only)
  // Setting both realtime_start and realtime_end to today gets only the most recent revision
  std::string today = getTodayDate();
  auto response = co_await impl_->fetchSeriesAsync(series_id, from, to, today, today);
  if (!response)
    co_return std::unexpected(response.error());

  // Process observations into simple DataFrame
  std::vector<std::string> dates;
  std::vector<double> values;

  dates.reserve(response->observations.size());
  values.reserve(response->observations.size());

  for (const auto &obs : response->observations) {
    dates.push_back(obs.date.value_or(""));

    // Parse value string to double
    double val = std::numeric_limits<double>::quiet_NaN();
    if (obs.value.has_value()) {
      const auto& val_str = *obs.value;
      if (val_str != "." && !val_str.empty()) {
        try {
          val = std::stod(val_str);
        } catch (...) {
          // Keep as NaN if parsing fails
        }
      }
    }
    values.push_back(val);
  }

  // Create DataFrame with observation_date as UTC NANO datetime index
  auto date_timestamps = parseDateStringsToNanoseconds(dates);
  auto index = epoch_frame::factory::index::make_datetime_index(
      date_timestamps, "observation_date", "UTC");

  std::vector<std::string> columns = {"value"};
  std::vector<arrow::ChunkedArrayPtr> arrays = {
      epoch_frame::factory::array::make_array(values)
  };

  co_return epoch_frame::make_dataframe(index, arrays, columns);
}

data_sdk::DataFrameMetadata FredClient::getMetadata() {
  return data_sdk::DataFrameMetadata{
      .data_type = "fred_series",
      .description = "Federal Reserve Economic Data (FRED) provides 'the most accurate information about the past that is available today.' This client retrieves current, fully-revised economic time series data indexed by observation date. FRED automatically sets the real-time period to today's date, returning the latest published values for each observation period. Economic statistics are frequently revised as more complete information becomes available—FRED always provides the most recent revision. The DataFrame contains one row per observation date (e.g., one value per month for monthly data). Index: observation_date (the economic measurement period). Column: value (the latest revised data point). Ideal for current economic analysis, visualization, policy research, and applications that need today's best understanding of historical economic conditions. For backtesting or studying how data was known at specific historical moments (before later revisions), use AlfredClient to access vintage data and avoid lookahead bias.",
      .asset_class = std::nullopt,  // Economic data doesn't fit into asset classes
      .index_normalized = true,
      .columns = {
          {.id = "value",
           .name = "Value",
           .description = "The current/latest revised value of the economic data series as known today. Units and frequency are series-specific (examples: percent for unemployment rate, index value for CPI, billions of dollars for GDP, thousands of persons for payroll employment). Null (NaN) indicates data unavailable for that observation period. This value represents FRED's best current estimate and may differ from values originally published on the observation date—economic data undergoes revisions as source agencies refine estimates with more complete information. Real-time period is implicitly set to today, meaning you see the data as it exists in FRED's database now, incorporating all historical revisions. Index is observation_date as timestamp[ns, UTC] at midnight UTC.",
           .type = data_sdk::ArrowType::FLOAT64,
           .nullable = true},
      }};
}

} // namespace data_sdk::fred
