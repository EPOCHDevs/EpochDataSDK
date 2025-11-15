#include "fred/alfred_client.hpp"

#include <limits>
#include <map>
#include <numeric>
#include <vector>

#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>

#include "fred/series_impl.hpp"

namespace data_sdk::fred {

// PImpl implementation
class AlfredClient::Impl : public SeriesImpl {
public:
  explicit Impl(Options options) : SeriesImpl(std::move(options)) {}
};

AlfredClient::AlfredClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

AlfredClient::~AlfredClient() = default;
AlfredClient::AlfredClient(AlfredClient&&) noexcept = default;
AlfredClient& AlfredClient::operator=(AlfredClient&&) noexcept = default;

Expected<epoch_frame::DataFrame>
AlfredClient::getSeries(const std::string &series_id,
                        const std::string &from,
                        const std::string &to) const {

  // FIXED: Set both observation and realtime dates to get all revisions
  // observation dates: which economic periods we want
  // realtime dates: get all revisions from 'from' through today
  auto response = impl_->fetchSeries(series_id, from, to, from, "9999-12-31");
  if (!response)
    return std::unexpected(response.error());

  // Process observations and calculate revision numbers
  std::vector<std::string> published_at_dates;
  std::vector<std::string> observation_dates;
  std::vector<double> values;
  std::vector<int64_t> revisions;

  published_at_dates.reserve(response->observations.size());
  observation_dates.reserve(response->observations.size());
  values.reserve(response->observations.size());
  revisions.reserve(response->observations.size());

  // Map to track revision count per observation_date
  std::map<std::string, int> revision_counter;

  for (const auto &obs : response->observations) {
    std::string obs_date = obs.date.value_or("");
    std::string pub_date = obs.realtime_start.value_or("");

    // Increment revision number for this observation_date
    revision_counter[obs_date]++;
    int revision_num = revision_counter[obs_date];

    published_at_dates.push_back(pub_date);
    observation_dates.push_back(obs_date);
    revisions.push_back(revision_num);

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

  // Create DataFrame with published_at as index
  auto index = epoch_frame::factory::index::make_index(
      epoch_frame::factory::array::make_array(published_at_dates),
      epoch_frame::MonotonicDirection::Increasing, "published_at");

  std::vector<std::string> columns = {"observation_date", "value", "revision"};
  std::vector<arrow::ChunkedArrayPtr> arrays = {
      epoch_frame::factory::array::make_array(observation_dates),
      epoch_frame::factory::array::make_array(values),
      epoch_frame::factory::array::make_array(revisions)
  };

  return epoch_frame::make_dataframe(index, arrays, columns);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
AlfredClient::getSeriesAsync(std::string series_id,
                             std::string from,
                             std::string to) const {

  // FIXED: Set both observation and realtime dates to get all revisions
  auto response = co_await impl_->fetchSeriesAsync(series_id, from, to, from, "9999-12-31");
  if (!response)
    co_return std::unexpected(response.error());

  // Process observations and calculate revision numbers
  std::vector<std::string> published_at_dates;
  std::vector<std::string> observation_dates;
  std::vector<double> values;
  std::vector<int64_t> revisions;

  published_at_dates.reserve(response->observations.size());
  observation_dates.reserve(response->observations.size());
  values.reserve(response->observations.size());
  revisions.reserve(response->observations.size());

  // Map to track revision count per observation_date
  std::map<std::string, int> revision_counter;

  for (const auto &obs : response->observations) {
    std::string obs_date = obs.date.value_or("");
    std::string pub_date = obs.realtime_start.value_or("");

    // Increment revision number for this observation_date
    revision_counter[obs_date]++;
    int revision_num = revision_counter[obs_date];

    published_at_dates.push_back(pub_date);
    observation_dates.push_back(obs_date);
    revisions.push_back(revision_num);

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

  // Create DataFrame with published_at as index
  auto index = epoch_frame::factory::index::make_index(
      epoch_frame::factory::array::make_array(published_at_dates),
      epoch_frame::MonotonicDirection::Increasing, "published_at");

  std::vector<std::string> columns = {"observation_date", "value", "revision"};
  std::vector<arrow::ChunkedArrayPtr> arrays = {
      epoch_frame::factory::array::make_array(observation_dates),
      epoch_frame::factory::array::make_array(values),
      epoch_frame::factory::array::make_array(revisions)
  };

  co_return epoch_frame::make_dataframe(index, arrays, columns);
}

data_sdk::DataFrameMetadata AlfredClient::getMetadata() {
  return data_sdk::DataFrameMetadata{
      .data_type = "alfred_series",
      .description = "Archival Federal Reserve Economic Data (ALFRED) preserves 'what information was known on some past date in history.' This client retrieves complete revision history (vintages) of economic data series, indexed by publication date (real-time period). ALFRED archives when values were originally released and subsequently revised, enabling analysis of economic understanding as it evolved. The DataFrame contains multiple rows per observation date—each row represents a data vintage from a specific publication date. Index: published_at (real-time period when this vintage became available). Columns: observation_date (economic measurement period), value (data as published on that real-time date), revision (sequential revision number per observation). Example: January 1990 unemployment was initially reported as 5.3% in February 1990, then revised to 5.4% in March 1996—ALFRED preserves both vintages. Critical for backtesting: filter by published_at <= your_backtest_date to reconstruct what economists actually knew at that historical moment, avoiding lookahead bias. FRED shows what we know now; ALFRED shows what we knew then.",
      .asset_class = std::nullopt,  // Economic data doesn't fit into asset classes
      .index_normalized = true,
      .columns = {
          {.id = "observation_date",
           .name = "Observation Date",
           .description = "The date of the economic observation period being measured (YYYY-MM-DD). This represents WHEN the economic activity occurred (e.g., January 2023 for monthly unemployment rate). Multiple DataFrame rows share the same observation_date when data for that period was revised over time—each revision appears as a separate row with a different published_at index. Observation dates define the economic timeline (data collection period), while the published_at index defines the information timeline (when values became known).",
           .type = data_sdk::ArrowType::STRING,
           .nullable = false},
          {.id = "value",
           .name = "Value",
           .description = "The economic data series value as it was published/available during the real-time period specified by the published_at index. Units and frequency are series-specific (examples: percent, index value, billions of dollars, thousands of persons). Null (NaN) indicates data was unavailable or reported as '.' by the source agency for that vintage. This is the actual figure economists, policymakers, and traders saw at the published_at date—it may differ from later revisions as source agencies refined estimates. Each value represents a historical snapshot: what FRED reported at that moment in time before subsequent updates.",
           .type = data_sdk::ArrowType::FLOAT64,
           .nullable = true},
          {.id = "revision",
           .name = "Revision Number",
           .description = "Sequential revision counter for each observation_date, automatically calculated during data retrieval. First published value = 1 (initial release), second value = 2 (first revision), third = 3 (second revision), etc. Revisions are numbered chronologically by published_at date within each observation_date group. Use revision=1 to isolate initial releases (what was first reported). Use max(revision) per observation_date to identify the most recent vintage in the dataset. Revision numbers enable analysis of data quality evolution: how many times was an observation revised? How long until estimates stabilized?",
           .type = data_sdk::ArrowType::INT64,
           .nullable = false},
      }};
}

} // namespace data_sdk::fred
