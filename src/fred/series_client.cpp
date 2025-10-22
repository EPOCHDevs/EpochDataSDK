#include "epoch_data_sdk/fred/series_client.hpp"

#include <glaze/glaze.hpp>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "epoch_data_sdk/fred/base_client.hpp"
#include "epoch_data_sdk/fred/models.hpp"

namespace data_sdk::fred {

// Private implementation using PImpl pattern
class SeriesClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getSeries(const std::string &series_id,
            const std::string &from,
            const std::string &to,
            bool use_alfred) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("series_id", series_id);
    q.emplace_back("sort_order", "asc");

    if (use_alfred) {
      // ALFRED mode: Get data as published during the backtest period
      // realtime filters determine when the data was available
      q.emplace_back("realtime_start", from);
      q.emplace_back("realtime_end", to);
    } else {
      // Non-ALFRED mode: Get current/revised data for observation period
      q.emplace_back("observation_start", from);
      q.emplace_back("observation_end", to);
    }

    const std::string path = "/fred/series/observations";
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    SeriesObservationsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse FRED series observations JSON response", nullptr);
    }

    const auto N = parsed.observations.size();
    std::vector<std::string> dates;
    std::vector<double> values;
    std::vector<std::string> published_at_dates;

    dates.reserve(N);
    values.reserve(N);

    // Track if we have ALFRED data
    if (use_alfred)
      published_at_dates.reserve(N);

    for (const auto &obs : parsed.observations) {
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

      // ALFRED published date (realtime_start is when data became available)
      if (use_alfred) {
        published_at_dates.push_back(obs.realtime_start.value_or(""));
      }
    }

    // Build columns and index based on whether we have ALFRED data
    std::vector<std::string> columns;
    std::vector<arrow::ChunkedArrayPtr> arrays;
    epoch_frame::IndexPtr index;

    if (use_alfred) {
      // When we have ALFRED data, use published_at as index
      index = epoch_frame::factory::index::make_index(
          epoch_frame::factory::array::make_array(published_at_dates),  // Use ALFRED dates!
          epoch_frame::MonotonicDirection::NotMonotonic, "published_at");

      columns.push_back("observation_date");  // Which economic period this measures
      arrays.push_back(epoch_frame::factory::array::make_array(dates));
    } else {
      // When no ALFRED data, use observation date as index (backward compatibility)
      index = epoch_frame::factory::index::make_index(
          epoch_frame::factory::array::make_array(dates),
          epoch_frame::MonotonicDirection::NotMonotonic, "date");
    }

    columns.push_back("value");
    arrays.push_back(epoch_frame::factory::array::make_array(values));

    return epoch_frame::make_dataframe(index, arrays, columns);
  }
};

// Public API implementation
SeriesClient::SeriesClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

SeriesClient::~SeriesClient() = default;

Expected<epoch_frame::DataFrame>
SeriesClient::getSeries(const std::string &series_id,
                        const std::string &from,
                        const std::string &to,
                        bool use_alfred) const {
  return impl_->getSeries(series_id, from, to, use_alfred);
}

} // namespace data_sdk::fred
