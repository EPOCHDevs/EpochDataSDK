#include "fred/series_client.hpp"

#include <glaze/glaze.hpp>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "fred/base_client.hpp"
#include "fred/models.hpp"

namespace data_sdk::fred {

// Private implementation using PImpl pattern
class SeriesClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

private:
  // Helper to process observations and populate vectors
  static void processObservations(
      const std::vector<Observation>& observations,
      std::vector<std::string>& dates,
      std::vector<double>& values,
      std::vector<std::string>& published_at_dates,
      bool use_alfred) {
    for (const auto &obs : observations) {
      dates.push_back(obs.date.value_or(""));

      // Parse value string to double
      double val = std::numeric_limits<double>::quiet_NaN();
      if (obs.value.has_value()) {
        const auto& val_str = *obs.value;
        if (val_str != "." && !val_str.empty()) {
          try {
            val = std::stod(val_str);
          } catch (const std::exception& e) {
            SPDLOG_DEBUG("Cannot parse FRED value '{}' for observation {}: {} - using NaN",
                         val_str, obs.date.value_or("unknown"), e.what());
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
  }

public:

  Expected<epoch_frame::DataFrame>
  getSeries(const std::string &series_id,
            const std::string &from,
            const std::string &to,
            bool use_alfred) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("series_id", series_id);
    q.emplace_back("sort_order", "asc");
    q.emplace_back("limit", "100000");  // FRED default/max limit per request

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

    std::vector<std::string> dates;
    std::vector<double> values;
    std::vector<std::string> published_at_dates;

    // Reserve space for initial batch
    const auto initial_size = parsed.observations.size();
    dates.reserve(initial_size);
    values.reserve(initial_size);
    if (use_alfred)
      published_at_dates.reserve(initial_size);

    // Process initial batch
    processObservations(parsed.observations, dates, values, published_at_dates, use_alfred);

    // Follow pagination if more data is available
    int page_count = 1;
    int current_offset = parsed.offset.value_or(0);
    int limit = parsed.limit.value_or(100000);
    int total_count = parsed.count.value_or(0);

    while (current_offset + limit < total_count) {
      current_offset += limit;
      page_count++;

      SPDLOG_DEBUG("FRED pagination: fetching page {} with offset {}", page_count, current_offset);

      // Create query with new offset
      std::vector<std::pair<std::string, std::string>> page_q = q;
      // Find and update offset parameter, or add it if not present
      bool offset_found = false;
      for (auto& [key, val] : page_q) {
        if (key == "offset") {
          val = std::to_string(current_offset);
          offset_found = true;
          break;
        }
      }
      if (!offset_found) {
        page_q.emplace_back("offset", std::to_string(current_offset));
      }

      auto page_body_res = httpGet(path, page_q);
      if (!page_body_res) {
        SPDLOG_WARN("FRED pagination failed at page {}: {}", page_count, page_body_res.error().message);
        break;
      }

      SeriesObservationsResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*page_body_res)); ec) {
        SPDLOG_WARN("FRED pagination parse failed at page {}", page_count);
        break;
      }

      processObservations(page.observations, dates, values, published_at_dates, use_alfred);
    }

    if (page_count > 1) {
      SPDLOG_INFO("FRED series: fetched {} pages for series_id={} use_alfred={} total_obs={}",
                  page_count, series_id, use_alfred, dates.size());
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

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getSeriesAsync(std::string series_id,
                 std::string from,
                 std::string to,
                 bool use_alfred) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("series_id", series_id);
    q.emplace_back("sort_order", "asc");
    q.emplace_back("limit", "100000");  // FRED default/max limit per request

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
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    SeriesObservationsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse FRED series observations JSON response", nullptr);
    }

    std::vector<std::string> dates;
    std::vector<double> values;
    std::vector<std::string> published_at_dates;

    // Reserve space for initial batch
    const auto initial_size = parsed.observations.size();
    dates.reserve(initial_size);
    values.reserve(initial_size);
    if (use_alfred)
      published_at_dates.reserve(initial_size);

    // Process initial batch
    processObservations(parsed.observations, dates, values, published_at_dates, use_alfred);

    // Follow pagination if more data is available (async)
    int page_count = 1;
    int current_offset = parsed.offset.value_or(0);
    int limit = parsed.limit.value_or(100000);
    int total_count = parsed.count.value_or(0);

    while (current_offset + limit < total_count) {
      current_offset += limit;
      page_count++;

      SPDLOG_DEBUG("FRED pagination (async): fetching page {} with offset {}", page_count, current_offset);

      // Create query with new offset
      std::vector<std::pair<std::string, std::string>> page_q = q;
      // Find and update offset parameter, or add it if not present
      bool offset_found = false;
      for (auto& [key, val] : page_q) {
        if (key == "offset") {
          val = std::to_string(current_offset);
          offset_found = true;
          break;
        }
      }
      if (!offset_found) {
        page_q.emplace_back("offset", std::to_string(current_offset));
      }

      auto page_body_res = co_await httpAsyncGet(path, page_q);
      if (!page_body_res) {
        SPDLOG_WARN("FRED pagination (async) failed at page {}: {}", page_count, page_body_res.error().message);
        break;
      }

      SeriesObservationsResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*page_body_res)); ec) {
        SPDLOG_WARN("FRED pagination (async) parse failed at page {}", page_count);
        break;
      }

      processObservations(page.observations, dates, values, published_at_dates, use_alfred);
    }

    if (page_count > 1) {
      SPDLOG_INFO("FRED series (async): fetched {} pages for series_id={} use_alfred={} total_obs={}",
                  page_count, series_id, use_alfred, dates.size());
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

    co_return epoch_frame::make_dataframe(index, arrays, columns);
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

drogon::Task<Expected<epoch_frame::DataFrame>>
SeriesClient::getSeriesAsync(std::string series_id,
                             std::string from,
                             std::string to,
                             bool use_alfred) const {
  return impl_->getSeriesAsync(std::move(series_id), std::move(from),
                               std::move(to), use_alfred);
}

data_sdk::DataFrameMetadata SeriesClient::getMetadata() {
  return data_sdk::DataFrameMetadata{
      .data_type = "fred_series",
      .description = "Retrieve economic data series observations from the Federal Reserve Economic Data (FRED) database. Supports both current/revised data and ALFRED (Archival FRED) point-in-time data for accurate backtesting. ALFRED mode provides data exactly as it was published during historical periods, capturing revisions and ensuring realistic backtests that reflect what information was actually available at each point in time. Without ALFRED, you get the latest revised data, suitable for current analysis but not for historical simulations. Use Cases: Economic indicator analysis, macroeconomic backtesting, policy impact studies, recession prediction models, interest rate forecasting, and inflation tracking with revision-aware historical accuracy.",
      .asset_class = std::nullopt,  // Economic data doesn't fit into asset classes
      .index_normalized = true,
      .category_prefix = "",  // Empty prefix for economic timeseries data
      .columns = {
          {.id = "observation_date",
           .name = "Observation Date",
           .description = "The date of the economic period this observation measures (YYYY-MM-DD format). When using ALFRED mode, this represents the economic period being measured, while the index (published_at) shows when this data became available. Only present when use_alfred=true.",
           .type = data_sdk::ArrowType::STRING,
           .nullable = false},
          {.id = "value",
           .name = "Value",
           .description = "The numeric value of the economic indicator for this observation. May be null (NaN) if data was not available or reported as '.' by FRED. Units and frequency vary by series (e.g., percent, index value, thousands of persons, billions of dollars).",
           .type = data_sdk::ArrowType::FLOAT64,
           .nullable = true},
      }};
}

} // namespace data_sdk::fred
