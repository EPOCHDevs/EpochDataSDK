#include "series_impl.hpp"

#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>

namespace data_sdk::fred {

Expected<SeriesObservationsResponse>
SeriesImpl::fetchSeries(const std::string &series_id,
                        const std::string &observation_start,
                        const std::string &observation_end,
                        const std::string &realtime_start,
                        const std::string &realtime_end) const {

  std::vector<std::pair<std::string, std::string>> q;
  q.emplace_back("series_id", series_id);
  q.emplace_back("observation_start", observation_start);
  q.emplace_back("observation_end", observation_end);
  q.emplace_back("realtime_start", realtime_start);
  q.emplace_back("realtime_end", realtime_end);
  q.emplace_back("sort_order", "asc");
  q.emplace_back("limit", "100000");  // FRED default/max limit per request

  const std::string path = "/fred/series/observations";
  auto bodyRes = httpGet(path, q);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  SeriesObservationsResponse parsed{};
  if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
    return makeError<SeriesObservationsResponse>(
        200, "Failed to parse FRED series observations JSON response", nullptr);
  }

  // Collect all observations from initial batch
  SeriesObservationsResponse result = std::move(parsed);

  // Follow pagination if more data is available
  int page_count = 1;
  int current_offset = result.offset.value_or(0);
  int limit = result.limit.value_or(100000);
  int total_count = result.count.value_or(0);

  while (current_offset + limit < total_count) {
    current_offset += limit;
    page_count++;

    SPDLOG_DEBUG("FRED pagination: fetching page {} with offset {}", page_count, current_offset);

    // Create query with new offset
    std::vector<std::pair<std::string, std::string>> page_q = q;
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

    // Append observations from this page
    result.observations.insert(result.observations.end(),
                               page.observations.begin(),
                               page.observations.end());
  }

  if (page_count > 1) {
    SPDLOG_INFO("FRED series: fetched {} pages for series_id={} total_obs={}",
                page_count, series_id, result.observations.size());
  }

  return result;
}

drogon::Task<Expected<SeriesObservationsResponse>>
SeriesImpl::fetchSeriesAsync(std::string series_id,
                             std::string observation_start,
                             std::string observation_end,
                             std::string realtime_start,
                             std::string realtime_end) const {

  std::vector<std::pair<std::string, std::string>> q;
  q.emplace_back("series_id", series_id);
  q.emplace_back("observation_start", observation_start);
  q.emplace_back("observation_end", observation_end);
  q.emplace_back("realtime_start", realtime_start);
  q.emplace_back("realtime_end", realtime_end);
  q.emplace_back("sort_order", "asc");
  q.emplace_back("limit", "100000");  // FRED default/max limit per request

  const std::string path = "/fred/series/observations";
  auto bodyRes = co_await httpAsyncGet(path, q);
  if (!bodyRes)
    co_return std::unexpected(bodyRes.error());

  SeriesObservationsResponse parsed{};
  if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
    co_return makeError<SeriesObservationsResponse>(
        200, "Failed to parse FRED series observations JSON response", nullptr);
  }

  // Collect all observations from initial batch
  SeriesObservationsResponse result = std::move(parsed);

  // Follow pagination if more data is available (async)
  int page_count = 1;
  int current_offset = result.offset.value_or(0);
  int limit = result.limit.value_or(100000);
  int total_count = result.count.value_or(0);

  while (current_offset + limit < total_count) {
    current_offset += limit;
    page_count++;

    SPDLOG_DEBUG("FRED pagination (async): fetching page {} with offset {}", page_count, current_offset);

    // Create query with new offset
    std::vector<std::pair<std::string, std::string>> page_q = q;
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

    // Append observations from this page
    result.observations.insert(result.observations.end(),
                               page.observations.begin(),
                               page.observations.end());
  }

  if (page_count > 1) {
    SPDLOG_INFO("FRED series (async): fetched {} pages for series_id={} total_obs={}",
                page_count, series_id, result.observations.size());
  }

  co_return result;
}

} // namespace data_sdk::fred
