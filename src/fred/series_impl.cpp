#include "series_impl.hpp"

#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>
#include <epoch_frame/datetime.h>

namespace data_sdk::fred {

// Helper function to check if error indicates series doesn't exist in ALFRED
static bool isAlfredNotAvailableError(const HttpError& error) {
  return error.message.find("does not exist in ALFRED") != std::string::npos;
}

// Helper function to check if error indicates too many vintage dates
static bool isTooManyVintagesError(const HttpError& error) {
  return error.message.find("vintage dates") != std::string::npos &&
         error.message.find("exceeds the maximum") != std::string::npos;
}

Expected<SeriesObservationsResponse>
SeriesImpl::fetchSeries(const std::string &series_id,
                        const std::string &observation_start,
                        const std::string &observation_end,
                        const std::string &realtime_start,
                        const std::string &realtime_end) const {

  // FRED API has a 2000 vintage date limit for JSON format
  // For daily series over 20 years, this limit is exceeded
  // Solution: chunk realtime period into smaller ranges (e.g., 3 years each)
  // Reference: https://github.com/mortada/fredapi/issues/28

  // Simple year-based chunking to avoid complex date parsing
  // Extract years from YYYY-MM-DD format
  auto parseYear = [](const std::string& dateStr) -> std::optional<int> {
    if (dateStr.size() >= 4 && dateStr != "9999-12-31") {
      try {
        return std::stoi(dateStr.substr(0, 4));
      } catch (...) {
        return std::nullopt;
      }
    }
    return std::nullopt;
  };

  auto rt_start_year = parseYear(realtime_start);
  auto rt_end_year = parseYear(realtime_end);

  // Chunk if span > 3 years (3 years × 365 days = 1095 vintages for daily series, safe margin under 2000)
  // Use smaller chunks (1 year) for very high-frequency data if 3-year chunks fail
  if (rt_start_year.has_value() && rt_end_year.has_value()) {
    const int year_span = *rt_end_year - *rt_start_year;

    // Determine chunk size: default to 3 years
    constexpr int chunk_years = 3;

    if (year_span > chunk_years) {
      SPDLOG_DEBUG("FRED realtime period chunking: {}-year span, splitting into {}-year chunks",
                   year_span, chunk_years);

      SeriesObservationsResponse result;
      result.observations.clear();

      int chunk_count = 0;
      int current_year = *rt_start_year;

      while (current_year <= *rt_end_year) {
        const int chunk_end_year = std::min(current_year + chunk_years, *rt_end_year);
        chunk_count++;

        // Use original dates for first and last chunks to preserve exact boundaries
        const std::string chunk_rt_start = (current_year == *rt_start_year)
            ? realtime_start
            : std::to_string(current_year) + "-01-01";

        const std::string chunk_rt_end = (chunk_end_year == *rt_end_year)
            ? realtime_end
            : std::to_string(chunk_end_year) + "-12-31";

        SPDLOG_DEBUG("FRED chunk {}: realtime [{} - {}]", chunk_count,
                    chunk_rt_start, chunk_rt_end);

        // Recursively call fetchSeries for this chunk (won't recurse again since chunk is small)
        auto chunk_result = fetchSeries(series_id, observation_start, observation_end,
                                       chunk_rt_start, chunk_rt_end);

        if (!chunk_result) {
          const auto& error = chunk_result.error();
          SPDLOG_WARN("FRED chunk {} failed for series_id={}: {}", chunk_count, series_id, error.message);

          // Check for specific error types on first chunk
          if (chunk_count == 1) {
            // Series doesn't exist in ALFRED - fallback to FRED mode
            if (isAlfredNotAvailableError(error)) {
              SPDLOG_INFO("FRED: Series {} not in ALFRED, falling back to FRED mode (no realtime)", series_id);
              // Retry without realtime parameters (FRED mode)
              return fetchSeries(series_id, observation_start, observation_end, "", "");
            }

            // Too many vintages even with 3-year chunking - try 1-year chunks
            if (isTooManyVintagesError(error) && chunk_years == 3 && year_span > 1) {
              SPDLOG_WARN("FRED: Too many vintages with 3-year chunks, retrying with 1-year chunks");

              // Manually do 1-year chunking
              SeriesObservationsResponse retry_result;
              retry_result.observations.clear();

              for (int year = *rt_start_year; year <= *rt_end_year; ++year) {
                const std::string year_rt_start = (year == *rt_start_year) ? realtime_start : std::to_string(year) + "-01-01";
                const std::string year_rt_end = (year == *rt_end_year) ? realtime_end : std::to_string(year) + "-12-31";

                SPDLOG_DEBUG("FRED 1-year chunk: realtime [{} - {}]", year_rt_start, year_rt_end);

                auto year_result = fetchSeries(series_id, observation_start, observation_end, year_rt_start, year_rt_end);
                if (year_result) {
                  retry_result.observations.insert(retry_result.observations.end(),
                                                  year_result->observations.begin(),
                                                  year_result->observations.end());
                }
              }

              retry_result.count = retry_result.observations.size();
              SPDLOG_INFO("FRED 1-year chunking: fetched {} total observations", *retry_result.count);
              return retry_result;
            }
          }

          // Continue with other chunks even if one fails
          current_year = chunk_end_year + 1;
          continue;
        }

        // Merge observations from this chunk
        result.observations.insert(result.observations.end(),
                                  chunk_result->observations.begin(),
                                  chunk_result->observations.end());

        // Update metadata from last successful chunk
        result.count = result.observations.size();
        result.offset = 0;
        result.limit = chunk_result->limit;

        current_year = chunk_end_year + 1;
      }

      SPDLOG_INFO("FRED realtime chunking: fetched {} chunks, total {} observations",
                  chunk_count, result.observations.size());

      return result;
    }
  }

  // Single request (no chunking needed)
  std::vector<std::pair<std::string, std::string>> q;
  q.emplace_back("series_id", series_id);

  // Only add observation dates if provided (empty = omit, gets all observations like Python fredapi)
  if (!observation_start.empty()) {
    q.emplace_back("observation_start", observation_start);
  }
  if (!observation_end.empty()) {
    q.emplace_back("observation_end", observation_end);
  }

  q.emplace_back("realtime_start", realtime_start);
  q.emplace_back("realtime_end", realtime_end);
  q.emplace_back("sort_order", "asc");
  q.emplace_back("limit", "100000");  // FRED default/max limit per request

  const std::string path = "/fred/series/observations";
  constexpr int max_retries = 3; // Retry up to 3 times with exponential backoff
  auto bodyRes = httpGetWithRetry(path, q, max_retries);
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

    auto page_body_res = httpGetWithRetry(path, page_q, max_retries);
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

  // FRED API has a 2000 vintage date limit for JSON format
  // For daily series over 20 years, this limit is exceeded
  // Solution: chunk realtime period into smaller ranges (e.g., 3 years each)
  // Reference: https://github.com/mortada/fredapi/issues/28

  // Simple year-based chunking to avoid complex date parsing
  // Extract years from YYYY-MM-DD format
  auto parseYear = [](const std::string& dateStr) -> std::optional<int> {
    if (dateStr.size() >= 4 && dateStr != "9999-12-31") {
      try {
        return std::stoi(dateStr.substr(0, 4));
      } catch (...) {
        return std::nullopt;
      }
    }
    return std::nullopt;
  };

  auto rt_start_year = parseYear(realtime_start);
  auto rt_end_year = parseYear(realtime_end);

  // Chunk if span > 3 years (3 years × 365 days = 1095 vintages for daily series, safe margin under 2000)
  // Use smaller chunks (1 year) for very high-frequency data if 3-year chunks fail
  if (rt_start_year.has_value() && rt_end_year.has_value()) {
    const int year_span = *rt_end_year - *rt_start_year;

    // Determine chunk size: default to 3 years
    constexpr int chunk_years = 3;

    if (year_span > chunk_years) {
      SPDLOG_DEBUG("FRED realtime period chunking (async): {}-year span, splitting into {}-year chunks",
                   year_span, chunk_years);

      SeriesObservationsResponse result;
      result.observations.clear();

      int chunk_count = 0;
      int current_year = *rt_start_year;

      while (current_year <= *rt_end_year) {
        const int chunk_end_year = std::min(current_year + chunk_years, *rt_end_year);
        chunk_count++;

        // Use original dates for first and last chunks to preserve exact boundaries
        const std::string chunk_rt_start = (current_year == *rt_start_year)
            ? realtime_start
            : std::to_string(current_year) + "-01-01";

        const std::string chunk_rt_end = (chunk_end_year == *rt_end_year)
            ? realtime_end
            : std::to_string(chunk_end_year) + "-12-31";

        SPDLOG_DEBUG("FRED chunk {} (async): realtime [{} - {}]", chunk_count,
                    chunk_rt_start, chunk_rt_end);

        // Recursively call fetchSeriesAsync for this chunk (won't recurse again since chunk is small)
        auto chunk_result = co_await fetchSeriesAsync(series_id, observation_start, observation_end,
                                                      chunk_rt_start, chunk_rt_end);

        if (!chunk_result) {
          const auto& error = chunk_result.error();
          SPDLOG_WARN("FRED chunk {} (async) failed for series_id={}: {}", chunk_count, series_id, error.message);

          // Check for specific error types on first chunk
          if (chunk_count == 1) {
            // Series doesn't exist in ALFRED - fallback to FRED mode
            if (isAlfredNotAvailableError(error)) {
              SPDLOG_INFO("FRED (async): Series {} not in ALFRED, falling back to FRED mode (no realtime)", series_id);
              // Retry without realtime parameters (FRED mode)
              co_return co_await fetchSeriesAsync(series_id, observation_start, observation_end, "", "");
            }

            // Too many vintages even with 3-year chunking - try 1-year chunks
            if (isTooManyVintagesError(error) && chunk_years == 3 && year_span > 1) {
              SPDLOG_WARN("FRED (async): Too many vintages with 3-year chunks, retrying with 1-year chunks");

              // Manually do 1-year chunking
              SeriesObservationsResponse retry_result;
              retry_result.observations.clear();

              for (int year = *rt_start_year; year <= *rt_end_year; ++year) {
                const std::string year_rt_start = (year == *rt_start_year) ? realtime_start : std::to_string(year) + "-01-01";
                const std::string year_rt_end = (year == *rt_end_year) ? realtime_end : std::to_string(year) + "-12-31";

                SPDLOG_DEBUG("FRED 1-year chunk (async): realtime [{} - {}]", year_rt_start, year_rt_end);

                auto year_result = co_await fetchSeriesAsync(series_id, observation_start, observation_end, year_rt_start, year_rt_end);
                if (year_result) {
                  retry_result.observations.insert(retry_result.observations.end(),
                                                  year_result->observations.begin(),
                                                  year_result->observations.end());
                }
              }

              retry_result.count = retry_result.observations.size();
              SPDLOG_INFO("FRED 1-year chunking (async): fetched {} total observations", *retry_result.count);
              co_return retry_result;
            }
          }

          // Continue with other chunks even if one fails
          current_year = chunk_end_year + 1;
          continue;
        }

        // Merge observations from this chunk
        result.observations.insert(result.observations.end(),
                                  chunk_result->observations.begin(),
                                  chunk_result->observations.end());

        // Update metadata from last successful chunk
        result.count = result.observations.size();
        result.offset = 0;
        result.limit = chunk_result->limit;

        current_year = chunk_end_year + 1;
      }

      SPDLOG_INFO("FRED realtime chunking (async): fetched {} chunks, total {} observations",
                  chunk_count, result.observations.size());

      co_return result;
    }
  }

  // Single request (no chunking needed)
  std::vector<std::pair<std::string, std::string>> q;
  q.emplace_back("series_id", series_id);

  // Only add observation dates if provided (empty = omit, gets all observations like Python fredapi)
  if (!observation_start.empty()) {
    q.emplace_back("observation_start", observation_start);
  }
  if (!observation_end.empty()) {
    q.emplace_back("observation_end", observation_end);
  }

  q.emplace_back("realtime_start", realtime_start);
  q.emplace_back("realtime_end", realtime_end);
  q.emplace_back("sort_order", "asc");
  q.emplace_back("limit", "100000");  // FRED default/max limit per request

  const std::string path = "/fred/series/observations";
  constexpr int max_retries = 3; // Retry up to 3 times with exponential backoff
  auto bodyRes = co_await httpAsyncGetWithRetry(path, q, max_retries);
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

    auto page_body_res = co_await httpAsyncGetWithRetry(path, page_q, max_retries);
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
