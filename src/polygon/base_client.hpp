#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <drogon/drogon.h>
#include <drogon/RateLimiter.h>
#include <expected>
#include <trantor/net/EventLoopThread.h>

#include "epoch_data_sdk/common/metadata.hpp"
#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// Date/Time Parsing Helpers
// Parse "YYYY-MM-DD" date strings to nanoseconds since epoch at midnight UTC
inline std::vector<std::int64_t> parseDateStringsToMidnightUTC(
    const std::vector<std::string>& date_strings) {
  std::vector<std::int64_t> timestamps;
  timestamps.reserve(date_strings.size());

  for (const auto& date_str : date_strings) {
    if (date_str.size() < 10) {
      timestamps.push_back(0);
      continue;
    }

    int y_val = std::atoi(date_str.substr(0, 4).c_str());
    int m_val = std::atoi(date_str.substr(5, 2).c_str());
    int d_val = std::atoi(date_str.substr(8, 2).c_str());

    using namespace std::chrono;
    auto ymd = year_month_day{year{y_val}, month{static_cast<unsigned>(m_val)},
                               day{static_cast<unsigned>(d_val)}};
    auto dp = sys_days{ymd};
    timestamps.push_back(duration_cast<nanoseconds>(dp.time_since_epoch()).count());
  }

  return timestamps;
}

// Convert date strings to Arrow timestamp ChunkedArray for DataFrame columns
// Returns arrow::Timestamp[nano, UTC] typed array
inline std::shared_ptr<arrow::ChunkedArray> makeDateTimestampArray(
    const std::vector<std::string>& date_strings) {

  auto timestamps = parseDateStringsToMidnightUTC(date_strings);

  // Create timestamp array with nanosecond precision and UTC timezone
  auto timestamp_type = arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
  arrow::TimestampBuilder builder(timestamp_type, arrow::default_memory_pool());

  auto status = builder.Reserve(timestamps.size());
  if (!status.ok()) {
    throw std::runtime_error("Failed to reserve timestamp builder: " + status.ToString());
  }

  for (const auto& ts : timestamps) {
    // Treat 0 as null (invalid dates)
    if (ts == 0) {
      auto append_status = builder.AppendNull();
      if (!append_status.ok()) {
        throw std::runtime_error("Failed to append null: " + append_status.ToString());
      }
    } else {
      auto append_status = builder.Append(ts);
      if (!append_status.ok()) {
        throw std::runtime_error("Failed to append timestamp: " + append_status.ToString());
      }
    }
  }

  auto result = builder.Finish();
  if (!result.ok()) {
    throw std::runtime_error("Failed to finish timestamp array: " + result.status().ToString());
  }

  auto chunked = arrow::ChunkedArray::Make({*result});
  if (!chunked.ok()) {
    throw std::runtime_error("Failed to create chunked array: " + chunked.status().ToString());
  }

  return *chunked;
}

// Parse RFC3339 "YYYY-MM-DDTHH:MM:SSZ" timestamp strings to nanoseconds since epoch
// Preserves time component (unlike parseDateStringsToMidnightUTC)
inline std::vector<std::int64_t> parseRFC3339ToNanoseconds(
    const std::vector<std::string>& timestamp_strings) {
  std::vector<std::int64_t> timestamps;
  timestamps.reserve(timestamp_strings.size());

  for (const auto& ts_str : timestamp_strings) {
    if (ts_str.size() < 19) {  // Minimum: "YYYY-MM-DDTHH:MM:SS"
      timestamps.push_back(0);
      continue;
    }

    // Parse date part: YYYY-MM-DD
    int year = std::atoi(ts_str.substr(0, 4).c_str());
    int month = std::atoi(ts_str.substr(5, 2).c_str());
    int day = std::atoi(ts_str.substr(8, 2).c_str());

    // Parse time part: HH:MM:SS (after the 'T')
    int hour = std::atoi(ts_str.substr(11, 2).c_str());
    int minute = std::atoi(ts_str.substr(14, 2).c_str());
    int second = std::atoi(ts_str.substr(17, 2).c_str());

    using namespace std::chrono;
    auto ymd = year_month_day{std::chrono::year{year},
                               std::chrono::month{static_cast<unsigned>(month)},
                               std::chrono::day{static_cast<unsigned>(day)}};
    auto dp = sys_days{ymd};
    auto time_point = dp + hours{hour} + minutes{minute} + seconds{second};

    timestamps.push_back(duration_cast<nanoseconds>(time_point.time_since_epoch()).count());
  }

  return timestamps;
}

// BaseClient - Private base class containing shared HTTP functionality
// This class is NOT exposed in the public API (not in include/)
class BaseClient {
protected:
  explicit BaseClient(Options options);
  virtual ~BaseClient();

  // HTTP request methods
  // NOTE: Parameters taken by value to avoid coroutine lifetime issues
  drogon::Task<Expected<std::string>> httpAsyncGet(
      std::string path,
      std::vector<std::pair<std::string, std::string>> query) const;

  // Async retry wrapper - retries on 5xx and network errors only (not 4xx)
  drogon::Task<Expected<std::string>> httpAsyncGetWithRetry(
      std::string path,
      std::vector<std::pair<std::string, std::string>> query,
      int max_retries) const;

  Expected<std::string>
  httpGet(const std::string &path,
          const std::vector<std::pair<std::string, std::string>> &query) const {
    return drogon::sync_wait(httpAsyncGet(path, query));
  }

  Expected<std::string> httpGetWithRetry(
      const std::string &path,
      const std::vector<std::pair<std::string, std::string>> &query,
      int max_retries) const;

  // Helper methods
  static std::string buildQueryString(
      const std::vector<std::pair<std::string, std::string>> &query);

  static std::optional<int> parseIntHeader(const drogon::HttpResponsePtr &resp,
                                           const std::string &key);

  // Pagination helper - parses next_url into path and query
  struct ParsedUrl {
    std::string path;
    std::vector<std::pair<std::string, std::string>> query;
  };

  static std::optional<ParsedUrl> parseNextUrl(const std::string &next_url);

  // Error creation helper
  template <typename T>
  static Expected<T> makeError(int status, std::string_view message,
                               const drogon::HttpResponsePtr &resp);

  // Options accessor for derived classes
  const Options &options() const { return options_; }

private:
  Options options_;
  // Dedicated event loop and HTTP client bound to it
  std::unique_ptr<trantor::EventLoopThread> loopThread_;
  drogon::HttpClientPtr httpClient_;
  // Rate limiter for API throttling (thread-safe)
  drogon::RateLimiterPtr rateLimiter_;
};

} // namespace data_sdk::polygon
