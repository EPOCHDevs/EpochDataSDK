#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <drogon/drogon.h>
#include <expected>
#include <trantor/net/EventLoopThread.h>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <epoch_frame/datetime.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::fred {

template <typename T> using Expected = std::expected<T, HttpError>;

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

  Expected<std::string>
  httpGet(const std::string &path,
          const std::vector<std::pair<std::string, std::string>> &query) const {
    return drogon::sync_wait(httpAsyncGet(path, query));
  }

  // Async retry wrapper - retries on 5xx and network errors only (not 4xx)
  drogon::Task<Expected<std::string>> httpAsyncGetWithRetry(
      std::string path,
      std::vector<std::pair<std::string, std::string>> query,
      int max_retries) const;

  Expected<std::string> httpGetWithRetry(
      const std::string &path,
      const std::vector<std::pair<std::string, std::string>> &query,
      int max_retries) const;

  // Helper methods
  static std::string buildQueryString(
      const std::vector<std::pair<std::string, std::string>> &query);

  static std::optional<int> parseIntHeader(const drogon::HttpResponsePtr &resp,
                                           const std::string &key);

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
};

// Helper function for FRED timestamp conversion
// Convert date strings ("YYYY-MM-DD") to DateTime objects with UTC timezone
// Returns vector of DateTime objects, ensuring proper UTC timezone handling
inline std::vector<epoch_frame::DateTime> parseDateStringsToDateTimes(
    const std::vector<std::string>& date_strings) {

  std::vector<epoch_frame::DateTime> datetimes;
  datetimes.reserve(date_strings.size());

  for (const auto& date_str : date_strings) {
    if (date_str.empty()) {
      // For null dates, use epoch as placeholder
      datetimes.push_back(epoch_frame::DateTime::fromtimestamp(0, "UTC"));
    } else {
      // Parse date string as UTC midnight
      datetimes.push_back(epoch_frame::DateTime::from_date_str(date_str, "UTC"));
    }
  }

  return datetimes;
}

inline std::vector<int64_t> parseDateStringsToNanoseconds(
    const std::vector<std::string>& date_strings) {

  // Create string array from date strings
  arrow::StringBuilder string_builder;
  auto status = string_builder.Reserve(date_strings.size());
  if (!status.ok()) {
    throw std::runtime_error("Failed to reserve string builder: " + status.ToString());
  }
  for (const auto& date_str : date_strings) {
    if (date_str.empty()) {
      auto append_status = string_builder.AppendNull();
      if (!append_status.ok()) {
        throw std::runtime_error("Failed to append null: " + append_status.ToString());
      }
    } else {
      auto append_status = string_builder.Append(date_str);
      if (!append_status.ok()) {
        throw std::runtime_error("Failed to append string: " + append_status.ToString());
      }
    }
  }

  auto string_array_result = string_builder.Finish();
  if (!string_array_result.ok()) {
    throw std::runtime_error("Failed to finish string array: " + string_array_result.status().ToString());
  }

  // Use Arrow's strptime to convert strings to timestamps
  arrow::compute::StrptimeOptions options("%Y-%m-%d", arrow::TimeUnit::NANO, false);
  options.error_is_null = true;  // Invalid dates become null instead of throwing error

  auto maybe_result = arrow::compute::Strptime({*string_array_result}, options);
  if (!maybe_result.ok()) {
    throw std::runtime_error("Failed to parse dates with strptime: " + maybe_result.status().ToString());
  }

  auto timestamp_array_no_tz = (*maybe_result).make_array();

  // Localize to UTC using AssumeTimezone
  arrow::compute::AssumeTimezoneOptions tz_opts("UTC");
  auto localize_result = arrow::compute::AssumeTimezone(timestamp_array_no_tz, tz_opts);
  if (!localize_result.ok()) {
    throw std::runtime_error("Failed to localize to UTC: " + localize_result.status().ToString());
  }

  auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>((*localize_result).make_array());

  // Extract timestamps into vector
  std::vector<int64_t> timestamps;
  timestamps.reserve(timestamp_array->length());

  for (int64_t i = 0; i < timestamp_array->length(); ++i) {
    if (timestamp_array->IsNull(i)) {
      timestamps.push_back(0);  // Use 0 for null timestamps
    } else {
      timestamps.push_back(timestamp_array->Value(i));
    }
  }

  return timestamps;
}

// Helper function to create timestamp ChunkedArray for DataFrame columns
// Returns arrow::Timestamp[nano, UTC] typed ChunkedArray
inline std::shared_ptr<arrow::ChunkedArray> makeDateTimestampArray(
    const std::vector<std::string>& date_strings) {

  auto timestamps = parseDateStringsToNanoseconds(date_strings);

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

} // namespace data_sdk::fred
