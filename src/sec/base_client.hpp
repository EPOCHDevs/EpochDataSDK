#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <drogon/drogon.h>
#include <drogon/RateLimiter.h>
#include <trantor/net/EventLoopThread.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::sec {

template <typename T> using Expected = std::expected<T, HttpError>;

// Parse ISO 8601 timestamp with timezone offset to nanoseconds since epoch
// Format: "YYYY-MM-DDTHH:MM:SS±HH:MM" (e.g., "2024-10-15T16:30:00-04:00")
inline std::vector<std::int64_t> parseISO8601ToNanoseconds(
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

    // Parse timezone offset if present (±HH:MM)
    int tz_offset_minutes = 0;
    if (ts_str.size() >= 25) {  // Has timezone: "YYYY-MM-DDTHH:MM:SS±HH:MM"
      size_t tz_pos = ts_str.find_first_of("+-", 19);
      if (tz_pos != std::string::npos && tz_pos + 5 < ts_str.size()) {
        int tz_sign = (ts_str[tz_pos] == '-') ? -1 : 1;
        int tz_hours = std::atoi(ts_str.substr(tz_pos + 1, 2).c_str());
        int tz_mins = std::atoi(ts_str.substr(tz_pos + 4, 2).c_str());
        tz_offset_minutes = tz_sign * (tz_hours * 60 + tz_mins);
      }
    }

    using namespace std::chrono;
    auto ymd = year_month_day{std::chrono::year{year},
                               std::chrono::month{static_cast<unsigned>(month)},
                               std::chrono::day{static_cast<unsigned>(day)}};
    auto dp = sys_days{ymd};
    auto time_point = dp + hours{hour} + minutes{minute} + seconds{second};

    // Convert to UTC by subtracting the timezone offset
    time_point -= minutes{tz_offset_minutes};

    timestamps.push_back(duration_cast<nanoseconds>(time_point.time_since_epoch()).count());
  }

  return timestamps;
}

/**
 * @brief Base client for SEC API operations
 *
 * Provides common HTTP functionality with rate limiting, retry logic,
 * and error handling for all SEC API endpoints.
 */
class BaseClient {
protected:
  Options options_;
  std::shared_ptr<drogon::HttpClient> httpClient_;
  std::shared_ptr<drogon::SafeRateLimiter> rateLimiter_;
  std::unique_ptr<trantor::EventLoopThread> loopThread_;

  explicit BaseClient(Options options);
  virtual ~BaseClient();

  // HTTP methods
  drogon::Task<Expected<std::string>>
  httpAsyncGet(std::string path,
               std::vector<std::pair<std::string, std::string>> query = {}) const;

  drogon::Task<Expected<std::string>>
  httpAsyncPost(std::string path, const std::string &body,
                std::vector<std::pair<std::string, std::string>> query = {}) const;

  // Utility methods
  static std::optional<int> parseIntHeader(const drogon::HttpResponsePtr &resp,
                                           const std::string &key);

  static std::string buildQueryString(
      const std::vector<std::pair<std::string, std::string>> &query);

public:
  BaseClient(const BaseClient &) = delete;
  BaseClient &operator=(const BaseClient &) = delete;
  BaseClient(BaseClient &&) = default;
  BaseClient &operator=(BaseClient &&) = default;
};

} // namespace data_sdk::sec
