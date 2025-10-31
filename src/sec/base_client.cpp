#include "epoch_data_sdk/sec/base_client.hpp"

#include <chrono>
#include <sstream>

#include <spdlog/spdlog.h>

#include "../common/event_loop_helper.hpp"

namespace data_sdk::sec {

namespace {

std::string urlEncode(const std::string &value) {
  return drogon::utils::urlEncode(value);
}

} // namespace

BaseClient::BaseClient(Options options) : options_(std::move(options)) {
  // Use the shared helper to get an event loop
  auto* loop = data_sdk::common::EventLoopHelper::getEventLoop(
      options_.use_drogon_main_loop,
      "SECBaseClient",
      loopThread_);

  httpClient_ = drogon::HttpClient::newHttpClient(options_.base_url, loop);

  // Initialize rate limiter (TokenBucket, thread-safe)
  if (options_.enable_rate_limiting) {
    size_t capacity = options_.rate_limit_burst_capacity.value_or(
        static_cast<size_t>(options_.max_requests_per_second * 2.0));

    // Create token bucket rate limiter with per-second granularity
    auto limiter = drogon::RateLimiter::newRateLimiter(
        drogon::RateLimiterType::kTokenBucket,
        capacity,
        std::chrono::seconds(1));

    // Wrap in SafeRateLimiter for thread safety
    rateLimiter_ = std::make_shared<drogon::SafeRateLimiter>(limiter);
  }
}

BaseClient::~BaseClient() {
  data_sdk::common::EventLoopHelper::quitEventLoopThread(loopThread_);
}

std::optional<int>
BaseClient::parseIntHeader(const drogon::HttpResponsePtr &resp,
                          const std::string &key) {
  if (!resp)
    return std::nullopt;
  const auto v = resp->getHeader(key);
  if (v.empty())
    return std::nullopt;
  try {
    return std::stoi(v);
  } catch (...) {
    return std::nullopt;
  }
}

std::string BaseClient::buildQueryString(
    const std::vector<std::pair<std::string, std::string>> &query) {
  if (query.empty())
    return {};
  std::ostringstream oss;
  bool first = true;
  for (const auto &[k, v] : query) {
    oss << (first ? '?' : '&') << urlEncode(k) << '=' << urlEncode(v);
    first = false;
  }
  return oss.str();
}

auto BaseClient::httpAsyncGet(
    std::string path,
    std::vector<std::pair<std::string, std::string>> query) const
    -> drogon::Task<Expected<std::string>> {
  // Parameters are passed by value to avoid coroutine lifetime issues

  if (options_.http_get_override) {
    auto result = options_.http_get_override(path, query);
    SPDLOG_WARN("httpAsyncGet override returned for: {}", path);
    co_return result;
  }

  // Add API token to query parameters
  query.emplace_back("token", options_.api_key);

  // Acquire rate limit token before making request
  if (rateLimiter_) {
    while (!rateLimiter_->isAllowed()) {
      // Yield control back to event loop
      co_await drogon::sleepCoro(httpClient_->getLoop(), 0.01); // 10ms
    }
  }

  auto client = httpClient_;
  const std::string url = path + buildQueryString(query);

  // Retry loop for handling 429 (Too Many Requests) errors
  for (int attempt = 0; attempt < options_.max_retries; ++attempt) {
    auto req = drogon::HttpRequest::newHttpRequest();
    req->setMethod(drogon::Get);
    req->setPath(url);

    try {
      auto resp = co_await client->sendRequestCoro(req);

      if (resp->getStatusCode() == drogon::k200OK) {
        co_return std::string(resp->getBody());
      } else if (resp->getStatusCode() == static_cast<drogon::HttpStatusCode>(429)) {
        // Rate limited - wait and retry
        double delay = options_.retry_delay_seconds * (attempt + 1);
        SPDLOG_WARN("SEC API rate limited (429), retrying in {}s (attempt {}/{})",
                   delay, attempt + 1, options_.max_retries);
        co_await drogon::sleepCoro(client->getLoop(), delay);
        continue;
      } else {
        // Other HTTP error
        co_return std::unexpected(HttpError{
            static_cast<int>(resp->getStatusCode()),
            "HTTP error: " + std::to_string(static_cast<int>(resp->getStatusCode())) +
            " - " + std::string(resp->getBody())
        });
      }
    } catch (const std::exception &e) {
      SPDLOG_ERROR("HTTP request exception: {}", e.what());
      co_return std::unexpected(
          HttpError{0, std::string("Request failed: ") + e.what()});
    }
  }

  // All retries exhausted
  co_return std::unexpected(
      HttpError{429, "Rate limit exceeded after " +
                     std::to_string(options_.max_retries) + " retries"});
}

auto BaseClient::httpAsyncPost(
    std::string path,
    const std::string &body,
    std::vector<std::pair<std::string, std::string>> query) const
    -> drogon::Task<Expected<std::string>> {
  // Parameters are passed by value to avoid coroutine lifetime issues

  if (options_.http_post_override) {
    auto result = options_.http_post_override(path, body, query);
    SPDLOG_WARN("httpAsyncPost override returned for: {}", path);
    co_return result;
  }

  // Add API token to query parameters
  query.emplace_back("token", options_.api_key);

  // Acquire rate limit token before making request
  if (rateLimiter_) {
    while (!rateLimiter_->isAllowed()) {
      co_await drogon::sleepCoro(httpClient_->getLoop(), 0.01); // 10ms
    }
  }

  auto client = httpClient_;
  const std::string url = path + buildQueryString(query);

  // Retry loop for handling 429 (Too Many Requests) errors
  for (int attempt = 0; attempt < options_.max_retries; ++attempt) {
    auto req = drogon::HttpRequest::newHttpRequest();
    req->setMethod(drogon::Post);
    req->setPath(url);
    req->setBody(body);
    req->setContentTypeCode(drogon::CT_APPLICATION_JSON);

    SPDLOG_DEBUG("POST {} with body: {}", url, body);

    try {
      auto resp = co_await client->sendRequestCoro(req);

      if (resp->getStatusCode() == drogon::k200OK) {
        co_return std::string(resp->getBody());
      } else if (resp->getStatusCode() == static_cast<drogon::HttpStatusCode>(429)) {
        // Rate limited - wait and retry
        double delay = options_.retry_delay_seconds * (attempt + 1);
        SPDLOG_WARN("SEC API rate limited (429), retrying in {}s (attempt {}/{})",
                   delay, attempt + 1, options_.max_retries);
        co_await drogon::sleepCoro(client->getLoop(), delay);
        continue;
      } else {
        // Other HTTP error
        co_return std::unexpected(HttpError{
            static_cast<int>(resp->getStatusCode()),
            "HTTP error: " + std::to_string(static_cast<int>(resp->getStatusCode())) +
            " - " + std::string(resp->getBody())
        });
      }
    } catch (const std::exception &e) {
      SPDLOG_ERROR("HTTP request exception: {}", e.what());
      co_return std::unexpected(
          HttpError{0, std::string("Request failed: ") + e.what()});
    }
  }

  // All retries exhausted
  co_return std::unexpected(
      HttpError{429, "Rate limit exceeded after " +
                     std::to_string(options_.max_retries) + " retries"});
}

} // namespace data_sdk::sec
