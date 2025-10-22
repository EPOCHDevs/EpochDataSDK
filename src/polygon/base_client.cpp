#include "epoch_data_sdk/polygon/base_client.hpp"

#include <chrono>
#include <sstream>
#include <thread>

#include <spdlog/spdlog.h>
#include <epoch_frame/dataframe.h>

#include "../common/event_loop_helper.hpp"

namespace data_sdk::polygon {

namespace {

std::string urlEncode(const std::string &value) {
  return drogon::utils::urlEncode(value);
}

} // namespace

BaseClient::BaseClient(Options options) : options_(std::move(options)) {
  // Use the shared helper to get an event loop
  auto* loop = data_sdk::common::EventLoopHelper::getEventLoop(
      options_.use_drogon_main_loop,
      "PolygonBaseClient",
      loopThread_);

  httpClient_ = drogon::HttpClient::newHttpClient(options_.base_url, loop);
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
    const std::string &path,
    const std::vector<std::pair<std::string, std::string>> &query) const
    -> drogon::Task<Expected<std::string>> {
  if (options_.http_get_override)
    co_return options_.http_get_override(path, query);

  auto client = httpClient_
                    ? httpClient_
                    : drogon::HttpClient::newHttpClient(options_.base_url);
  auto req = drogon::HttpRequest::newHttpRequest();
  req->setMethod(drogon::Get);
  req->addHeader("User-Agent", options_.user_agent);
  req->addHeader("Accept", "application/json");

  std::vector<std::pair<std::string, std::string>> q = query;
  q.emplace_back("apiKey", options_.api_key);
  req->setPath(path + buildQueryString(q));

  try {
    auto resp =
        co_await client->sendRequestCoro(req, options_.request_timeout_sec);
    if (!resp) {
      co_return makeError<std::string>(0, "Network error or no response", resp);
    }

    const auto status = static_cast<int>(resp->getStatusCode());
    if (status < 200 || status >= 300) {
      co_return makeError<std::string>(status, resp->getBody(), resp);
    }

    co_return std::string(resp->getBody());
  } catch (const std::exception &e) {
    co_return makeError<std::string>(0, e.what(), nullptr);
  }
}

Expected<std::string> BaseClient::httpGetWithRetry(
    const std::string &path,
    const std::vector<std::pair<std::string, std::string>> &query,
    int max_retries) const {
  for (int attempt = 0; attempt <= max_retries; ++attempt) {
    auto result = httpGet(path, query);
    if (result) {
      return result;
    }

    // Don't retry client errors (4xx) - these won't succeed on retry
    const int status = result.error().http_status;
    if (status >= 400 && status < 500) {
      SPDLOG_WARN("Polygon HTTP client error (won't retry): path={} status={} error={}",
                  path, status, result.error().message);
      return result;
    }

    // Log retry attempt for transient errors (5xx, network errors)
    if (attempt < max_retries) {
      const auto delay_ms =
          100 * (1 << attempt); // exponential backoff: 100, 200, 400ms
      SPDLOG_WARN(
          "Polygon HTTP retry {}/{} for path={} error={} retrying in {}ms",
          attempt + 1, max_retries, path, result.error().message, delay_ms);
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    } else {
      SPDLOG_ERROR("Polygon HTTP failed after {} retries for path={} error={}",
                   max_retries, path, result.error().message);
      return result; // Return the final error
    }
  }
  std::unreachable();
}

std::optional<BaseClient::ParsedUrl>
BaseClient::parseNextUrl(const std::string &next_url) {
  if (next_url.empty())
    return std::nullopt;

  ParsedUrl result;

  // Find query string separator
  auto qpos = next_url.find('?');
  if (qpos == std::string::npos)
    return std::nullopt;

  // Extract path - look for API version prefix (e.g., /v1/, /v2/, /vX/, /stocks/)
  // Check for /stocks/ first since it's more specific
  auto path_start = next_url.find("/stocks/");
  if (path_start == std::string::npos) {
    path_start = next_url.find("/vX/");
  }
  if (path_start == std::string::npos) {
    path_start = next_url.find("/v2/");
  }
  if (path_start == std::string::npos) {
    path_start = next_url.find("/v3/");
  }
  if (path_start == std::string::npos) {
    path_start = next_url.find("/v1/");
  }
  if (path_start == std::string::npos)
    return std::nullopt;

  result.path = next_url.substr(path_start, qpos - path_start);

  // Parse query string
  auto query_str = next_url.substr(qpos + 1);
  std::stringstream ss(query_str);
  std::string kv;
  while (std::getline(ss, kv, '&')) {
    auto eq = kv.find('=');
    if (eq != std::string::npos) {
      result.query.emplace_back(kv.substr(0, eq), kv.substr(eq + 1));
    }
  }

  return result;
}

template <typename T>
Expected<T> BaseClient::makeError(int status, std::string_view message,
                                  const drogon::HttpResponsePtr &resp) {
  HttpError e;
  e.http_status = status;
  e.message = std::string(message);
  if (resp) {
    e.request_id = resp->getHeader("X-Request-Id");
    if (e.request_id.empty()) {
      e.request_id = resp->getHeader("Request-Id");
    }
    const auto rem = resp->getHeader("X-RateLimit-Remaining");
    const auto lim = resp->getHeader("X-RateLimit-Limit");
    if (!rem.empty()) {
      try {
        e.rate_limit_remaining = std::stoi(rem);
      } catch (...) {
      }
    }
    if (!lim.empty()) {
      try {
        e.rate_limit_limit = std::stoi(lim);
      } catch (...) {
      }
    }
  }
  return std::unexpected(std::move(e));
}

// Explicit template instantiations for commonly used types
template Expected<std::string> BaseClient::makeError(int, std::string_view, const drogon::HttpResponsePtr &);
template Expected<epoch_frame::DataFrame> BaseClient::makeError(int, std::string_view, const drogon::HttpResponsePtr &);

} // namespace data_sdk::polygon
