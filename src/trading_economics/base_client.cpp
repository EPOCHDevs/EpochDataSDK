#include "base_client.hpp"

#include <sstream>

#include <glaze/glaze.hpp>

#include "../common/event_loop_helper.hpp"

namespace data_sdk::trading_economics {

BaseClient::BaseClient(Options options) : options_(std::move(options)) {
  // Use the shared helper to get an event loop
  auto* loop = data_sdk::common::EventLoopHelper::getEventLoop(
      options_.use_drogon_main_loop, "TradingEconomicsClient", loopThread_);

  httpClient_ = drogon::HttpClient::newHttpClient(options_.base_url, loop);
}

BaseClient::~BaseClient() {
  data_sdk::common::EventLoopHelper::quitEventLoopThread(loopThread_);
}

std::string BaseClient::buildQueryString(
    const std::map<std::string, std::string>& params) {
  if (params.empty())
    return {};

  std::ostringstream oss;
  bool first = true;
  for (const auto& [key, value] : params) {
    if (!first) {
      oss << '&';
    }
    oss << key << '=' << value; // TODO: URL encode if needed
    first = false;
  }
  return oss.str();
}

Expected<std::string>
BaseClient::httpGet(const std::string& path,
                    const std::map<std::string, std::string>& params) const {
  // If there's an override for testing, use it
  if (options_.http_get_override) {
    // Convert map to vector for override
    std::vector<std::pair<std::string, std::string>> query_vec(params.begin(),
                                                                params.end());
    return options_.http_get_override(path, query_vec);
  }

  // Use the persistent HTTP client
  auto client =
      httpClient_ ? httpClient_
                  : drogon::HttpClient::newHttpClient(options_.base_url);

  auto req = drogon::HttpRequest::newHttpRequest();
  req->setMethod(drogon::Get);
  req->addHeader("User-Agent", options_.user_agent);
  req->addHeader("Accept", "application/json");

  // Build query string with API key
  std::map<std::string, std::string> query_params = params;
  query_params["c"] = options_.api_key;      // TE uses ?c=apikey
  query_params["format"] = "json";

  std::string query_str = buildQueryString(query_params);
  std::string full_path = path;
  if (!query_str.empty()) {
    full_path += "?" + query_str;
  }
  req->setPath(full_path);

  drogon::ReqResult req_result;
  drogon::HttpResponsePtr resp;
  std::tie(req_result, resp) =
      client->sendRequest(req, options_.request_timeout_sec);

  if (req_result != drogon::ReqResult::Ok || !resp) {
    return makeError<std::string>(0, "Network error or no response", resp);
  }

  const auto status = static_cast<int>(resp->getStatusCode());
  if (status < 200 || status >= 300) {
    return makeError<std::string>(status, resp->getBody(), resp);
  }

  return std::string(resp->getBody());
}

Expected<epoch_frame::DataFrame>
BaseClient::httpGetDataFrame(
    const std::string& path,
    const std::map<std::string, std::string>& params) const {
  auto json_result = httpGet(path, params);
  if (!json_result) {
    return std::unexpected(json_result.error());
  }

  return jsonToDataFrame(*json_result);
}

Expected<epoch_frame::DataFrame>
BaseClient::jsonToDataFrame(const std::string& json_str) {
  // TODO: Implement proper JSON to DataFrame conversion
  // For now, return an empty DataFrame - this needs to be implemented
  // based on epoch_frame's actual API

  // Verify JSON parses correctly
  std::vector<std::map<std::string, glz::generic>> json_data;
  auto ec = glz::read_json(json_data, json_str);
  if (ec) {
    return makeError<epoch_frame::DataFrame>(
        200, "Failed to parse JSON response", nullptr);
  }

  // Return empty DataFrame for now
  // TODO: Build proper DataFrame from json_data using epoch_frame API
  return epoch_frame::DataFrame{};
}

} // namespace data_sdk::trading_economics
