#include "epoch_data_sdk/tradingeconomics/tradingeconomics_client.hpp"

#include <sstream>

#include <glaze/glaze.hpp>
#include "../common/event_loop_helper.hpp"

namespace data_sdk::tradingeconomics {

namespace {

std::string urlEncode(const std::string &value) {
  return drogon::utils::urlEncode(value);
}

template <typename T>
Expected<T> makeError(int status, std::string_view message,
                      const drogon::HttpResponsePtr &resp) {
  HttpError e;
  e.http_status = status;
  e.message = std::string(message);
  if (resp) {
    e.request_id = resp->getHeader("X-Request-Id");
  }
  return std::unexpected(std::move(e));
}

} // namespace

TradingEconomicsClient::TradingEconomicsClient(Options options)
    : options_(std::move(options)) {
  // Use the shared helper to get an event loop
  auto* loop = data_sdk::common::EventLoopHelper::getEventLoop(
      options_.use_drogon_main_loop,
      "TradingEconomicsClient",
      loopThread_);

  httpClient_ = drogon::HttpClient::newHttpClient(options_.base_url, loop);
}

TradingEconomicsClient::~TradingEconomicsClient() {
  data_sdk::common::EventLoopHelper::quitEventLoopThread(loopThread_);
}

std::string TradingEconomicsClient::buildQueryString(
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

Expected<std::string> TradingEconomicsClient::httpGet(
    const std::string &path,
    const std::vector<std::pair<std::string, std::string>> &query) const {
  if (options_.http_get_override) {
    return options_.http_get_override(path, query);
  }

  // Use the persistent HTTP client instead of creating a new one
  auto client = httpClient_ ? httpClient_
                            : drogon::HttpClient::newHttpClient(options_.base_url);

  auto req = drogon::HttpRequest::newHttpRequest();
  req->setMethod(drogon::Get);
  req->addHeader("User-Agent", options_.user_agent);
  req->addHeader("Accept", "application/json");

  std::vector<std::pair<std::string, std::string>> q = query;
  // TE uses ?c=apikey:secret or ?c=apikey depending on account; we accept plain
  // key
  q.emplace_back("c", options_.api_key);
  q.emplace_back("format", "json");
  req->setPath(path + buildQueryString(q));

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

Expected<std::vector<HistoricalSeriesResponse>>
TradingEconomicsClient::getHistoricalSeries(
    const std::string &symbol, std::optional<std::string> start_date,
    std::optional<std::string> end_date,
    std::optional<std::string> output_type) const {
  std::vector<std::pair<std::string, std::string>> q;
  if (start_date)
    q.emplace_back("d1", *start_date);
  if (end_date)
    q.emplace_back("d2", *end_date);
  if (output_type)
    q.emplace_back("output_type", *output_type);

  const std::string path = "/historical/series/" + urlEncode(symbol);
  auto bodyRes = httpGet(path, q);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  std::vector<HistoricalSeriesResponse> out{};
  if (auto ec = glz::read_json(out, std::string_view(*bodyRes)); ec) {
    return makeError<std::vector<HistoricalSeriesResponse>>(
        200, "Failed to parse JSON response", nullptr);
  }
  return out;
}

Expected<CalendarResponse>
TradingEconomicsClient::getCalendar(std::optional<std::string> country,
                                    std::optional<std::string> category,
                                    std::optional<std::string> start_date,
                                    std::optional<std::string> end_date) const {
  std::vector<std::pair<std::string, std::string>> q;
  if (country)
    q.emplace_back("country", *country);
  if (category)
    q.emplace_back("category", *category);
  if (start_date)
    q.emplace_back("d1", *start_date);
  if (end_date)
    q.emplace_back("d2", *end_date);

  const std::string path = "/calendar";
  auto bodyRes = httpGet(path, q);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  CalendarResponse out{};
  if (auto ec = glz::read_json(out, std::string_view(*bodyRes)); ec) {
    return makeError<CalendarResponse>(200, "Failed to parse JSON response",
                                       nullptr);
  }
  return out;
}

} // namespace data_sdk::tradingeconomics
