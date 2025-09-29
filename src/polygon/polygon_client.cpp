#include "polygon_client.hpp"

#include <chrono>
#include <sstream>

#include "epoch_frame/aliases.h"
#include "models.hpp"
#include "../common/event_loop_helper.hpp"
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>
#include <thread>
// Keep a reusable HTTP client using Drogon's own loop

namespace data_sdk::polygon {

namespace {

std::string urlEncode(const std::string &value) {
  return drogon::utils::urlEncode(value);
}

// Shared accessor for America/New_York time zone (handles DST correctly)
inline const std::chrono::time_zone *ny_time_zone() {
  static const std::chrono::time_zone *tz =
      std::chrono::locate_zone("America/New_York");
  return tz;
}

// (no-op helper removed; keep conversions inline where needed)

// Normalize various epoch units (s, ms, us, ns) to nanoseconds
// Detect crypto ticker (Polygon uses X: prefix)
inline bool isCryptoTicker(const std::string &ticker) {
  return ticker.rfind("X:", 0) == 0;
}

inline bool isFxTicker(const std::string &ticker) {
  return ticker.rfind("C:", 0) == 0;
}

inline bool isIndexTicker(const std::string &ticker) {
  return !ticker.empty() && ticker.front() == '^';
}

inline bool isStockTicker(const std::string &ticker) {
  return !isCryptoTicker(ticker) && !isFxTicker(ticker) &&
         !isIndexTicker(ticker);
}

// Forward declaration
inline std::int64_t normalizeMsESTToUTC_impl(std::int64_t ts_ms);

// Timestamp converters
inline std::int64_t utcMsToNs(std::int64_t ts_ms) {
  return ts_ms * 1'000'000LL;
}
inline std::int64_t etMsToUtcNs(std::int64_t ts_ms) {
  return normalizeMsESTToUTC_impl(ts_ms);
}

// Normalize EOD timestamp to midnight UTC (00:00:00)
inline std::int64_t normalizeEodToMidnightUTC(std::int64_t ts_ms) {
  using namespace std::chrono;
  // Convert ms to days, then back to get midnight
  const auto tp = sys_time<milliseconds>{milliseconds{ts_ms}};
  const auto day = floor<days>(tp);
  return duration_cast<nanoseconds>(day.time_since_epoch()).count();
}

// Check if UTC ns timestamp falls within NY RTH [09:31, 16:00]
inline bool isWithinNYRth(std::int64_t ts_utc_ns) {
  using namespace std::chrono;
  try {
    const time_zone *ny = ny_time_zone();
    const sys_time<nanoseconds> sys{nanoseconds{ts_utc_ns}};
    const zoned_time<nanoseconds> zt{ny, sys};
    const local_time<nanoseconds> lt = zt.get_local_time();
    const auto day = floor<days>(lt);
    const auto tod = lt - day; // duration since midnight
    const auto start = hours{9} + minutes{31};
    const auto end = hours{16};
    return tod >= start && tod <= end;
  } catch (...) {
    // If tz DB unavailable, do not filter out
    return true;
  }
}

// Convert milliseconds since local ET epoch to UTC nanoseconds using
// std::chrono
inline std::int64_t normalizeMsESTToUTC_impl(std::int64_t ts_ms) {
  using namespace std::chrono;
  try {
    const time_zone *ny = ny_time_zone();
    local_time<milliseconds> local_et{milliseconds{ts_ms}};
    zoned_time<milliseconds> zt{ny, local_et};
    const sys_time<milliseconds> sys = zt.get_sys_time();
    auto ttc = sys.time_since_epoch().count();
    return static_cast<int64_t>(ttc * 1'000'000LL);
  } catch (...) {
    // Fallback if timezone database is unavailable: treat as UTC ms
    return ts_ms * 1'000'000LL;
  }
}

// File-scope structs for v3 JSON parsing (avoid local types for Glaze)
struct V3QuoteRow {
  std::optional<int> ask_exchange;
  std::optional<double> ask_price;
  std::optional<std::int64_t> ask_size;
  std::optional<int> bid_exchange;
  std::optional<double> bid_price;
  std::optional<std::int64_t> bid_size;
  std::optional<std::vector<int>> conditions;
  std::optional<std::int64_t> participant_timestamp;
  std::optional<std::int64_t> sequence_number;
  std::optional<std::int64_t> sip_timestamp;
  std::optional<int> tape;
};

struct V3QuotesResp {
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<V3QuoteRow> results;
  std::string status;
};

struct V3TradeRow {
  std::optional<std::vector<int>> conditions;
  std::optional<int> exchange;
  std::optional<std::string> id;
  std::optional<std::int64_t> participant_timestamp;
  std::optional<double> price;
  std::optional<std::int64_t> sequence_number;
  std::optional<std::int64_t> sip_timestamp;
  std::optional<double> size; // crypto fractional sizes
  std::optional<int> tape;
};

struct V3TradesResp {
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<V3TradeRow> results;
  std::string status;
};

template <typename T>
Expected<T> makeError(int status, std::string_view message,
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

} // namespace

PolygonClient::PolygonClient(Options options) : options_(std::move(options)) {
  // Use the shared helper to get an event loop
  auto* loop = data_sdk::common::EventLoopHelper::getEventLoop(
      options_.use_drogon_main_loop,
      "PolygonClient",
      loopThread_);

  httpClient_ = drogon::HttpClient::newHttpClient(options_.base_url, loop);
}

PolygonClient::~PolygonClient() {
  data_sdk::common::EventLoopHelper::quitEventLoopThread(loopThread_);
}

std::optional<int>
PolygonClient::parseIntHeader(const drogon::HttpResponsePtr &resp,
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

std::string PolygonClient::buildQueryString(
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

auto PolygonClient::httpAsyncGet(
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

Expected<epoch_frame::DataFrame>
PolygonClient::getAggregates(const std::string &ticker,
                             const std::string &from_date,
                             const std::string &to_date, bool is_eod,
                             std::optional<bool> adjusted) const {

  std::vector<std::pair<std::string, std::string>> q;
  if (adjusted.has_value())
    q.emplace_back("adjusted", *adjusted ? "true" : "false");

  const int multiplier = 1;
  const std::string timespan = is_eod ? "day" : "minute";
  const std::string path = "/v2/aggs/ticker/" + ticker + "/range/" +
                           std::to_string(multiplier) + "/" + timespan + "/" +
                           from_date + "/" + to_date;

  auto bodyRes = httpGetWithRetry(path, q, 3);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  // Parse with glaze to extract results array, then let epoch_frame parse JSON
  AggregatesResponse parsed{};
  const std::string bodyStr = *bodyRes;
  if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
    SPDLOG_ERROR("Polygon getAggregates parse failed: ticker={} eod={} path={} "
                 "ec={} body_prefix={}",
                 ticker, is_eod, path, static_cast<int>(ec.ec),
                 bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
    return makeError<epoch_frame::DataFrame>(
        200, "Failed to parse JSON response", nullptr);
  }

  // Build DataFrame via loops and vectors; always include o,h,l,c,v,vw,n
  std::vector<std::int64_t> ts;
  std::vector<double> o, h, l, c, v, vw_vec;
  std::vector<std::int64_t> n_vec;
  const auto sz = parsed.results.size();
  ts.reserve(sz);
  o.reserve(sz);
  h.reserve(sz);
  l.reserve(sz);
  c.reserve(sz);
  v.reserve(sz);
  vw_vec.reserve(sz);
  n_vec.reserve(sz);

  // Choose conversion once per call
  const auto ts_converter =
      is_eod
          ? normalizeEodToMidnightUTC // EOD always normalized to midnight UTC
          : (isCryptoTicker(ticker) ? utcMsToNs : etMsToUtcNs);

  for (const auto &bar : parsed.results) {
    const auto ts_ns = ts_converter(bar.t);
    // Apply RTH filter only for stocks and only for intraday (minute) bars
    if (!is_eod && isStockTicker(ticker)) {
      if (!isWithinNYRth(ts_ns)) {
        continue;
      }
    }
    ts.push_back(ts_ns);
    o.push_back(bar.o);
    h.push_back(bar.h);
    l.push_back(bar.l);
    c.push_back(bar.c);
    v.push_back(bar.v);
    if (bar.vw.has_value()) {
      vw_vec.push_back(*bar.vw);
    }
    if (bar.n.has_value()) {
      n_vec.push_back(*bar.n);
    }
  }

  // Follow pagination if present
  std::string next = parsed.next_url.value_or("");
  int page_count = 1;
  while (!next.empty()) {
    SPDLOG_DEBUG("Polygon pagination: fetching page {} from {}", page_count + 1,
                 next);

    std::string next_path;
    std::vector<std::pair<std::string, std::string>> next_q;
    // Parse next_url into path and query
    if (auto qpos = next.find('?'); qpos != std::string::npos) {
      auto v2pos = next.find("/v2/");
      if (v2pos != std::string::npos) {
        next_path = next.substr(v2pos, qpos - v2pos);
      }
      auto query_str = next.substr(qpos + 1);
      std::stringstream ss(query_str);
      std::string kv;
      while (std::getline(ss, kv, '&')) {
        auto eq = kv.find('=');
        if (eq != std::string::npos) {
          next_q.emplace_back(kv.substr(0, eq), kv.substr(eq + 1));
        }
      }
    }

    auto bodyRes2 = httpGetWithRetry(next_path, next_q, 3);
    if (!bodyRes2) {
      SPDLOG_ERROR("Polygon pagination failed at page {} after retries: {}",
                   page_count + 1, bodyRes2.error().message);
      return makeError<epoch_frame::DataFrame>(
          bodyRes2.error().http_status,
          "Pagination failed: " + bodyRes2.error().message, nullptr);
    }

    AggregatesResponse page{};
    const std::string bodyStr2 = *bodyRes2;
    if (auto ec = glz::read_json(page, std::string_view(bodyStr2)); ec) {
      SPDLOG_ERROR("Polygon getAggregates page parse failed: page={} path={} "
                   "ec={} body_prefix={}",
                   page_count + 1, next_path, static_cast<int>(ec.ec),
                   bodyStr2.substr(0, std::min<size_t>(bodyStr2.size(), 256)));
      break;
    }

    for (const auto &bar : page.results) {
      const auto ts_ns = ts_converter(bar.t);
      if (!is_eod && isStockTicker(ticker)) {
        if (!isWithinNYRth(ts_ns)) {
          continue;
        }
      }
      ts.push_back(ts_ns);
      o.push_back(bar.o);
      h.push_back(bar.h);
      l.push_back(bar.l);
      c.push_back(bar.c);
      v.push_back(bar.v);
      if (bar.vw.has_value())
        vw_vec.push_back(*bar.vw);
      if (bar.n.has_value())
        n_vec.push_back(*bar.n);
    }
    next = page.next_url.value_or("");
    page_count++;
  }

  if (page_count > 1) {
    SPDLOG_INFO("Polygon aggregates: fetched {} pages for ticker={} eod={} "
                "total_bars={}",
                page_count, ticker, is_eod, ts.size());
  }

  std::vector<std::string> columns = {"o", "h", "l", "c", "v"};
  std::vector<arrow::ChunkedArrayPtr> data{
      epoch_frame::factory::array::make_array(o),
      epoch_frame::factory::array::make_array(h),
      epoch_frame::factory::array::make_array(l),
      epoch_frame::factory::array::make_array(c),
      epoch_frame::factory::array::make_array(v)};
  if (!vw_vec.empty()) {
    columns.push_back("vw");
    data.push_back(epoch_frame::factory::array::make_array(vw_vec));
  }
  if (!n_vec.empty()) {
    columns.push_back("n");
    data.push_back(epoch_frame::factory::array::make_array(n_vec));
  }

  auto index = epoch_frame::factory::index::make_datetime_index(ts, "", "UTC");
  auto df = epoch_frame::make_dataframe(index, data, columns);
  return df;
}

Expected<epoch_frame::DataFrame>
PolygonClient::getV3Quotes(const std::string &ticker, std::optional<int> limit,
                           std::optional<std::string> order,
                           std::optional<std::string> sort,
                           std::optional<std::string> timestamp_gte,
                           std::optional<std::string> timestamp_gt,
                           std::optional<std::string> timestamp_lte,
                           std::optional<std::string> timestamp_lt) const {

  std::vector<std::pair<std::string, std::string>> q;
  if (order.has_value())
    q.emplace_back("order", *order);
  if (sort.has_value())
    q.emplace_back("sort", *sort);
  if (limit.has_value())
    q.emplace_back("limit", std::to_string(*limit));
  if (timestamp_gte.has_value())
    q.emplace_back("timestamp.gte", *timestamp_gte);
  if (timestamp_gt.has_value())
    q.emplace_back("timestamp.gt", *timestamp_gt);
  if (timestamp_lte.has_value())
    q.emplace_back("timestamp.lte", *timestamp_lte);
  if (timestamp_lt.has_value())
    q.emplace_back("timestamp.lt", *timestamp_lt);

  const std::string path = "/v3/quotes/" + urlEncode(ticker);
  auto bodyRes = httpGet(path, q);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  V3QuotesResp parsed{};
  if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
    return makeError<epoch_frame::DataFrame>(
        200, "Failed to parse quotes JSON response", nullptr);
  }

  const auto N = parsed.results.size();
  std::vector<std::int64_t> t;        // participant_timestamp
  std::vector<double> ap, bp;         // ask_price, bid_price
  std::vector<std::int64_t> asz, bsz; // sizes
  std::vector<int> ax, bx;            // exchanges
  std::vector<std::int64_t> seq, sip;
  std::vector<int> tape;

  bool has_ap = false, has_bp = false, has_asz = false, has_bsz = false,
       has_ax = false, has_bx = false, has_seq = false, has_sip = false,
       has_tape = false;

  t.reserve(N);
  ap.reserve(N);
  bp.reserve(N);
  asz.reserve(N);
  bsz.reserve(N);
  ax.reserve(N);
  bx.reserve(N);
  seq.reserve(N);
  sip.reserve(N);
  tape.reserve(N);

  for (const auto &r : parsed.results) {
    const auto tt = r.participant_timestamp.value_or(0);
    // Quotes are UTC nanoseconds
    t.push_back(tt);

    const auto ap_v = r.ask_price.value_or(0.0);
    ap.push_back(ap_v);
    has_ap |= r.ask_price.has_value();
    const auto bp_v = r.bid_price.value_or(0.0);
    bp.push_back(bp_v);
    has_bp |= r.bid_price.has_value();
    const auto as_v = r.ask_size.value_or(0);
    asz.push_back(as_v);
    has_asz |= r.ask_size.has_value();
    const auto bs_v = r.bid_size.value_or(0);
    bsz.push_back(bs_v);
    has_bsz |= r.bid_size.has_value();
    const auto ax_v = r.ask_exchange.value_or(0);
    ax.push_back(ax_v);
    has_ax |= r.ask_exchange.has_value();
    const auto bx_v = r.bid_exchange.value_or(0);
    bx.push_back(bx_v);
    has_bx |= r.bid_exchange.has_value();
    const auto sq_v = r.sequence_number.value_or(0);
    seq.push_back(sq_v);
    has_seq |= r.sequence_number.has_value();
    const auto sp_v = r.sip_timestamp.value_or(0);
    sip.push_back(sp_v);
    has_sip |= r.sip_timestamp.has_value();
    const auto tp_v = r.tape.value_or(0);
    tape.push_back(tp_v);
    has_tape |= r.tape.has_value();
  }

  auto index = epoch_frame::factory::index::make_datetime_index(t, "", "UTC");

  std::vector<std::string> columns;
  std::vector<arrow::ChunkedArrayPtr> arrays;
  if (has_ap) {
    columns.push_back("ap");
    arrays.push_back(epoch_frame::factory::array::make_array(ap));
  }
  if (has_bp) {
    columns.push_back("bp");
    arrays.push_back(epoch_frame::factory::array::make_array(bp));
  }
  if (has_asz) {
    columns.push_back("asz");
    arrays.push_back(epoch_frame::factory::array::make_array(asz));
  }
  if (has_bsz) {
    columns.push_back("bsz");
    arrays.push_back(epoch_frame::factory::array::make_array(bsz));
  }
  if (has_ax) {
    columns.push_back("ax");
    arrays.push_back(epoch_frame::factory::array::make_array(ax));
  }
  if (has_bx) {
    columns.push_back("bx");
    arrays.push_back(epoch_frame::factory::array::make_array(bx));
  }
  if (has_seq) {
    columns.push_back("seq");
    arrays.push_back(epoch_frame::factory::array::make_array(seq));
  }
  if (has_sip) {
    columns.push_back("sip");
    arrays.push_back(epoch_frame::factory::array::make_array(sip));
  }
  if (has_tape) {
    columns.push_back("tape");
    arrays.push_back(epoch_frame::factory::array::make_array(tape));
  }

  return epoch_frame::make_dataframe(index, arrays, columns);
}

Expected<epoch_frame::DataFrame> PolygonClient::getQuotes(
    const std::string &ticker, const std::string &from_date,
    const std::string &to_date, std::optional<int> limit,
    std::optional<std::string> order, std::optional<std::string> sort) const {
  return getV3Quotes(ticker, limit, order, sort, from_date, std::nullopt,
                     to_date, std::nullopt);
}

Expected<epoch_frame::DataFrame> PolygonClient::getTrades(
    const std::string &ticker, const std::string &from_date,
    const std::string &to_date, std::optional<int> limit,
    std::optional<std::string> order, std::optional<std::string> sort) const {
  return getV3Trades(ticker, limit, order, sort, from_date, std::nullopt,
                     to_date, std::nullopt);
}

Expected<epoch_frame::DataFrame>
PolygonClient::getV3Trades(const std::string &ticker, std::optional<int> limit,
                           std::optional<std::string> order,
                           std::optional<std::string> sort,
                           std::optional<std::string> timestamp_gte,
                           std::optional<std::string> timestamp_gt,
                           std::optional<std::string> timestamp_lte,
                           std::optional<std::string> timestamp_lt) const {

  std::vector<std::pair<std::string, std::string>> q;
  if (order.has_value())
    q.emplace_back("order", *order);
  if (sort.has_value())
    q.emplace_back("sort", *sort);
  if (limit.has_value())
    q.emplace_back("limit", std::to_string(*limit));
  if (timestamp_gte.has_value())
    q.emplace_back("timestamp.gte", *timestamp_gte);
  if (timestamp_gt.has_value())
    q.emplace_back("timestamp.gt", *timestamp_gt);
  if (timestamp_lte.has_value())
    q.emplace_back("timestamp.lte", *timestamp_lte);
  if (timestamp_lt.has_value())
    q.emplace_back("timestamp.lt", *timestamp_lt);

  const std::string path = "/v3/trades/" + urlEncode(ticker);
  auto bodyRes = httpGet(path, q);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  V3TradesResp parsed{};
  if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
    return makeError<epoch_frame::DataFrame>(
        200, "Failed to parse trades JSON response", nullptr);
  }

  const auto N = parsed.results.size();
  std::vector<std::int64_t> t; // participant_timestamp
  std::vector<double> p, s;    // price, size
  std::vector<int> x;          // exchange
  std::vector<std::int64_t> seq, sip;
  std::vector<int> tape;

  bool has_p = false, has_s = false, has_x = false, has_seq = false,
       has_sip = false, has_tape = false;

  t.reserve(N);
  p.reserve(N);
  s.reserve(N);
  x.reserve(N);
  seq.reserve(N);
  sip.reserve(N);
  tape.reserve(N);

  for (const auto &r : parsed.results) {
    const auto tt = r.participant_timestamp.value_or(0);
    // Trades are UTC nanoseconds
    t.push_back(tt);
    const auto pv = r.price.value_or(0.0);
    p.push_back(pv);
    has_p |= r.price.has_value();
    const auto sv = r.size.value_or(0.0);
    s.push_back(sv);
    has_s |= r.size.has_value();
    const auto xv = r.exchange.value_or(0);
    x.push_back(xv);
    has_x |= r.exchange.has_value();
    const auto sq_v = r.sequence_number.value_or(0);
    seq.push_back(sq_v);
    has_seq |= r.sequence_number.has_value();
    const auto sp_v = r.sip_timestamp.value_or(0);
    sip.push_back(sp_v);
    has_sip |= r.sip_timestamp.has_value();
    const auto tp_v = r.tape.value_or(0);
    tape.push_back(tp_v);
    has_tape |= r.tape.has_value();
  }

  auto index = epoch_frame::factory::index::make_datetime_index(t, "", "UTC");
  std::vector<std::string> columns;
  std::vector<arrow::ChunkedArrayPtr> arrays;
  if (has_p) {
    columns.push_back("p");
    arrays.push_back(epoch_frame::factory::array::make_array(p));
  }
  if (has_s) {
    columns.push_back("s");
    arrays.push_back(epoch_frame::factory::array::make_array(s));
  }
  if (has_x) {
    columns.push_back("x");
    arrays.push_back(epoch_frame::factory::array::make_array(x));
  }
  if (has_seq) {
    columns.push_back("seq");
    arrays.push_back(epoch_frame::factory::array::make_array(seq));
  }
  if (has_sip) {
    columns.push_back("sip");
    arrays.push_back(epoch_frame::factory::array::make_array(sip));
  }
  if (has_tape) {
    columns.push_back("tape");
    arrays.push_back(epoch_frame::factory::array::make_array(tape));
  }

  return epoch_frame::make_dataframe(index, arrays, columns);
}

Expected<std::string> PolygonClient::httpGetWithRetry(
    const std::string &path,
    const std::vector<std::pair<std::string, std::string>> &query,
    int max_retries) const {
  for (int attempt = 0; attempt <= max_retries; ++attempt) {
    auto result = httpGet(path, query);
    if (result) {
      return result;
    }

    // Log retry attempt
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

Expected<epoch_frame::DataFrame>
PolygonClient::getBalanceSheets(std::optional<std::string> cik,
                                std::optional<std::string> period_end,
                                std::optional<std::string> tickers,
                                std::optional<int> fiscal_year,
                                std::optional<int> fiscal_quarter,
                                std::optional<std::string> timeframe,
                                std::optional<int> limit,
                                std::optional<std::string> sort) const {
  std::vector<std::pair<std::string, std::string>> q;
  if (cik.has_value())
    q.emplace_back("cik", *cik);
  if (period_end.has_value())
    q.emplace_back("period_end", *period_end);
  if (tickers.has_value())
    q.emplace_back("tickers", *tickers);
  if (fiscal_year.has_value())
    q.emplace_back("fiscal_year", std::to_string(*fiscal_year));
  if (fiscal_quarter.has_value())
    q.emplace_back("fiscal_quarter", std::to_string(*fiscal_quarter));
  if (timeframe.has_value())
    q.emplace_back("timeframe", *timeframe);
  if (limit.has_value())
    q.emplace_back("limit", std::to_string(*limit));
  if (sort.has_value())
    q.emplace_back("sort", *sort);

  const std::string path = "/stocks/financials/v1/balance-sheets";
  auto bodyRes = httpGetWithRetry(path, q, 3);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  BalanceSheetsResponse parsed{};
  if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
    return makeError<epoch_frame::DataFrame>(
        200, "Failed to parse balance sheets JSON response", nullptr);
  }

  std::vector<std::string> period_ends;
  std::vector<std::string> tickers_col;
  std::vector<std::string> ciks;
  std::vector<std::int64_t> fiscal_years, fiscal_quarters;
  std::vector<std::string> timeframes;
  std::vector<double> accounts_payable, accrued_liab, aoci, cash, debt_current,
      deferred_rev, inventories, lt_debt, ppe_net, receivables, retained_earn;

  for (const auto &r : parsed.results) {
    period_ends.push_back(r.period_end.value_or(""));
    tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
    ciks.push_back(r.cik.value_or(""));
    fiscal_years.push_back(r.fiscal_year.value_or(0));
    fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
    timeframes.push_back(r.timeframe.value_or(""));
    accounts_payable.push_back(r.accounts_payable.value_or(0.0));
    accrued_liab.push_back(
        r.accrued_and_other_current_liabilities.value_or(0.0));
    aoci.push_back(r.accumulated_other_comprehensive_income.value_or(0.0));
    cash.push_back(r.cash_and_equivalents.value_or(0.0));
    debt_current.push_back(r.debt_current.value_or(0.0));
    deferred_rev.push_back(r.deferred_revenue_current.value_or(0.0));
    inventories.push_back(r.inventories.value_or(0.0));
    lt_debt.push_back(r.long_term_debt_and_capital_lease_obligations.value_or(0.0));
    ppe_net.push_back(r.property_plant_equipment_net.value_or(0.0));
    receivables.push_back(r.receivables.value_or(0.0));
    retained_earn.push_back(r.retained_earnings_deficit.value_or(0.0));
  }

  auto index = epoch_frame::factory::index::make_index(
      epoch_frame::factory::array::make_array(period_ends),
      epoch_frame::MonotonicDirection::NotMonotonic, "period_end");
  std::vector<std::string> columns = {"ticker", "cik", "fiscal_year", "fiscal_quarter", "timeframe",
                                      "accounts_payable", "accrued_liabilities", "aoci", "cash",
                                      "debt_current", "deferred_revenue", "inventories", "lt_debt",
                                      "ppe_net", "receivables", "retained_earnings"};
  std::vector<arrow::ChunkedArrayPtr> data{
      epoch_frame::factory::array::make_array(tickers_col),
      epoch_frame::factory::array::make_array(ciks),
      epoch_frame::factory::array::make_array(fiscal_years),
      epoch_frame::factory::array::make_array(fiscal_quarters),
      epoch_frame::factory::array::make_array(timeframes),
      epoch_frame::factory::array::make_array(accounts_payable),
      epoch_frame::factory::array::make_array(accrued_liab),
      epoch_frame::factory::array::make_array(aoci),
      epoch_frame::factory::array::make_array(cash),
      epoch_frame::factory::array::make_array(debt_current),
      epoch_frame::factory::array::make_array(deferred_rev),
      epoch_frame::factory::array::make_array(inventories),
      epoch_frame::factory::array::make_array(lt_debt),
      epoch_frame::factory::array::make_array(ppe_net),
      epoch_frame::factory::array::make_array(receivables),
      epoch_frame::factory::array::make_array(retained_earn)};

  return epoch_frame::make_dataframe(index, data, columns);
}

Expected<epoch_frame::DataFrame> PolygonClient::getCashFlowStatements(
    std::optional<std::string> cik, std::optional<std::string> period_end,
    std::optional<std::string> tickers, std::optional<int> fiscal_year,
    std::optional<int> fiscal_quarter, std::optional<std::string> timeframe,
    std::optional<int> limit, std::optional<std::string> sort) const {
  std::vector<std::pair<std::string, std::string>> q;
  if (cik.has_value())
    q.emplace_back("cik", *cik);
  if (period_end.has_value())
    q.emplace_back("period_end", *period_end);
  if (tickers.has_value())
    q.emplace_back("tickers", *tickers);
  if (fiscal_year.has_value())
    q.emplace_back("fiscal_year", std::to_string(*fiscal_year));
  if (fiscal_quarter.has_value())
    q.emplace_back("fiscal_quarter", std::to_string(*fiscal_quarter));
  if (timeframe.has_value())
    q.emplace_back("timeframe", *timeframe);
  if (limit.has_value())
    q.emplace_back("limit", std::to_string(*limit));
  if (sort.has_value())
    q.emplace_back("sort", *sort);

  const std::string path = "/stocks/financials/v1/cash-flow-statements";
  auto bodyRes = httpGetWithRetry(path, q, 3);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  CashFlowStatementsResponse parsed{};
  if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
    return makeError<epoch_frame::DataFrame>(
        200, "Failed to parse cash flow statements JSON response", nullptr);
  }

  std::vector<std::string> period_ends, tickers_col, ciks, timeframes;
  std::vector<std::int64_t> fiscal_years, fiscal_quarters;
  std::vector<double> cfo, change_cash, change_assets, dda, dividends,
      lt_debt_iss, ncf_fin, ncf_inv, ncf_oper, net_income, capex;

  for (const auto &r : parsed.results) {
    period_ends.push_back(r.period_end.value_or(""));
    tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
    ciks.push_back(r.cik.value_or(""));
    fiscal_years.push_back(r.fiscal_year.value_or(0));
    fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
    timeframes.push_back(r.timeframe.value_or(""));
    cfo.push_back(r.cash_from_operating_activities_continuing_operations.value_or(0.0));
    change_cash.push_back(r.change_in_cash_and_equivalents.value_or(0.0));
    change_assets.push_back(r.change_in_other_operating_assets_and_liabilities_net.value_or(0.0));
    dda.push_back(r.depreciation_depletion_and_amortization.value_or(0.0));
    dividends.push_back(r.dividends.value_or(0.0));
    lt_debt_iss.push_back(r.long_term_debt_issuances_repayments.value_or(0.0));
    ncf_fin.push_back(r.net_cash_from_financing_activities.value_or(0.0));
    ncf_inv.push_back(r.net_cash_from_investing_activities.value_or(0.0));
    ncf_oper.push_back(r.net_cash_from_operating_activities.value_or(0.0));
    net_income.push_back(r.net_income.value_or(0.0));
    capex.push_back(r.purchase_of_property_plant_and_equipment.value_or(0.0));
  }

  auto index = epoch_frame::factory::index::make_index(
      epoch_frame::factory::array::make_array(period_ends),
      epoch_frame::MonotonicDirection::NotMonotonic, "period_end");
  std::vector<std::string> columns = {"ticker", "cik", "fiscal_year", "fiscal_quarter", "timeframe",
                                      "cfo", "change_cash", "change_assets", "dda", "dividends",
                                      "lt_debt_issuances", "ncf_financing", "ncf_investing",
                                      "ncf_operating", "net_income", "capex"};
  std::vector<arrow::ChunkedArrayPtr> data{
      epoch_frame::factory::array::make_array(tickers_col),
      epoch_frame::factory::array::make_array(ciks),
      epoch_frame::factory::array::make_array(fiscal_years),
      epoch_frame::factory::array::make_array(fiscal_quarters),
      epoch_frame::factory::array::make_array(timeframes),
      epoch_frame::factory::array::make_array(cfo),
      epoch_frame::factory::array::make_array(change_cash),
      epoch_frame::factory::array::make_array(change_assets),
      epoch_frame::factory::array::make_array(dda),
      epoch_frame::factory::array::make_array(dividends),
      epoch_frame::factory::array::make_array(lt_debt_iss),
      epoch_frame::factory::array::make_array(ncf_fin),
      epoch_frame::factory::array::make_array(ncf_inv),
      epoch_frame::factory::array::make_array(ncf_oper),
      epoch_frame::factory::array::make_array(net_income),
      epoch_frame::factory::array::make_array(capex)};

  return epoch_frame::make_dataframe(index, data, columns);
}

Expected<epoch_frame::DataFrame> PolygonClient::getIncomeStatements(
    std::optional<std::string> cik, std::optional<std::string> period_end,
    std::optional<std::string> tickers, std::optional<int> fiscal_year,
    std::optional<int> fiscal_quarter, std::optional<std::string> timeframe,
    std::optional<int> limit, std::optional<std::string> sort) const {
  std::vector<std::pair<std::string, std::string>> q;
  if (cik.has_value())
    q.emplace_back("cik", *cik);
  if (period_end.has_value())
    q.emplace_back("period_end", *period_end);
  if (tickers.has_value())
    q.emplace_back("tickers", *tickers);
  if (fiscal_year.has_value())
    q.emplace_back("fiscal_year", std::to_string(*fiscal_year));
  if (fiscal_quarter.has_value())
    q.emplace_back("fiscal_quarter", std::to_string(*fiscal_quarter));
  if (timeframe.has_value())
    q.emplace_back("timeframe", *timeframe);
  if (limit.has_value())
    q.emplace_back("limit", std::to_string(*limit));
  if (sort.has_value())
    q.emplace_back("sort", *sort);

  const std::string path = "/stocks/financials/v1/income-statements";
  auto bodyRes = httpGetWithRetry(path, q, 3);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  IncomeStatementsResponse parsed{};
  if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
    return makeError<epoch_frame::DataFrame>(
        200, "Failed to parse income statements JSON response", nullptr);
  }

  std::vector<std::string> period_ends, tickers_col, ciks, timeframes;
  std::vector<std::int64_t> fiscal_years, fiscal_quarters;
  std::vector<double> basic_eps, diluted_eps, revenue, cogs, gross_profit,
      operating_income, net_income, rd, sga;

  for (const auto &r : parsed.results) {
    period_ends.push_back(r.period_end.value_or(""));
    tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
    ciks.push_back(r.cik.value_or(""));
    fiscal_years.push_back(r.fiscal_year.value_or(0));
    fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
    timeframes.push_back(r.timeframe.value_or(""));
    basic_eps.push_back(r.basic_earnings_per_share.value_or(0.0));
    diluted_eps.push_back(r.diluted_earnings_per_share.value_or(0.0));
    revenue.push_back(r.revenue.value_or(0.0));
    cogs.push_back(r.cost_of_revenue.value_or(0.0));
    gross_profit.push_back(r.gross_profit.value_or(0.0));
    operating_income.push_back(r.operating_income.value_or(0.0));
    net_income.push_back(r.consolidated_net_income_loss.value_or(0.0));
    rd.push_back(r.research_development.value_or(0.0));
    sga.push_back(r.selling_general_administrative.value_or(0.0));
  }

  auto index = epoch_frame::factory::index::make_index(
      epoch_frame::factory::array::make_array(period_ends),
      epoch_frame::MonotonicDirection::NotMonotonic, "period_end");
  std::vector<std::string> columns = {"ticker", "cik", "fiscal_year", "fiscal_quarter", "timeframe",
                                      "basic_eps", "diluted_eps", "revenue", "cogs", "gross_profit",
                                      "operating_income", "net_income", "rd", "sga"};
  std::vector<arrow::ChunkedArrayPtr> data{
      epoch_frame::factory::array::make_array(tickers_col),
      epoch_frame::factory::array::make_array(ciks),
      epoch_frame::factory::array::make_array(fiscal_years),
      epoch_frame::factory::array::make_array(fiscal_quarters),
      epoch_frame::factory::array::make_array(timeframes),
      epoch_frame::factory::array::make_array(basic_eps),
      epoch_frame::factory::array::make_array(diluted_eps),
      epoch_frame::factory::array::make_array(revenue),
      epoch_frame::factory::array::make_array(cogs),
      epoch_frame::factory::array::make_array(gross_profit),
      epoch_frame::factory::array::make_array(operating_income),
      epoch_frame::factory::array::make_array(net_income),
      epoch_frame::factory::array::make_array(rd),
      epoch_frame::factory::array::make_array(sga)};

  return epoch_frame::make_dataframe(index, data, columns);
}

Expected<epoch_frame::DataFrame> PolygonClient::getFinancialRatios(
    std::optional<std::string> ticker, std::optional<std::string> cik,
    std::optional<double> price, std::optional<double> average_volume,
    std::optional<double> market_cap, std::optional<double> earnings_per_share,
    std::optional<double> price_to_earnings, std::optional<double> price_to_book,
    std::optional<double> price_to_sales, std::optional<int> limit,
    std::optional<std::string> sort) const {
  std::vector<std::pair<std::string, std::string>> q;
  if (ticker.has_value())
    q.emplace_back("ticker", *ticker);
  if (cik.has_value())
    q.emplace_back("cik", *cik);
  if (price.has_value())
    q.emplace_back("price", std::to_string(*price));
  if (average_volume.has_value())
    q.emplace_back("average_volume", std::to_string(*average_volume));
  if (market_cap.has_value())
    q.emplace_back("market_cap", std::to_string(*market_cap));
  if (earnings_per_share.has_value())
    q.emplace_back("earnings_per_share", std::to_string(*earnings_per_share));
  if (price_to_earnings.has_value())
    q.emplace_back("price_to_earnings", std::to_string(*price_to_earnings));
  if (price_to_book.has_value())
    q.emplace_back("price_to_book", std::to_string(*price_to_book));
  if (price_to_sales.has_value())
    q.emplace_back("price_to_sales", std::to_string(*price_to_sales));
  if (limit.has_value())
    q.emplace_back("limit", std::to_string(*limit));
  if (sort.has_value())
    q.emplace_back("sort", *sort);

  const std::string path = "/stocks/financials/v1/ratios";
  auto bodyRes = httpGetWithRetry(path, q, 3);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

  FinancialRatiosResponse parsed{};
  if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
    return makeError<epoch_frame::DataFrame>(
        200, "Failed to parse financial ratios JSON response", nullptr);
  }

  std::vector<std::string> dates, tickers_col, ciks;
  std::vector<double> avg_vol, cash_ratio, current_ratio, debt_eq, div_yield,
      eps, ev, ev_ebitda, ev_sales, fcf, mkt_cap, price_val, ptb, ptcf, pte,
      ptfcf, pts, quick_ratio, roa, roe;

  for (const auto &r : parsed.results) {
    dates.push_back(r.date.value_or(""));
    tickers_col.push_back(r.ticker.value_or(""));
    ciks.push_back(r.cik.value_or(""));
    avg_vol.push_back(r.average_volume.value_or(0.0));
    cash_ratio.push_back(r.cash.value_or(0.0));
    current_ratio.push_back(r.current.value_or(0.0));
    debt_eq.push_back(r.debt_to_equity.value_or(0.0));
    div_yield.push_back(r.dividend_yield.value_or(0.0));
    eps.push_back(r.earnings_per_share.value_or(0.0));
    ev.push_back(r.enterprise_value.value_or(0.0));
    ev_ebitda.push_back(r.ev_to_ebitda.value_or(0.0));
    ev_sales.push_back(r.ev_to_sales.value_or(0.0));
    fcf.push_back(r.free_cash_flow.value_or(0.0));
    mkt_cap.push_back(r.market_cap.value_or(0.0));
    price_val.push_back(r.price.value_or(0.0));
    ptb.push_back(r.price_to_book.value_or(0.0));
    ptcf.push_back(r.price_to_cash_flow.value_or(0.0));
    pte.push_back(r.price_to_earnings.value_or(0.0));
    ptfcf.push_back(r.price_to_free_cash_flow.value_or(0.0));
    pts.push_back(r.price_to_sales.value_or(0.0));
    quick_ratio.push_back(r.quick.value_or(0.0));
    roa.push_back(r.return_on_assets.value_or(0.0));
    roe.push_back(r.return_on_equity.value_or(0.0));
  }

  auto index = epoch_frame::factory::index::make_index(
      epoch_frame::factory::array::make_array(dates),
      epoch_frame::MonotonicDirection::NotMonotonic, "date");
  std::vector<std::string> columns = {
      "ticker", "cik", "avg_volume", "cash", "current", "debt_to_equity",
      "div_yield", "eps", "ev", "ev_ebitda", "ev_sales", "fcf", "market_cap",
      "price", "pb", "pcf", "pe", "pfcf", "ps", "quick", "roa", "roe"};
  std::vector<arrow::ChunkedArrayPtr> data{
      epoch_frame::factory::array::make_array(tickers_col),
      epoch_frame::factory::array::make_array(ciks),
      epoch_frame::factory::array::make_array(avg_vol),
      epoch_frame::factory::array::make_array(cash_ratio),
      epoch_frame::factory::array::make_array(current_ratio),
      epoch_frame::factory::array::make_array(debt_eq),
      epoch_frame::factory::array::make_array(div_yield),
      epoch_frame::factory::array::make_array(eps),
      epoch_frame::factory::array::make_array(ev),
      epoch_frame::factory::array::make_array(ev_ebitda),
      epoch_frame::factory::array::make_array(ev_sales),
      epoch_frame::factory::array::make_array(fcf),
      epoch_frame::factory::array::make_array(mkt_cap),
      epoch_frame::factory::array::make_array(price_val),
      epoch_frame::factory::array::make_array(ptb),
      epoch_frame::factory::array::make_array(ptcf),
      epoch_frame::factory::array::make_array(pte),
      epoch_frame::factory::array::make_array(ptfcf),
      epoch_frame::factory::array::make_array(pts),
      epoch_frame::factory::array::make_array(quick_ratio),
      epoch_frame::factory::array::make_array(roa),
      epoch_frame::factory::array::make_array(roe)};

  return epoch_frame::make_dataframe(index, data, columns);
}

} // namespace data_sdk::polygon
