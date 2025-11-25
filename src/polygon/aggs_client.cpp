#include "polygon/aggs_client.hpp"

#include <chrono>
#include <sstream>

#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "polygon/models.hpp"

namespace data_sdk::polygon {

namespace {

// Shared accessor for America/New_York time zone (handles DST correctly)
inline const std::chrono::time_zone *ny_time_zone() {
  static const std::chrono::time_zone *tz =
      std::chrono::locate_zone("America/New_York");
  return tz;
}

// Ticker type detection
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

// Timestamp converters
inline std::int64_t utcMsToNs(std::int64_t ts_ms) {
  return ts_ms * 1'000'000LL;
}

// Convert milliseconds since local ET epoch to UTC nanoseconds
inline std::int64_t etMsToUtcNs(std::int64_t ts_ms) {
  using namespace std::chrono;
  try {
    const time_zone *ny = ny_time_zone();
    local_time<milliseconds> local_et{milliseconds{ts_ms}};
    zoned_time<milliseconds> zt{ny, local_et};
    const sys_time<milliseconds> sys = zt.get_sys_time();
    auto ttc = sys.time_since_epoch().count();
    return static_cast<int64_t>(ttc * 1'000'000LL);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Timezone database error for ET->UTC conversion (falling back to UTC): {} - timestamp: {}", e.what(), ts_ms);
    // Fallback if timezone database is unavailable: treat as UTC ms
    return ts_ms * 1'000'000LL;
  }
}

// Normalize EOD timestamp to midnight UTC (00:00:00)
inline std::int64_t normalizeEodToMidnightUTC(std::int64_t ts_ms) {
  using namespace std::chrono;
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
  } catch (const std::exception& e) {
    SPDLOG_WARN("Timezone check failed for NY RTH filtering (including bar): {} - timestamp: {}", e.what(), ts_utc_ns);
    // If tz DB unavailable, do not filter out
    return true;
  }
}

} // namespace

Expected<epoch_frame::DataFrame>
AggsClient::getAggregates(const std::string &ticker, const std::string &from_date,
                          const std::string &to_date, bool is_eod,
                          std::optional<bool> adjusted) const {

  std::vector<std::pair<std::string, std::string>> q;
  if (adjusted.has_value())
    q.emplace_back("adjusted", *adjusted ? "true" : "false");

  // Always fetch in ascending chronological order for backtesting consistency
  q.emplace_back("sort", "asc");

  const int multiplier = 1;
  const std::string timespan = is_eod ? "day" : "minute";
  const std::string path = "/v2/aggs/ticker/" + ticker + "/range/" +
                           std::to_string(multiplier) + "/" + timespan + "/" +
                           from_date + "/" + to_date;

  auto bodyRes = httpGetWithRetry(path, q, 3);
  if (!bodyRes)
    return std::unexpected(bodyRes.error());

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

  // Build DataFrame via loops and vectors
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
          ? normalizeEodToMidnightUTC
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

drogon::Task<Expected<epoch_frame::DataFrame>>
AggsClient::getAggregatesAsync(std::string ticker, std::string from_date,
                               std::string to_date, bool is_eod,
                               std::optional<bool> adjusted) const {

  // Parameters are passed by value to avoid coroutine lifetime issues
  SPDLOG_WARN("getAggregatesAsync START: ticker={}", ticker);

  std::vector<std::pair<std::string, std::string>> q;
  if (adjusted.has_value())
    q.emplace_back("adjusted", *adjusted ? "true" : "false");

  // Always fetch in ascending chronological order for backtesting consistency
  q.emplace_back("sort", "asc");

  const int multiplier = 1;
  const std::string timespan = is_eod ? "day" : "minute";
  const std::string path = "/v2/aggs/ticker/" + ticker + "/range/" +
                           std::to_string(multiplier) + "/" + timespan + "/" +
                           from_date + "/" + to_date;

  // First request (async with retry)
  SPDLOG_WARN("getAggregatesAsync BEFORE CO_AWAIT: ticker={}, path={}", ticker, path);

  auto bodyRes = co_await httpAsyncGetWithRetry(path, q, options().max_retries);
  if (!bodyRes)
    co_return std::unexpected(bodyRes.error());

  AggregatesResponse parsed{};
  const std::string bodyStr = *bodyRes;
  if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
    SPDLOG_ERROR("Polygon getAggregatesAsync parse failed: ticker={} eod={} path={} "
                 "ec={} body_prefix={}",
                 ticker, is_eod, path, static_cast<int>(ec.ec),
                 bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
    co_return makeError<epoch_frame::DataFrame>(
        200, "Failed to parse JSON response", nullptr);
  }

  // Build DataFrame via loops and vectors
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
          ? normalizeEodToMidnightUTC
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

  // Follow pagination if present (async)
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

    auto bodyRes2 = co_await httpAsyncGetWithRetry(next_path, next_q, options().max_retries);
    if (!bodyRes2) {
      SPDLOG_ERROR("Polygon pagination failed at page {} after {} retries: {}",
                   page_count + 1, options().max_retries, bodyRes2.error().message);
      co_return makeError<epoch_frame::DataFrame>(
          bodyRes2.error().http_status,
          "Pagination failed: " + bodyRes2.error().message, nullptr);
    }

    AggregatesResponse page{};
    const std::string bodyStr2 = *bodyRes2;
    if (auto ec = glz::read_json(page, std::string_view(bodyStr2)); ec) {
      SPDLOG_ERROR("Polygon getAggregatesAsync page parse failed: page={} path={} "
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
  co_return df;
}

data_sdk::DataFrameMetadata AggsClient::getMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "aggregates",
      .description = "Retrieve historical OHLCV (Open, High, Low, Close, Volume) aggregate bars for stocks, forex (C: prefix), crypto (X: prefix), and indices. Supports both daily (end-of-day) and intraday (minute-level) data with optional split/dividend adjustments. For stocks, minute bars are automatically filtered to NYSE Regular Trading Hours (09:31-16:00 ET). Data is returned in ascending chronological order. Use Cases: Backtesting trading strategies, technical analysis, portfolio performance analysis, and quantitative research requiring price/volume time series.",
      .asset_class = std::nullopt,  // Multiple asset classes supported (Stocks, Crypto, FX, Indices)
      .index_normalized = false,  // Minute bars have time-of-day (intraday timestamps)
      .category_prefix = "",  // Empty prefix for timeseries data
      .columns = {
          {.id = "o",
           .name = "Open",
           .description = "Opening price of the bar",
           .type = ArrowType::FLOAT64,
           .nullable = false},
          {.id = "h",
           .name = "High",
           .description = "Highest price during the bar period",
           .type = ArrowType::FLOAT64,
           .nullable = false},
          {.id = "l",
           .name = "Low",
           .description = "Lowest price during the bar period",
           .type = ArrowType::FLOAT64,
           .nullable = false},
          {.id = "c",
           .name = "Close",
           .description = "Closing price of the bar",
           .type = ArrowType::FLOAT64,
           .nullable = false},
          {.id = "v",
           .name = "Volume",
           .description = "Total trading volume during the bar period",
           .type = ArrowType::FLOAT64,
           .nullable = false},
          {.id = "vw",
           .name = "Volume Weighted Average Price",
           .description = "Volume weighted average price during the bar period (VWAP). Optional field, may not be present for all tickers or time periods.",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "n",
           .name = "Number of Trades",
           .description = "Number of individual trades aggregated into this bar. Optional field, may not be present for all tickers or time periods.",
           .type = ArrowType::INT64,
           .nullable = true},
      }};
}

} // namespace data_sdk::polygon
