#include "polygon/short_volume_client.hpp"

#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>

#include <arrow/compute/api.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "polygon/base_client.hpp"
#include "polygon/models.hpp"

namespace data_sdk::polygon {

namespace {
// Helper to parse date strings (YYYY-MM-DD) to nanoseconds since epoch using Arrow
std::vector<std::int64_t> parseDatesToNs(const std::vector<std::string> &date_strings) {
  if (date_strings.empty()) return {};

  // Build Arrow StringArray from input strings
  arrow::StringBuilder builder;
  auto status = builder.AppendValues(date_strings);
  if (!status.ok()) {
    SPDLOG_ERROR("Failed to build StringArray for date parsing: {}", status.message());
    return std::vector<std::int64_t>(date_strings.size(), 0);
  }

  auto maybe_array = builder.Finish();
  if (!maybe_array.ok()) {
    SPDLOG_ERROR("Failed to finish StringArray: {}", maybe_array.status().message());
    return std::vector<std::int64_t>(date_strings.size(), 0);
  }

  // Parse strings to timestamps using Arrow compute strptime
  arrow::compute::StrptimeOptions options("%Y-%m-%d", arrow::TimeUnit::NANO, false);
  auto maybe_result = arrow::compute::CallFunction("strptime", {maybe_array.ValueOrDie()}, &options);
  if (!maybe_result.ok()) {
    SPDLOG_ERROR("Failed to parse dates with strptime: {}", maybe_result.status().message());
    return std::vector<std::int64_t>(date_strings.size(), 0);
  }

  // Extract nanosecond values from TimestampArray
  auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(maybe_result.ValueOrDie().make_array());
  std::vector<std::int64_t> result;
  result.reserve(timestamp_array->length());
  for (int64_t i = 0; i < timestamp_array->length(); ++i) {
    if (timestamp_array->IsNull(i)) {
      result.push_back(0);
    } else {
      result.push_back(timestamp_array->Value(i));
    }
  }

  return result;
}
} // namespace

// Private implementation
class ShortVolumeClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getShortVolume(const std::string &ticker, const std::string &date_from,
                 const std::string &date_to, std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("ticker", ticker);
    q.emplace_back("date.gte", date_from);
    q.emplace_back("date.lte", date_to);
    // Always fetch in ascending order for backtesting consistency
    q.emplace_back("order", "asc");
    q.emplace_back("sort", "date");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/v1/short-volume";
    auto bodyRes = httpGetWithRetry(path, q, 3);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    ShortVolumeResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getShortVolume parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse JSON response", nullptr);
    }

    // Build DataFrame
    std::vector<std::string> date_strings;
    std::vector<std::string> tickers_col;
    std::vector<int> short_volume, total_volume, exempt_volume, non_exempt_volume;
    std::vector<double> short_volume_ratio;

    const auto sz = parsed.results.size();
    date_strings.reserve(sz);
    tickers_col.reserve(sz);
    short_volume.reserve(sz);
    total_volume.reserve(sz);
    exempt_volume.reserve(sz);
    non_exempt_volume.reserve(sz);
    short_volume_ratio.reserve(sz);

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.date.value_or(""));
      tickers_col.push_back(r.ticker.value_or(""));
      short_volume.push_back(r.short_volume.value_or(0));
      total_volume.push_back(r.total_volume.value_or(0));
      exempt_volume.push_back(r.exempt_volume.value_or(0));
      non_exempt_volume.push_back(r.non_exempt_volume.value_or(0));
      short_volume_ratio.push_back(r.short_volume_ratio.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon short_volume pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = httpGetWithRetry(parsed_url->path, parsed_url->query, 3);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon short_volume pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      ShortVolumeResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon short_volume page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.date.value_or(""));
        tickers_col.push_back(r.ticker.value_or(""));
        short_volume.push_back(r.short_volume.value_or(0));
        total_volume.push_back(r.total_volume.value_or(0));
        exempt_volume.push_back(r.exempt_volume.value_or(0));
        non_exempt_volume.push_back(r.non_exempt_volume.value_or(0));
        short_volume_ratio.push_back(r.short_volume_ratio.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon short_volume: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "short_volume", "total_volume",
                                         "short_volume_ratio", "exempt_volume",
                                         "non_exempt_volume"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(short_volume),
        epoch_frame::factory::array::make_array(total_volume),
        epoch_frame::factory::array::make_array(short_volume_ratio),
        epoch_frame::factory::array::make_array(exempt_volume),
        epoch_frame::factory::array::make_array(non_exempt_volume)};

    return epoch_frame::make_dataframe(index, data, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getShortVolumeAsync(std::string ticker, std::string date_from,
                      std::string date_to, std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("ticker", ticker);
    q.emplace_back("date.gte", date_from);
    q.emplace_back("date.lte", date_to);
    // Always fetch in ascending order for backtesting consistency
    q.emplace_back("order", "asc");
    q.emplace_back("sort", "date");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/v1/short-volume";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    ShortVolumeResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getShortVolumeAsync parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse JSON response", nullptr);
    }

    // Build DataFrame
    std::vector<std::string> date_strings;
    std::vector<std::string> tickers_col;
    std::vector<int> short_volume, total_volume, exempt_volume, non_exempt_volume;
    std::vector<double> short_volume_ratio;

    const auto sz = parsed.results.size();
    date_strings.reserve(sz);
    tickers_col.reserve(sz);
    short_volume.reserve(sz);
    total_volume.reserve(sz);
    exempt_volume.reserve(sz);
    non_exempt_volume.reserve(sz);
    short_volume_ratio.reserve(sz);

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.date.value_or(""));
      tickers_col.push_back(r.ticker.value_or(""));
      short_volume.push_back(r.short_volume.value_or(0));
      total_volume.push_back(r.total_volume.value_or(0));
      exempt_volume.push_back(r.exempt_volume.value_or(0));
      non_exempt_volume.push_back(r.non_exempt_volume.value_or(0));
      short_volume_ratio.push_back(r.short_volume_ratio.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon short_volume Async pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = co_await httpAsyncGet(parsed_url->path, parsed_url->query);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon short_volume Async pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      ShortVolumeResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon short_volume Async page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.date.value_or(""));
        tickers_col.push_back(r.ticker.value_or(""));
        short_volume.push_back(r.short_volume.value_or(0));
        total_volume.push_back(r.total_volume.value_or(0));
        exempt_volume.push_back(r.exempt_volume.value_or(0));
        non_exempt_volume.push_back(r.non_exempt_volume.value_or(0));
        short_volume_ratio.push_back(r.short_volume_ratio.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon short_volume Async: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "short_volume", "total_volume",
                                         "short_volume_ratio", "exempt_volume",
                                         "non_exempt_volume"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(short_volume),
        epoch_frame::factory::array::make_array(total_volume),
        epoch_frame::factory::array::make_array(short_volume_ratio),
        epoch_frame::factory::array::make_array(exempt_volume),
        epoch_frame::factory::array::make_array(non_exempt_volume)};

    co_return epoch_frame::make_dataframe(index, data, columns);
  }
};

// Public API
ShortVolumeClient::ShortVolumeClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

ShortVolumeClient::~ShortVolumeClient() = default;

Expected<epoch_frame::DataFrame>
ShortVolumeClient::getShortVolume(const std::string &ticker,
                                   const std::string &date_from,
                                   const std::string &date_to,
                                   std::optional<int> limit) const {
  return impl_->getShortVolume(ticker, date_from, date_to, limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
ShortVolumeClient::getShortVolumeAsync(std::string ticker,
                                        std::string date_from,
                                        std::string date_to,
                                        std::optional<int> limit) const {
  return impl_->getShortVolumeAsync(std::move(ticker), std::move(date_from),
                                    std::move(date_to), limit);
}

data_sdk::DataFrameMetadata ShortVolumeClient::getMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "short_volume",
      .description = "Retrieve daily aggregated short sale volume data reported to FINRA from off-exchange trading venues and alternative trading systems (ATS). Unlike short interest metrics that measure outstanding positions at specific intervals, this endpoint captures daily trading activity of short sales, helping analysts detect market sentiment shifts and identify short-selling trends. Use cases include intraday sentiment analysis, short-sale trend identification, liquidity analysis, and trading strategy optimization. Available across all Stocks plans with data updated daily and historical availability ranging from 2 years (Basic) to all history (Starter and above).",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,
      .category_prefix = "SV:",
      .columns = {
          {.id = "ticker",
           .name = "Ticker",
           .description = "Stock symbol identifier",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "short_volume",
           .name = "Short Volume",
           .description = "Total number of shares sold short across all reporting venues during the trading date",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "total_volume",
           .name = "Total Volume",
           .description = "Total reported trading volume for the date across all venues",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "short_volume_ratio",
           .name = "Short Volume Ratio",
           .description = "Percentage of total volume that was sold short, calculated as (short_volume / total_volume) × 100",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "exempt_volume",
           .name = "Exempt Volume",
           .description = "Portion of short volume marked exempt from Regulation SHO requirements",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "non_exempt_volume",
           .name = "Non-Exempt Volume",
           .description = "Portion of short volume subject to Regulation SHO requirements (non-exempt short sales)",
           .type = ArrowType::INT32,
           .nullable = true},
      }};
}

} // namespace data_sdk::polygon
