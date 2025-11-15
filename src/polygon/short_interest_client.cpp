#include "polygon/short_interest_client.hpp"

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
class ShortInterestClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getShortInterest(const std::string &ticker, const std::string &date_from,
                   const std::string &date_to, std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("ticker", ticker);
    q.emplace_back("settlement_date.gte", date_from);
    q.emplace_back("settlement_date.lte", date_to);
    // Always fetch in ascending order for backtesting consistency
    q.emplace_back("order", "asc");
    q.emplace_back("sort", "settlement_date");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/v1/short-interest";
    auto bodyRes = httpGetWithRetry(path, q, 3);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    ShortInterestResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getShortInterest parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse JSON response", nullptr);
    }

    // Build DataFrame
    std::vector<std::string> date_strings;
    std::vector<std::string> tickers_col;
    std::vector<int> short_interest, avg_daily_volume;
    std::vector<double> days_to_cover;

    const auto sz = parsed.results.size();
    date_strings.reserve(sz);
    tickers_col.reserve(sz);
    short_interest.reserve(sz);
    avg_daily_volume.reserve(sz);
    days_to_cover.reserve(sz);

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.settlement_date.value_or(""));
      tickers_col.push_back(r.ticker.value_or(""));
      short_interest.push_back(r.short_interest.value_or(0));
      avg_daily_volume.push_back(r.avg_daily_volume.value_or(0));
      days_to_cover.push_back(r.days_to_cover.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon short_interest pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = httpGetWithRetry(parsed_url->path, parsed_url->query, 3);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon short_interest pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      ShortInterestResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon short_interest page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.settlement_date.value_or(""));
        tickers_col.push_back(r.ticker.value_or(""));
        short_interest.push_back(r.short_interest.value_or(0));
        avg_daily_volume.push_back(r.avg_daily_volume.value_or(0));
        days_to_cover.push_back(r.days_to_cover.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon short_interest: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "short_interest",
                                         "avg_daily_volume", "days_to_cover"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(short_interest),
        epoch_frame::factory::array::make_array(avg_daily_volume),
        epoch_frame::factory::array::make_array(days_to_cover)};

    return epoch_frame::make_dataframe(index, data, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getShortInterestAsync(std::string ticker, std::string date_from,
                        std::string date_to, std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("ticker", ticker);
    q.emplace_back("settlement_date.gte", date_from);
    q.emplace_back("settlement_date.lte", date_to);
    // Always fetch in ascending order for backtesting consistency
    q.emplace_back("order", "asc");
    q.emplace_back("sort", "settlement_date");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/v1/short-interest";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    ShortInterestResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getShortInterestAsync parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse JSON response", nullptr);
    }

    // Build DataFrame
    std::vector<std::string> date_strings;
    std::vector<std::string> tickers_col;
    std::vector<int> short_interest, avg_daily_volume;
    std::vector<double> days_to_cover;

    const auto sz = parsed.results.size();
    date_strings.reserve(sz);
    tickers_col.reserve(sz);
    short_interest.reserve(sz);
    avg_daily_volume.reserve(sz);
    days_to_cover.reserve(sz);

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.settlement_date.value_or(""));
      tickers_col.push_back(r.ticker.value_or(""));
      short_interest.push_back(r.short_interest.value_or(0));
      avg_daily_volume.push_back(r.avg_daily_volume.value_or(0));
      days_to_cover.push_back(r.days_to_cover.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon short_interest Async pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = co_await httpAsyncGet(parsed_url->path, parsed_url->query);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon short_interest Async pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      ShortInterestResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon short_interest Async page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.settlement_date.value_or(""));
        tickers_col.push_back(r.ticker.value_or(""));
        short_interest.push_back(r.short_interest.value_or(0));
        avg_daily_volume.push_back(r.avg_daily_volume.value_or(0));
        days_to_cover.push_back(r.days_to_cover.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon short_interest Async: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "short_interest",
                                         "avg_daily_volume", "days_to_cover"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(short_interest),
        epoch_frame::factory::array::make_array(avg_daily_volume),
        epoch_frame::factory::array::make_array(days_to_cover)};

    co_return epoch_frame::make_dataframe(index, data, columns);
  }
};

// Public API
ShortInterestClient::ShortInterestClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

ShortInterestClient::~ShortInterestClient() = default;

Expected<epoch_frame::DataFrame>
ShortInterestClient::getShortInterest(const std::string &ticker,
                                       const std::string &date_from,
                                       const std::string &date_to,
                                       std::optional<int> limit) const {
  return impl_->getShortInterest(ticker, date_from, date_to, limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
ShortInterestClient::getShortInterestAsync(std::string ticker,
                                            std::string date_from,
                                            std::string date_to,
                                            std::optional<int> limit) const {
  return impl_->getShortInterestAsync(std::move(ticker), std::move(date_from),
                                      std::move(date_to), limit);
}

data_sdk::DataFrameMetadata ShortInterestClient::getMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "short_interest",
      .description = "Retrieve bi-monthly aggregated short interest data reported to FINRA by broker-dealers for specified stock tickers. Short interest represents the total number of shares sold short but not yet covered or closed out, serving as a market sentiment indicator. Use cases include market sentiment analysis, short-squeeze prediction, risk management, and trading strategy refinement. Data is available across all plan tiers, updated bi-weekly with historical depth varying by subscription level (Basic: 2 years; Starter+: all history).",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,
      .category_prefix = "SI:",
      .columns = {
          {.id = "ticker",
           .name = "Ticker",
           .description = "Stock symbol identifier",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "short_interest",
           .name = "Short Interest",
           .description = "Total number of shares sold short but not yet covered or closed out, representing open short positions",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "avg_daily_volume",
           .name = "Average Daily Volume",
           .description = "Average daily trading volume providing context for short interest magnitude",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "days_to_cover",
           .name = "Days to Cover",
           .description = "Estimated number of days to cover all short positions, calculated as short interest divided by average daily volume",
           .type = ArrowType::FLOAT64,
           .nullable = true},
      }};
}

} // namespace data_sdk::polygon
