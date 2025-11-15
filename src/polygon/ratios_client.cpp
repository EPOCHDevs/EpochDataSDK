#include "polygon/ratios_client.hpp"

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
class RatiosClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getRatios(std::optional<std::string> ticker,
            std::optional<int> limit,
            std::optional<std::string> sort) const {

    std::vector<std::pair<std::string, std::string>> q;
    if (ticker.has_value())
      q.emplace_back("ticker", *ticker);
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));
    if (sort.has_value())
      q.emplace_back("sort", *sort);

    const std::string path = "/stocks/financials/v1/ratios";
    auto bodyRes = httpGetWithRetry(path, q, 3);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    FinancialRatiosResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getRatios parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker.value_or(""), path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse JSON response", nullptr);
    }

    // Build DataFrame
    std::vector<std::string> date_strings;
    std::vector<std::string> tickers_col;
    std::vector<double> average_volume, cash, current, debt_to_equity, dividend_yield,
        earnings_per_share, enterprise_value, ev_to_ebitda, ev_to_sales, free_cash_flow,
        market_cap, price, price_to_book, price_to_cash_flow, price_to_earnings,
        price_to_free_cash_flow, price_to_sales, quick, return_on_assets, return_on_equity;

    const auto sz = parsed.results.size();
    date_strings.reserve(sz);
    tickers_col.reserve(sz);
    average_volume.reserve(sz);
    cash.reserve(sz);
    current.reserve(sz);
    debt_to_equity.reserve(sz);
    dividend_yield.reserve(sz);
    earnings_per_share.reserve(sz);
    enterprise_value.reserve(sz);
    ev_to_ebitda.reserve(sz);
    ev_to_sales.reserve(sz);
    free_cash_flow.reserve(sz);
    market_cap.reserve(sz);
    price.reserve(sz);
    price_to_book.reserve(sz);
    price_to_cash_flow.reserve(sz);
    price_to_earnings.reserve(sz);
    price_to_free_cash_flow.reserve(sz);
    price_to_sales.reserve(sz);
    quick.reserve(sz);
    return_on_assets.reserve(sz);
    return_on_equity.reserve(sz);

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.date.value_or(""));
      tickers_col.push_back(r.ticker.value_or(""));
      average_volume.push_back(r.average_volume.value_or(0.0));
      cash.push_back(r.cash.value_or(0.0));
      current.push_back(r.current.value_or(0.0));
      debt_to_equity.push_back(r.debt_to_equity.value_or(0.0));
      dividend_yield.push_back(r.dividend_yield.value_or(0.0));
      earnings_per_share.push_back(r.earnings_per_share.value_or(0.0));
      enterprise_value.push_back(r.enterprise_value.value_or(0.0));
      ev_to_ebitda.push_back(r.ev_to_ebitda.value_or(0.0));
      ev_to_sales.push_back(r.ev_to_sales.value_or(0.0));
      free_cash_flow.push_back(r.free_cash_flow.value_or(0.0));
      market_cap.push_back(r.market_cap.value_or(0.0));
      price.push_back(r.price.value_or(0.0));
      price_to_book.push_back(r.price_to_book.value_or(0.0));
      price_to_cash_flow.push_back(r.price_to_cash_flow.value_or(0.0));
      price_to_earnings.push_back(r.price_to_earnings.value_or(0.0));
      price_to_free_cash_flow.push_back(r.price_to_free_cash_flow.value_or(0.0));
      price_to_sales.push_back(r.price_to_sales.value_or(0.0));
      quick.push_back(r.quick.value_or(0.0));
      return_on_assets.push_back(r.return_on_assets.value_or(0.0));
      return_on_equity.push_back(r.return_on_equity.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon ratios pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = httpGetWithRetry(parsed_url->path, parsed_url->query, 3);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon ratios pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      FinancialRatiosResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon ratios page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.date.value_or(""));
        tickers_col.push_back(r.ticker.value_or(""));
        average_volume.push_back(r.average_volume.value_or(0.0));
        cash.push_back(r.cash.value_or(0.0));
        current.push_back(r.current.value_or(0.0));
        debt_to_equity.push_back(r.debt_to_equity.value_or(0.0));
        dividend_yield.push_back(r.dividend_yield.value_or(0.0));
        earnings_per_share.push_back(r.earnings_per_share.value_or(0.0));
        enterprise_value.push_back(r.enterprise_value.value_or(0.0));
        ev_to_ebitda.push_back(r.ev_to_ebitda.value_or(0.0));
        ev_to_sales.push_back(r.ev_to_sales.value_or(0.0));
        free_cash_flow.push_back(r.free_cash_flow.value_or(0.0));
        market_cap.push_back(r.market_cap.value_or(0.0));
        price.push_back(r.price.value_or(0.0));
        price_to_book.push_back(r.price_to_book.value_or(0.0));
        price_to_cash_flow.push_back(r.price_to_cash_flow.value_or(0.0));
        price_to_earnings.push_back(r.price_to_earnings.value_or(0.0));
        price_to_free_cash_flow.push_back(r.price_to_free_cash_flow.value_or(0.0));
        price_to_sales.push_back(r.price_to_sales.value_or(0.0));
        quick.push_back(r.quick.value_or(0.0));
        return_on_assets.push_back(r.return_on_assets.value_or(0.0));
        return_on_equity.push_back(r.return_on_equity.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon ratios: fetched {} pages total_rows={}",
                  page_count, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");

    std::vector<std::string> columns = {
        "ticker", "average_volume", "cash", "current", "debt_to_equity",
        "dividend_yield", "earnings_per_share", "enterprise_value", "ev_to_ebitda",
        "ev_to_sales", "free_cash_flow", "market_cap", "price", "price_to_book",
        "price_to_cash_flow", "price_to_earnings", "price_to_free_cash_flow",
        "price_to_sales", "quick", "return_on_assets", "return_on_equity"};

    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(average_volume),
        epoch_frame::factory::array::make_array(cash),
        epoch_frame::factory::array::make_array(current),
        epoch_frame::factory::array::make_array(debt_to_equity),
        epoch_frame::factory::array::make_array(dividend_yield),
        epoch_frame::factory::array::make_array(earnings_per_share),
        epoch_frame::factory::array::make_array(enterprise_value),
        epoch_frame::factory::array::make_array(ev_to_ebitda),
        epoch_frame::factory::array::make_array(ev_to_sales),
        epoch_frame::factory::array::make_array(free_cash_flow),
        epoch_frame::factory::array::make_array(market_cap),
        epoch_frame::factory::array::make_array(price),
        epoch_frame::factory::array::make_array(price_to_book),
        epoch_frame::factory::array::make_array(price_to_cash_flow),
        epoch_frame::factory::array::make_array(price_to_earnings),
        epoch_frame::factory::array::make_array(price_to_free_cash_flow),
        epoch_frame::factory::array::make_array(price_to_sales),
        epoch_frame::factory::array::make_array(quick),
        epoch_frame::factory::array::make_array(return_on_assets),
        epoch_frame::factory::array::make_array(return_on_equity)};

    return epoch_frame::make_dataframe(index, data, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getRatiosAsync(std::optional<std::string> ticker,
                 std::optional<int> limit,
                 std::optional<std::string> sort) const {
    // For async, we delegate to sync implementation for simplicity
    // In production, you'd implement full async HTTP calls
    co_return getRatios(ticker, limit, sort);
  }
};

// Constructor/Destructor implementations
RatiosClient::RatiosClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

RatiosClient::~RatiosClient() = default;

// Public method implementations
Expected<epoch_frame::DataFrame>
RatiosClient::getRatios(std::optional<std::string> ticker,
                        std::optional<int> limit,
                        std::optional<std::string> sort) const {
  return impl_->getRatios(ticker, limit, sort);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
RatiosClient::getRatiosAsync(std::optional<std::string> ticker,
                             std::optional<int> limit,
                             std::optional<std::string> sort) const {
  return impl_->getRatiosAsync(ticker, limit, sort);
}

data_sdk::DataFrameMetadata RatiosClient::getMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "financial_ratios",
      .description = "Retrieve comprehensive financial ratios data providing key valuation, profitability, liquidity, and leverage metrics for public companies. This endpoint combines income statements, balance sheets, and cash flow data with daily stock prices to calculate TTM (trailing twelve months) ratios. Use cases include company valuation analysis, comparative financial assessment, health evaluation, and investment screening. Data is updated end-of-day with all historical data available.",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,
      .category_prefix = "R:",
      .columns = {
          {.id = "ticker",
           .name = "Ticker",
           .description = "Stock symbol identifier",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "average_volume",
           .name = "Average Volume",
           .description = "Average daily trading volume over a specified period",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "cash",
           .name = "Cash Ratio",
           .description = "Cash and cash equivalents divided by current liabilities, measuring immediate liquidity",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "current",
           .name = "Current Ratio",
           .description = "Current assets divided by current liabilities, measuring short-term liquidity",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "debt_to_equity",
           .name = "Debt-to-Equity Ratio",
           .description = "Total debt divided by shareholders' equity, measuring financial leverage",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "dividend_yield",
           .name = "Dividend Yield",
           .description = "Annual dividends per share divided by stock price, expressed as percentage",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "earnings_per_share",
           .name = "Earnings Per Share",
           .description = "Net income divided by number of outstanding shares",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "enterprise_value",
           .name = "Enterprise Value",
           .description = "Market capitalization plus debt minus cash, representing total company value",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "ev_to_ebitda",
           .name = "EV/EBITDA",
           .description = "Enterprise value divided by earnings before interest, taxes, depreciation, and amortization",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "ev_to_sales",
           .name = "EV/Sales",
           .description = "Enterprise value divided by total revenue",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "free_cash_flow",
           .name = "Free Cash Flow",
           .description = "Operating cash flow minus capital expenditures, representing cash available for distribution",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "market_cap",
           .name = "Market Capitalization",
           .description = "Total market value of outstanding shares (stock price × shares outstanding)",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "price",
           .name = "Stock Price",
           .description = "Most recent trading day closing stock price used for ratio calculations",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "price_to_book",
           .name = "Price-to-Book Ratio",
           .description = "Market value divided by book value of equity, measuring market premium over accounting value",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "price_to_cash_flow",
           .name = "Price-to-Cash-Flow Ratio",
           .description = "Stock price divided by operating cash flow per share, only calculated when cash flow is positive",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "price_to_earnings",
           .name = "Price-to-Earnings Ratio (P/E)",
           .description = "Stock price divided by earnings per share, only calculated when EPS is positive",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "price_to_free_cash_flow",
           .name = "Price-to-Free-Cash-Flow Ratio",
           .description = "Stock price divided by free cash flow per share",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "price_to_sales",
           .name = "Price-to-Sales Ratio",
           .description = "Market capitalization divided by total revenue, measuring valuation relative to sales",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "quick",
           .name = "Quick Ratio",
           .description = "(Current assets minus inventory) divided by current liabilities, measuring immediate liquidity",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "return_on_assets",
           .name = "Return on Assets (ROA)",
           .description = "Net income divided by total assets, measuring profitability relative to asset base",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "return_on_equity",
           .name = "Return on Equity (ROE)",
           .description = "Net income divided by shareholders' equity, measuring profitability relative to equity",
           .type = ArrowType::FLOAT64,
           .nullable = true},
      }};
}

} // namespace data_sdk::polygon
