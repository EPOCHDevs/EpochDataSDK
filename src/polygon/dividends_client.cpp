#include "polygon/dividends_client.hpp"

#include <glaze/glaze.hpp>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "polygon/base_client.hpp"

namespace data_sdk::polygon {

namespace {

// Dividend JSON structure
struct DividendData {
  std::optional<std::string> ticker;
  std::optional<std::string> ex_dividend_date;
  std::optional<double> cash_amount;
  std::optional<std::string> currency;
  std::optional<std::string> declaration_date;
  std::optional<std::string> pay_date;
  std::optional<std::string> record_date;
  std::optional<int> frequency;
  std::optional<std::string> dividend_type;
  std::optional<std::string> id;
};

struct DividendsResponse {
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<DividendData> results;
  std::string status;
};

} // namespace

// Private implementation
class DividendsClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getDividends(std::optional<std::string> ticker,
               std::optional<std::string> ex_dividend_date,
               std::optional<std::string> ex_dividend_date_gte,
               std::optional<std::string> ex_dividend_date_lte,
               std::optional<std::string> declaration_date,
               std::optional<std::string> record_date,
               std::optional<std::string> pay_date,
               std::optional<int> frequency,
               std::optional<double> cash_amount,
               std::optional<std::string> dividend_type,
               std::optional<int> limit,
               std::optional<std::string> sort,
               std::optional<std::string> order) const {
    return drogon::sync_wait(getDividendsAsync(ticker, ex_dividend_date, ex_dividend_date_gte,
                                                ex_dividend_date_lte, declaration_date, record_date,
                                                pay_date, frequency, cash_amount, dividend_type,
                                                limit, sort, order));
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getDividendsAsync(std::optional<std::string> ticker,
                    std::optional<std::string> ex_dividend_date,
                    std::optional<std::string> ex_dividend_date_gte,
                    std::optional<std::string> ex_dividend_date_lte,
                    std::optional<std::string> declaration_date,
                    std::optional<std::string> record_date,
                    std::optional<std::string> pay_date,
                    std::optional<int> frequency,
                    std::optional<double> cash_amount,
                    std::optional<std::string> dividend_type,
                    std::optional<int> limit,
                    std::optional<std::string> sort,
                    std::optional<std::string> order) const {

    std::vector<std::pair<std::string, std::string>> q;
    if (ticker.has_value())
      q.emplace_back("ticker", *ticker);
    if (ex_dividend_date.has_value())
      q.emplace_back("ex_dividend_date", *ex_dividend_date);
    if (ex_dividend_date_gte.has_value())
      q.emplace_back("ex_dividend_date.gte", *ex_dividend_date_gte);
    if (ex_dividend_date_lte.has_value())
      q.emplace_back("ex_dividend_date.lte", *ex_dividend_date_lte);
    if (declaration_date.has_value())
      q.emplace_back("declaration_date", *declaration_date);
    if (record_date.has_value())
      q.emplace_back("record_date", *record_date);
    if (pay_date.has_value())
      q.emplace_back("pay_date", *pay_date);
    if (frequency.has_value())
      q.emplace_back("frequency", std::to_string(*frequency));
    if (cash_amount.has_value())
      q.emplace_back("cash_amount", std::to_string(*cash_amount));
    if (dividend_type.has_value())
      q.emplace_back("dividend_type", *dividend_type);
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));
    if (sort.has_value())
      q.emplace_back("sort", *sort);
    if (order.has_value())
      q.emplace_back("order", *order);

    const std::string path = "/v3/reference/dividends";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    DividendsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      std::string error_msg = "Failed to parse dividends JSON response: " +
                              glz::format_error(ec, *bodyRes);
      SPDLOG_ERROR("Dividends parsing error (async): {}", error_msg);
      SPDLOG_DEBUG("Raw JSON response: {}", *bodyRes);
      co_return makeError<epoch_frame::DataFrame>(200, error_msg, nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> tickers, ex_dates, decl_dates, rec_dates, pay_dates, div_types, ids, currencies;
    std::vector<double> amounts;
    std::vector<int64_t> frequencies;

    tickers.reserve(N);
    ex_dates.reserve(N);
    amounts.reserve(N);
    currencies.reserve(N);
    decl_dates.reserve(N);
    rec_dates.reserve(N);
    pay_dates.reserve(N);
    frequencies.reserve(N);
    div_types.reserve(N);
    ids.reserve(N);

    for (const auto &r : parsed.results) {
      tickers.push_back(r.ticker.value_or(""));
      ex_dates.push_back(r.ex_dividend_date.value_or(""));
      amounts.push_back(r.cash_amount.value_or(0.0));
      currencies.push_back(r.currency.value_or(""));
      decl_dates.push_back(r.declaration_date.value_or(""));
      rec_dates.push_back(r.record_date.value_or(""));
      pay_dates.push_back(r.pay_date.value_or(""));
      frequencies.push_back(r.frequency.value_or(0));
      div_types.push_back(r.dividend_type.value_or(""));
      ids.push_back(r.id.value_or(""));
    }

    // Convert ex_dividend_date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(ex_dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "ex_dividend_date", "UTC");

    std::vector<std::string> columns = {
        "ticker", "id", "cash_amount", "currency", "declaration_date", "record_date",
        "pay_date", "frequency", "dividend_type"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(tickers),
        epoch_frame::factory::array::make_array(ids),
        epoch_frame::factory::array::make_array(amounts),
        epoch_frame::factory::array::make_array(currencies),
        makeDateTimestampArray(decl_dates),
        makeDateTimestampArray(rec_dates),
        makeDateTimestampArray(pay_dates),
        epoch_frame::factory::array::make_array(frequencies),
        epoch_frame::factory::array::make_array(div_types)};

    co_return epoch_frame::make_dataframe(index, arrays, columns);
  }
};

// Public API
DividendsClient::DividendsClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

DividendsClient::~DividendsClient() = default;

Expected<epoch_frame::DataFrame>
DividendsClient::getDividends(std::optional<std::string> ticker,
                              std::optional<std::string> ex_dividend_date,
                              std::optional<std::string> ex_dividend_date_gte,
                              std::optional<std::string> ex_dividend_date_lte,
                              std::optional<std::string> declaration_date,
                              std::optional<std::string> record_date,
                              std::optional<std::string> pay_date,
                              std::optional<int> frequency,
                              std::optional<double> cash_amount,
                              std::optional<std::string> dividend_type,
                              std::optional<int> limit,
                              std::optional<std::string> sort,
                              std::optional<std::string> order) const {
  return impl_->getDividends(ticker, ex_dividend_date, ex_dividend_date_gte,
                             ex_dividend_date_lte, declaration_date, record_date,
                             pay_date, frequency, cash_amount, dividend_type,
                             limit, sort, order);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
DividendsClient::getDividendsAsync(std::optional<std::string> ticker,
                                   std::optional<std::string> ex_dividend_date,
                                   std::optional<std::string> ex_dividend_date_gte,
                                   std::optional<std::string> ex_dividend_date_lte,
                                   std::optional<std::string> declaration_date,
                                   std::optional<std::string> record_date,
                                   std::optional<std::string> pay_date,
                                   std::optional<int> frequency,
                                   std::optional<double> cash_amount,
                                   std::optional<std::string> dividend_type,
                                   std::optional<int> limit,
                                   std::optional<std::string> sort,
                                   std::optional<std::string> order) const {
  return impl_->getDividendsAsync(ticker, ex_dividend_date, ex_dividend_date_gte,
                                  ex_dividend_date_lte, declaration_date, record_date,
                                  pay_date, frequency, cash_amount, dividend_type,
                                  limit, sort, order);
}

data_sdk::DataFrameMetadata DividendsClient::getMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "dividends",
      .description = "Retrieve historical dividend distribution records for a specified ticker, including declaration, ex-dividend, record, and pay dates along with payout amounts and frequency. The service consolidates key dividend information to support income analysis, total return calculations, dividend-focused strategies, and tax planning activities.",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,
      .category_prefix = "D:",
      .columns = {
          {.id = "ticker",
           .name = "Ticker",
           .description = "Stock symbol",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "id",
           .name = "Dividend ID",
           .description = "Unique dividend identifier",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "cash_amount",
           .name = "Cash Amount",
           .description = "Dividend payout per share",
           .type = ArrowType::FLOAT64,
           .nullable = false},
          {.id = "currency",
           .name = "Currency",
           .description = "Currency code for the dividend payment (e.g., USD, EUR)",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "declaration_date",
           .name = "Declaration Date",
           .description = "Announcement date",
           .type = ArrowType::TIMESTAMP_NS_UTC,
           .nullable = true},
          {.id = "record_date",
           .name = "Record Date",
           .description = "Holder registration deadline",
           .type = ArrowType::TIMESTAMP_NS_UTC,
           .nullable = true},
          {.id = "pay_date",
           .name = "Pay Date",
           .description = "Actual distribution date",
           .type = ArrowType::TIMESTAMP_NS_UTC,
           .nullable = true},
          {.id = "frequency",
           .name = "Frequency",
           .description = "Annual payment frequency count: 0 (one-time), 1 (annual), 2 (bi-annual), 4 (quarterly), 12 (monthly), 24 (bi-monthly), 52 (weekly)",
           .type = ArrowType::INT64,
           .nullable = false},
          {.id = "dividend_type",
           .name = "Dividend Type",
           .description = "Distribution type classification: CD (consistent dividends), SC (special cash), LT (long-term capital gains), ST (short-term capital gains)",
           .type = ArrowType::STRING,
           .nullable = true},
      }};
}

} // namespace data_sdk::polygon
