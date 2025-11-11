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
  std::optional<std::string> declaration_date;
  std::optional<std::string> pay_date;
  std::optional<std::string> record_date;
  std::optional<int> frequency;
  std::optional<std::string> dividend_type;
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
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    DividendsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse dividends JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> tickers, ex_dates, decl_dates, rec_dates, pay_dates, div_types;
    std::vector<double> amounts;
    std::vector<int64_t> frequencies;

    tickers.reserve(N);
    ex_dates.reserve(N);
    amounts.reserve(N);
    decl_dates.reserve(N);
    rec_dates.reserve(N);
    pay_dates.reserve(N);
    frequencies.reserve(N);
    div_types.reserve(N);

    for (const auto &r : parsed.results) {
      tickers.push_back(r.ticker.value_or(""));
      ex_dates.push_back(r.ex_dividend_date.value_or(""));
      amounts.push_back(r.cash_amount.value_or(0.0));
      decl_dates.push_back(r.declaration_date.value_or(""));
      rec_dates.push_back(r.record_date.value_or(""));
      pay_dates.push_back(r.pay_date.value_or(""));
      frequencies.push_back(r.frequency.value_or(0));
      div_types.push_back(r.dividend_type.value_or(""));
    }

    // Convert ex_dividend_date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(ex_dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "ex_dividend_date", "UTC");

    std::vector<std::string> columns = {
        "ticker", "cash_amount", "declaration_date", "record_date",
        "pay_date", "frequency", "dividend_type"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(tickers),
        epoch_frame::factory::array::make_array(amounts),
        epoch_frame::factory::array::make_array(decl_dates),
        epoch_frame::factory::array::make_array(rec_dates),
        epoch_frame::factory::array::make_array(pay_dates),
        epoch_frame::factory::array::make_array(frequencies),
        epoch_frame::factory::array::make_array(div_types)};

    return epoch_frame::make_dataframe(index, arrays, columns);
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
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse dividends JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> tickers, ex_dates, decl_dates, rec_dates, pay_dates, div_types;
    std::vector<double> amounts;
    std::vector<int64_t> frequencies;

    tickers.reserve(N);
    ex_dates.reserve(N);
    amounts.reserve(N);
    decl_dates.reserve(N);
    rec_dates.reserve(N);
    pay_dates.reserve(N);
    frequencies.reserve(N);
    div_types.reserve(N);

    for (const auto &r : parsed.results) {
      tickers.push_back(r.ticker.value_or(""));
      ex_dates.push_back(r.ex_dividend_date.value_or(""));
      amounts.push_back(r.cash_amount.value_or(0.0));
      decl_dates.push_back(r.declaration_date.value_or(""));
      rec_dates.push_back(r.record_date.value_or(""));
      pay_dates.push_back(r.pay_date.value_or(""));
      frequencies.push_back(r.frequency.value_or(0));
      div_types.push_back(r.dividend_type.value_or(""));
    }

    // Convert ex_dividend_date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(ex_dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "ex_dividend_date", "UTC");

    std::vector<std::string> columns = {
        "ticker", "cash_amount", "declaration_date", "record_date",
        "pay_date", "frequency", "dividend_type"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(tickers),
        epoch_frame::factory::array::make_array(amounts),
        epoch_frame::factory::array::make_array(decl_dates),
        epoch_frame::factory::array::make_array(rec_dates),
        epoch_frame::factory::array::make_array(pay_dates),
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

} // namespace data_sdk::polygon
