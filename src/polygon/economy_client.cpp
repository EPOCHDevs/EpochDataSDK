#include "epoch_data_sdk/polygon/economy_client.hpp"

#include <glaze/glaze.hpp>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "epoch_data_sdk/polygon/base_client.hpp"
#include "epoch_data_sdk/polygon/models.hpp"

namespace data_sdk::polygon {

// Private implementation using PImpl pattern
class EconomyClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getTreasuryYields(const std::string &from_date,
                    const std::string &to_date,
                    std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("date.gte", from_date);
    q.emplace_back("date.lte", to_date);
    q.emplace_back("sort", "date");
    q.emplace_back("order", "desc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/fed/v1/treasury-yields";
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    TreasuryYieldsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse treasury yields JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> dates;
    std::vector<double> y1m, y3m, y6m, y1y, y2y, y3y, y5y, y7y, y10y, y20y, y30y;

    dates.reserve(N);
    y1m.reserve(N); y3m.reserve(N); y6m.reserve(N);
    y1y.reserve(N); y2y.reserve(N); y3y.reserve(N);
    y5y.reserve(N); y7y.reserve(N); y10y.reserve(N);
    y20y.reserve(N); y30y.reserve(N);

    for (const auto &r : parsed.results) {
      dates.push_back(r.date.value_or(""));
      y1m.push_back(r.yield_1_month.value_or(std::numeric_limits<double>::quiet_NaN()));
      y3m.push_back(r.yield_3_month.value_or(std::numeric_limits<double>::quiet_NaN()));
      y6m.push_back(r.yield_6_month.value_or(std::numeric_limits<double>::quiet_NaN()));
      y1y.push_back(r.yield_1_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y2y.push_back(r.yield_2_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y3y.push_back(r.yield_3_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y5y.push_back(r.yield_5_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y7y.push_back(r.yield_7_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y10y.push_back(r.yield_10_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y20y.push_back(r.yield_20_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y30y.push_back(r.yield_30_year.value_or(std::numeric_limits<double>::quiet_NaN()));
    }

    // Convert date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "date", "UTC");

    std::vector<std::string> columns = {
        "yield_1_month", "yield_3_month", "yield_6_month",
        "yield_1_year", "yield_2_year", "yield_3_year",
        "yield_5_year", "yield_7_year", "yield_10_year",
        "yield_20_year", "yield_30_year"};

    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(y1m),
        epoch_frame::factory::array::make_array(y3m),
        epoch_frame::factory::array::make_array(y6m),
        epoch_frame::factory::array::make_array(y1y),
        epoch_frame::factory::array::make_array(y2y),
        epoch_frame::factory::array::make_array(y3y),
        epoch_frame::factory::array::make_array(y5y),
        epoch_frame::factory::array::make_array(y7y),
        epoch_frame::factory::array::make_array(y10y),
        epoch_frame::factory::array::make_array(y20y),
        epoch_frame::factory::array::make_array(y30y)};

    return epoch_frame::make_dataframe(index, arrays, columns);
  }

  Expected<epoch_frame::DataFrame>
  getInflation(const std::string &from_date,
               const std::string &to_date,
               std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("date.gte", from_date);
    q.emplace_back("date.lte", to_date);
    q.emplace_back("sort", "date");
    q.emplace_back("order", "desc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/fed/v1/inflation";
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    InflationResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse inflation JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> dates;
    std::vector<double> cpi, cpi_core, cpi_yoy, pce, pce_core, pce_spending;

    dates.reserve(N);
    cpi.reserve(N); cpi_core.reserve(N); cpi_yoy.reserve(N);
    pce.reserve(N); pce_core.reserve(N); pce_spending.reserve(N);

    for (const auto &r : parsed.results) {
      dates.push_back(r.date.value_or(""));
      cpi.push_back(r.cpi.value_or(std::numeric_limits<double>::quiet_NaN()));
      cpi_core.push_back(r.cpi_core.value_or(std::numeric_limits<double>::quiet_NaN()));
      cpi_yoy.push_back(r.cpi_year_over_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      pce.push_back(r.pce.value_or(std::numeric_limits<double>::quiet_NaN()));
      pce_core.push_back(r.pce_core.value_or(std::numeric_limits<double>::quiet_NaN()));
      pce_spending.push_back(r.pce_spending.value_or(std::numeric_limits<double>::quiet_NaN()));
    }

    // Convert date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "date", "UTC");

    std::vector<std::string> columns = {
        "cpi", "cpi_core", "cpi_year_over_year",
        "pce", "pce_core", "pce_spending"};

    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(cpi),
        epoch_frame::factory::array::make_array(cpi_core),
        epoch_frame::factory::array::make_array(cpi_yoy),
        epoch_frame::factory::array::make_array(pce),
        epoch_frame::factory::array::make_array(pce_core),
        epoch_frame::factory::array::make_array(pce_spending)};

    return epoch_frame::make_dataframe(index, arrays, columns);
  }

  Expected<epoch_frame::DataFrame>
  getInflationExpectations(const std::string &from_date,
                           const std::string &to_date,
                           std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("date.gte", from_date);
    q.emplace_back("date.lte", to_date);
    q.emplace_back("sort", "date");
    q.emplace_back("order", "desc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/fed/v1/inflation-expectations";
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    InflationExpectationsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse inflation expectations JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> dates;
    std::vector<double> market_5y, market_10y, forward_5_10,
                        model_1y, model_5y, model_10y, model_30y;

    dates.reserve(N);
    market_5y.reserve(N); market_10y.reserve(N); forward_5_10.reserve(N);
    model_1y.reserve(N); model_5y.reserve(N); model_10y.reserve(N); model_30y.reserve(N);

    for (const auto &r : parsed.results) {
      dates.push_back(r.date.value_or(""));
      market_5y.push_back(r.market_5_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      market_10y.push_back(r.market_10_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      forward_5_10.push_back(r.forward_years_5_to_10.value_or(std::numeric_limits<double>::quiet_NaN()));
      model_1y.push_back(r.model_1_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      model_5y.push_back(r.model_5_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      model_10y.push_back(r.model_10_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      model_30y.push_back(r.model_30_year.value_or(std::numeric_limits<double>::quiet_NaN()));
    }

    // Convert date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "date", "UTC");

    std::vector<std::string> columns = {
        "market_5_year", "market_10_year", "forward_years_5_to_10",
        "model_1_year", "model_5_year", "model_10_year", "model_30_year"};

    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(market_5y),
        epoch_frame::factory::array::make_array(market_10y),
        epoch_frame::factory::array::make_array(forward_5_10),
        epoch_frame::factory::array::make_array(model_1y),
        epoch_frame::factory::array::make_array(model_5y),
        epoch_frame::factory::array::make_array(model_10y),
        epoch_frame::factory::array::make_array(model_30y)};

    return epoch_frame::make_dataframe(index, arrays, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTreasuryYieldsAsync(std::string from_date,
                         std::string to_date,
                         std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("date.gte", from_date);
    q.emplace_back("date.lte", to_date);
    q.emplace_back("sort", "date");
    q.emplace_back("order", "desc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/fed/v1/treasury-yields";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    TreasuryYieldsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse treasury yields JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> dates;
    std::vector<double> y1m, y3m, y6m, y1y, y2y, y3y, y5y, y7y, y10y, y20y, y30y;

    dates.reserve(N);
    y1m.reserve(N); y3m.reserve(N); y6m.reserve(N);
    y1y.reserve(N); y2y.reserve(N); y3y.reserve(N);
    y5y.reserve(N); y7y.reserve(N); y10y.reserve(N);
    y20y.reserve(N); y30y.reserve(N);

    for (const auto &r : parsed.results) {
      dates.push_back(r.date.value_or(""));
      y1m.push_back(r.yield_1_month.value_or(std::numeric_limits<double>::quiet_NaN()));
      y3m.push_back(r.yield_3_month.value_or(std::numeric_limits<double>::quiet_NaN()));
      y6m.push_back(r.yield_6_month.value_or(std::numeric_limits<double>::quiet_NaN()));
      y1y.push_back(r.yield_1_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y2y.push_back(r.yield_2_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y3y.push_back(r.yield_3_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y5y.push_back(r.yield_5_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y7y.push_back(r.yield_7_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y10y.push_back(r.yield_10_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y20y.push_back(r.yield_20_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      y30y.push_back(r.yield_30_year.value_or(std::numeric_limits<double>::quiet_NaN()));
    }

    // Convert date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "date", "UTC");

    std::vector<std::string> columns = {
        "yield_1_month", "yield_3_month", "yield_6_month",
        "yield_1_year", "yield_2_year", "yield_3_year",
        "yield_5_year", "yield_7_year", "yield_10_year",
        "yield_20_year", "yield_30_year"};

    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(y1m),
        epoch_frame::factory::array::make_array(y3m),
        epoch_frame::factory::array::make_array(y6m),
        epoch_frame::factory::array::make_array(y1y),
        epoch_frame::factory::array::make_array(y2y),
        epoch_frame::factory::array::make_array(y3y),
        epoch_frame::factory::array::make_array(y5y),
        epoch_frame::factory::array::make_array(y7y),
        epoch_frame::factory::array::make_array(y10y),
        epoch_frame::factory::array::make_array(y20y),
        epoch_frame::factory::array::make_array(y30y)};

    co_return epoch_frame::make_dataframe(index, arrays, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getInflationAsync(std::string from_date,
                    std::string to_date,
                    std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("date.gte", from_date);
    q.emplace_back("date.lte", to_date);
    q.emplace_back("sort", "date");
    q.emplace_back("order", "desc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/fed/v1/inflation";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    InflationResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse inflation JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> dates;
    std::vector<double> cpi, cpi_core, cpi_yoy, pce, pce_core, pce_spending;

    dates.reserve(N);
    cpi.reserve(N); cpi_core.reserve(N); cpi_yoy.reserve(N);
    pce.reserve(N); pce_core.reserve(N); pce_spending.reserve(N);

    for (const auto &r : parsed.results) {
      dates.push_back(r.date.value_or(""));
      cpi.push_back(r.cpi.value_or(std::numeric_limits<double>::quiet_NaN()));
      cpi_core.push_back(r.cpi_core.value_or(std::numeric_limits<double>::quiet_NaN()));
      cpi_yoy.push_back(r.cpi_year_over_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      pce.push_back(r.pce.value_or(std::numeric_limits<double>::quiet_NaN()));
      pce_core.push_back(r.pce_core.value_or(std::numeric_limits<double>::quiet_NaN()));
      pce_spending.push_back(r.pce_spending.value_or(std::numeric_limits<double>::quiet_NaN()));
    }

    // Convert date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "date", "UTC");

    std::vector<std::string> columns = {
        "cpi", "cpi_core", "cpi_year_over_year",
        "pce", "pce_core", "pce_spending"};

    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(cpi),
        epoch_frame::factory::array::make_array(cpi_core),
        epoch_frame::factory::array::make_array(cpi_yoy),
        epoch_frame::factory::array::make_array(pce),
        epoch_frame::factory::array::make_array(pce_core),
        epoch_frame::factory::array::make_array(pce_spending)};

    co_return epoch_frame::make_dataframe(index, arrays, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getInflationExpectationsAsync(std::string from_date,
                                 std::string to_date,
                                 std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("date.gte", from_date);
    q.emplace_back("date.lte", to_date);
    q.emplace_back("sort", "date");
    q.emplace_back("order", "desc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/fed/v1/inflation-expectations";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    InflationExpectationsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse inflation expectations JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> dates;
    std::vector<double> market_5y, market_10y, forward_5_10,
                        model_1y, model_5y, model_10y, model_30y;

    dates.reserve(N);
    market_5y.reserve(N); market_10y.reserve(N); forward_5_10.reserve(N);
    model_1y.reserve(N); model_5y.reserve(N); model_10y.reserve(N); model_30y.reserve(N);

    for (const auto &r : parsed.results) {
      dates.push_back(r.date.value_or(""));
      market_5y.push_back(r.market_5_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      market_10y.push_back(r.market_10_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      forward_5_10.push_back(r.forward_years_5_to_10.value_or(std::numeric_limits<double>::quiet_NaN()));
      model_1y.push_back(r.model_1_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      model_5y.push_back(r.model_5_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      model_10y.push_back(r.model_10_year.value_or(std::numeric_limits<double>::quiet_NaN()));
      model_30y.push_back(r.model_30_year.value_or(std::numeric_limits<double>::quiet_NaN()));
    }

    // Convert date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "date", "UTC");

    std::vector<std::string> columns = {
        "market_5_year", "market_10_year", "forward_years_5_to_10",
        "model_1_year", "model_5_year", "model_10_year", "model_30_year"};

    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(market_5y),
        epoch_frame::factory::array::make_array(market_10y),
        epoch_frame::factory::array::make_array(forward_5_10),
        epoch_frame::factory::array::make_array(model_1y),
        epoch_frame::factory::array::make_array(model_5y),
        epoch_frame::factory::array::make_array(model_10y),
        epoch_frame::factory::array::make_array(model_30y)};

    co_return epoch_frame::make_dataframe(index, arrays, columns);
  }
};

// Public API implementation
EconomyClient::EconomyClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

EconomyClient::~EconomyClient() = default;

Expected<epoch_frame::DataFrame>
EconomyClient::getTreasuryYields(const std::string &from_date,
                                 const std::string &to_date,
                                 std::optional<int> limit) const {
  return impl_->getTreasuryYields(from_date, to_date, limit);
}

Expected<epoch_frame::DataFrame>
EconomyClient::getInflation(const std::string &from_date,
                            const std::string &to_date,
                            std::optional<int> limit) const {
  return impl_->getInflation(from_date, to_date, limit);
}

Expected<epoch_frame::DataFrame>
EconomyClient::getInflationExpectations(const std::string &from_date,
                                        const std::string &to_date,
                                        std::optional<int> limit) const {
  return impl_->getInflationExpectations(from_date, to_date, limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
EconomyClient::getTreasuryYieldsAsync(std::string from_date,
                                      std::string to_date,
                                      std::optional<int> limit) const {
  return impl_->getTreasuryYieldsAsync(std::move(from_date), std::move(to_date), limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
EconomyClient::getInflationAsync(std::string from_date,
                                 std::string to_date,
                                 std::optional<int> limit) const {
  return impl_->getInflationAsync(std::move(from_date), std::move(to_date), limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
EconomyClient::getInflationExpectationsAsync(std::string from_date,
                                             std::string to_date,
                                             std::optional<int> limit) const {
  return impl_->getInflationExpectationsAsync(std::move(from_date), std::move(to_date), limit);
}

} // namespace data_sdk::polygon
