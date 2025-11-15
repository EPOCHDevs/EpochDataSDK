#include "polygon/splits_client.hpp"

#include <glaze/glaze.hpp>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "polygon/base_client.hpp"

namespace data_sdk::polygon {

namespace {

// Split JSON structure
struct SplitData {
  std::optional<std::string> ticker;
  std::optional<std::string> execution_date;
  std::optional<std::string> id;
  std::optional<double> split_from;
  std::optional<double> split_to;
};

struct SplitsResponse {
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<SplitData> results;
  std::string status;
};

} // namespace

// Private implementation
class SplitsClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getSplits(std::optional<std::string> ticker,
            std::optional<std::string> execution_date,
            std::optional<std::string> execution_date_gte,
            std::optional<std::string> execution_date_lte,
            std::optional<bool> reverse_split,
            std::optional<int> limit,
            std::optional<std::string> sort,
            std::optional<std::string> order) const {

    std::vector<std::pair<std::string, std::string>> q;
    if (ticker.has_value())
      q.emplace_back("ticker", *ticker);
    if (execution_date.has_value())
      q.emplace_back("execution_date", *execution_date);
    if (execution_date_gte.has_value())
      q.emplace_back("execution_date.gte", *execution_date_gte);
    if (execution_date_lte.has_value())
      q.emplace_back("execution_date.lte", *execution_date_lte);
    if (reverse_split.has_value())
      q.emplace_back("reverse_split", *reverse_split ? "true" : "false");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));
    if (sort.has_value())
      q.emplace_back("sort", *sort);
    if (order.has_value())
      q.emplace_back("order", *order);

    const std::string path = "/v3/reference/splits";
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    SplitsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse splits JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> tickers, dates, ids;
    std::vector<double> split_from, split_to, split_ratio;

    tickers.reserve(N);
    dates.reserve(N);
    ids.reserve(N);
    split_from.reserve(N);
    split_to.reserve(N);
    split_ratio.reserve(N);

    for (const auto &r : parsed.results) {
      tickers.push_back(r.ticker.value_or(""));
      dates.push_back(r.execution_date.value_or(""));
      ids.push_back(r.id.value_or(""));
      const auto from = r.split_from.value_or(1.0);
      const auto to = r.split_to.value_or(1.0);
      split_from.push_back(from);
      split_to.push_back(to);
      // Calculate split ratio (e.g., 2-for-1 = 2.0)
      split_ratio.push_back(to > 0 ? from / to : 1.0);
    }

    // Convert execution_date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "execution_date", "UTC");

    std::vector<std::string> columns = {"ticker", "id", "split_from", "split_to", "split_ratio"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(tickers),
        epoch_frame::factory::array::make_array(ids),
        epoch_frame::factory::array::make_array(split_from),
        epoch_frame::factory::array::make_array(split_to),
        epoch_frame::factory::array::make_array(split_ratio)};

    return epoch_frame::make_dataframe(index, arrays, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getSplitsAsync(std::optional<std::string> ticker,
                 std::optional<std::string> execution_date,
                 std::optional<std::string> execution_date_gte,
                 std::optional<std::string> execution_date_lte,
                 std::optional<bool> reverse_split,
                 std::optional<int> limit,
                 std::optional<std::string> sort,
                 std::optional<std::string> order) const {

    std::vector<std::pair<std::string, std::string>> q;
    if (ticker.has_value())
      q.emplace_back("ticker", *ticker);
    if (execution_date.has_value())
      q.emplace_back("execution_date", *execution_date);
    if (execution_date_gte.has_value())
      q.emplace_back("execution_date.gte", *execution_date_gte);
    if (execution_date_lte.has_value())
      q.emplace_back("execution_date.lte", *execution_date_lte);
    if (reverse_split.has_value())
      q.emplace_back("reverse_split", *reverse_split ? "true" : "false");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));
    if (sort.has_value())
      q.emplace_back("sort", *sort);
    if (order.has_value())
      q.emplace_back("order", *order);

    const std::string path = "/v3/reference/splits";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    SplitsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse splits JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> tickers, dates, ids;
    std::vector<double> split_from, split_to, split_ratio;

    tickers.reserve(N);
    dates.reserve(N);
    ids.reserve(N);
    split_from.reserve(N);
    split_to.reserve(N);
    split_ratio.reserve(N);

    for (const auto &r : parsed.results) {
      tickers.push_back(r.ticker.value_or(""));
      dates.push_back(r.execution_date.value_or(""));
      ids.push_back(r.id.value_or(""));
      const auto from = r.split_from.value_or(1.0);
      const auto to = r.split_to.value_or(1.0);
      split_from.push_back(from);
      split_to.push_back(to);
      // Calculate split ratio (e.g., 2-for-1 = 2.0)
      split_ratio.push_back(to > 0 ? from / to : 1.0);
    }

    // Convert execution_date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "execution_date", "UTC");

    std::vector<std::string> columns = {"ticker", "id", "split_from", "split_to", "split_ratio"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(tickers),
        epoch_frame::factory::array::make_array(ids),
        epoch_frame::factory::array::make_array(split_from),
        epoch_frame::factory::array::make_array(split_to),
        epoch_frame::factory::array::make_array(split_ratio)};

    co_return epoch_frame::make_dataframe(index, arrays, columns);
  }
};

// Public API
SplitsClient::SplitsClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

SplitsClient::~SplitsClient() = default;

Expected<epoch_frame::DataFrame>
SplitsClient::getSplits(std::optional<std::string> ticker,
                        std::optional<std::string> execution_date,
                        std::optional<std::string> execution_date_gte,
                        std::optional<std::string> execution_date_lte,
                        std::optional<bool> reverse_split,
                        std::optional<int> limit,
                        std::optional<std::string> sort,
                        std::optional<std::string> order) const {
  return impl_->getSplits(ticker, execution_date, execution_date_gte,
                          execution_date_lte, reverse_split, limit, sort, order);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
SplitsClient::getSplitsAsync(std::optional<std::string> ticker,
                             std::optional<std::string> execution_date,
                             std::optional<std::string> execution_date_gte,
                             std::optional<std::string> execution_date_lte,
                             std::optional<bool> reverse_split,
                             std::optional<int> limit,
                             std::optional<std::string> sort,
                             std::optional<std::string> order) const {
  return impl_->getSplitsAsync(ticker, execution_date, execution_date_gte,
                               execution_date_lte, reverse_split, limit, sort, order);
}

data_sdk::DataFrameMetadata SplitsClient::getMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "splits",
      .description = "Retrieve historical stock split events with execution dates and ratio factors. This endpoint helps analyze changes in a company's share structure over time. Massive.com uses this data for accurate price adjustments in other endpoints like the Aggregates API, enabling access to both adjusted and unadjusted historical price views. Use Cases: Historical analysis, price adjustments, data consistency, modeling.",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,
      .category_prefix = "S:",
      .columns = {
          {.id = "ticker",
           .name = "Ticker",
           .description = "Ticker symbol of the stock split",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "id",
           .name = "Split ID",
           .description = "Unique identifier for the stock split",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "split_from",
           .name = "Split From",
           .description = "Second number in the split ratio (e.g., 1 in a 2-for-1 split)",
           .type = ArrowType::FLOAT64,
           .nullable = false},
          {.id = "split_to",
           .name = "Split To",
           .description = "First number in the split ratio (e.g., 2 in a 2-for-1 split)",
           .type = ArrowType::FLOAT64,
           .nullable = false},
          {.id = "split_ratio",
           .name = "Split Ratio",
           .description = "Calculated split ratio (split_from / split_to, e.g., 2-for-1 = 2.0)",
           .type = ArrowType::FLOAT64,
           .nullable = false},
      }};
}

} // namespace data_sdk::polygon
