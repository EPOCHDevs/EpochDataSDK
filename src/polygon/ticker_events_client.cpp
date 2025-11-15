#include "polygon/ticker_events_client.hpp"

#include <glaze/glaze.hpp>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "polygon/base_client.hpp"

namespace data_sdk::polygon {

namespace {

// Ticker change event details
struct TickerChangeData {
  std::optional<std::string> ticker;
};

// Generic event structure
struct EventData {
  std::optional<std::string> type;
  std::optional<std::string> date;
  std::optional<TickerChangeData> ticker_change;
};

// Response structure
struct TickerEventsResults {
  std::optional<std::string> name;
  std::optional<std::string> composite_figi;
  std::optional<std::string> cik;
  std::vector<EventData> events;
};

struct TickerEventsResponse {
  std::optional<TickerEventsResults> results;
  std::string request_id;
  std::string status;
};

} // namespace

// Private implementation
class TickerEventsClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getTickerEvents(const std::string& ticker,
                  std::optional<std::string> types) const {

    std::vector<std::pair<std::string, std::string>> q;
    if (types.has_value())
      q.emplace_back("types", *types);

    const std::string path = "/vX/reference/tickers/" + ticker + "/events";
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    TickerEventsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse ticker events JSON response", nullptr);
    }

    // Handle case where results might be empty
    if (!parsed.results.has_value() || parsed.results->events.empty()) {
      // Return empty DataFrame with correct structure
      auto empty_index = epoch_frame::factory::index::make_datetime_index(
          std::vector<int64_t>{}, "date", "UTC");
      std::vector<std::string> columns = {"event_type", "ticker", "name", "composite_figi", "cik"};
      std::vector<arrow::ChunkedArrayPtr> arrays{
          epoch_frame::factory::array::make_array(std::vector<std::string>{}),
          epoch_frame::factory::array::make_array(std::vector<std::string>{}),
          epoch_frame::factory::array::make_array(std::vector<std::string>{}),
          epoch_frame::factory::array::make_array(std::vector<std::string>{}),
          epoch_frame::factory::array::make_array(std::vector<std::string>{})};
      return epoch_frame::make_dataframe(empty_index, arrays, columns);
    }

    const auto& events = parsed.results->events;
    const auto N = events.size();
    std::vector<std::string> event_dates, event_types, tickers, names, composite_figis, ciks;

    event_dates.reserve(N);
    event_types.reserve(N);
    tickers.reserve(N);
    names.reserve(N);
    composite_figis.reserve(N);
    ciks.reserve(N);

    // Extract company metadata (same for all events)
    const auto company_name = parsed.results->name.value_or("");
    const auto company_figi = parsed.results->composite_figi.value_or("");
    const auto company_cik = parsed.results->cik.value_or("");

    for (const auto& event : events) {
      event_dates.push_back(event.date.value_or(""));
      event_types.push_back(event.type.value_or(""));

      // Extract ticker from ticker_change event
      if (event.ticker_change.has_value() && event.ticker_change->ticker.has_value()) {
        tickers.push_back(*event.ticker_change->ticker);
      } else {
        tickers.push_back("");
      }

      // Add company metadata (repeats for each event)
      names.push_back(company_name);
      composite_figis.push_back(company_figi);
      ciks.push_back(company_cik);
    }

    // Convert date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(event_dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "date", "UTC");

    std::vector<std::string> columns = {"event_type", "ticker", "name", "composite_figi", "cik"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(event_types),
        epoch_frame::factory::array::make_array(tickers),
        epoch_frame::factory::array::make_array(names),
        epoch_frame::factory::array::make_array(composite_figis),
        epoch_frame::factory::array::make_array(ciks)};

    return epoch_frame::make_dataframe(index, arrays, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTickerEventsAsync(std::string ticker,
                       std::optional<std::string> types) const {

    std::vector<std::pair<std::string, std::string>> q;
    if (types.has_value())
      q.emplace_back("types", *types);

    const std::string path = "/vX/reference/tickers/" + ticker + "/events";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    TickerEventsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse ticker events JSON response", nullptr);
    }

    // Handle case where results might be empty
    if (!parsed.results.has_value() || parsed.results->events.empty()) {
      // Return empty DataFrame with correct structure
      auto empty_index = epoch_frame::factory::index::make_datetime_index(
          std::vector<int64_t>{}, "date", "UTC");
      std::vector<std::string> columns = {"event_type", "ticker", "name", "composite_figi", "cik"};
      std::vector<arrow::ChunkedArrayPtr> arrays{
          epoch_frame::factory::array::make_array(std::vector<std::string>{}),
          epoch_frame::factory::array::make_array(std::vector<std::string>{}),
          epoch_frame::factory::array::make_array(std::vector<std::string>{}),
          epoch_frame::factory::array::make_array(std::vector<std::string>{}),
          epoch_frame::factory::array::make_array(std::vector<std::string>{})};
      co_return epoch_frame::make_dataframe(empty_index, arrays, columns);
    }

    const auto& events = parsed.results->events;
    const auto N = events.size();
    std::vector<std::string> event_dates, event_types, tickers, names, composite_figis, ciks;

    event_dates.reserve(N);
    event_types.reserve(N);
    tickers.reserve(N);
    names.reserve(N);
    composite_figis.reserve(N);
    ciks.reserve(N);

    // Extract company metadata (same for all events)
    const auto company_name = parsed.results->name.value_or("");
    const auto company_figi = parsed.results->composite_figi.value_or("");
    const auto company_cik = parsed.results->cik.value_or("");

    for (const auto& event : events) {
      event_dates.push_back(event.date.value_or(""));
      event_types.push_back(event.type.value_or(""));

      // Extract ticker from ticker_change event
      if (event.ticker_change.has_value() && event.ticker_change->ticker.has_value()) {
        tickers.push_back(*event.ticker_change->ticker);
      } else {
        tickers.push_back("");
      }

      // Add company metadata (repeats for each event)
      names.push_back(company_name);
      composite_figis.push_back(company_figi);
      ciks.push_back(company_cik);
    }

    // Convert date strings to nanosecond timestamps
    auto timestamps = parseDateStringsToMidnightUTC(event_dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "date", "UTC");

    std::vector<std::string> columns = {"event_type", "ticker", "name", "composite_figi", "cik"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(event_types),
        epoch_frame::factory::array::make_array(tickers),
        epoch_frame::factory::array::make_array(names),
        epoch_frame::factory::array::make_array(composite_figis),
        epoch_frame::factory::array::make_array(ciks)};

    co_return epoch_frame::make_dataframe(index, arrays, columns);
  }
};

// Public API
TickerEventsClient::TickerEventsClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

TickerEventsClient::~TickerEventsClient() = default;

Expected<epoch_frame::DataFrame>
TickerEventsClient::getTickerEvents(const std::string& ticker,
                                    std::optional<std::string> types) const {
  return impl_->getTickerEvents(ticker, types);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
TickerEventsClient::getTickerEventsAsync(std::string ticker,
                                         std::optional<std::string> types) const {
  return impl_->getTickerEventsAsync(std::move(ticker), types);
}

data_sdk::DataFrameMetadata TickerEventsClient::getMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "ticker_events",
      .description = "Retrieve a timeline of key events associated with a given ticker, CUSIP, or Composite FIGI. This experimental endpoint highlights ticker changes such as symbol renaming or rebranding, helping users maintain continuity in their records and analyses.",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,
      .category_prefix = "TE:",
      .columns = {
          {.id = "event_type",
           .name = "Event Type",
           .description = "Type of event (e.g., \"ticker_change\")",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "ticker",
           .name = "Ticker",
           .description = "New ticker symbol for ticker_change events",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "name",
           .name = "Name",
           .description = "Name of the asset",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "composite_figi",
           .name = "Composite FIGI",
           .description = "Composite FIGI identifier for the asset",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "cik",
           .name = "CIK",
           .description = "Central Index Key (CIK) number for SEC filings",
           .type = ArrowType::STRING,
           .nullable = true},
      }};
}

} // namespace data_sdk::polygon
