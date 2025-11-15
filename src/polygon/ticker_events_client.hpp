#pragma once

#include <optional>
#include <string>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>

#include "epoch_data_sdk/common/metadata.hpp"
#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// TickerEventsClient - Handles ticker events (ticker changes, splits, etc.)
// Useful for tracking ticker symbol changes over time
class TickerEventsClient {
public:
  explicit TickerEventsClient(Options options);
  ~TickerEventsClient();

  // Prevent copying
  TickerEventsClient(const TickerEventsClient&) = delete;
  TickerEventsClient& operator=(const TickerEventsClient&) = delete;

  // Allow moving
  TickerEventsClient(TickerEventsClient&&) = default;
  TickerEventsClient& operator=(TickerEventsClient&&) = default;

  // Get ticker events for a specific ticker
  // Returns DataFrame with columns: event_type, ticker (for ticker_change events)
  // Index: DatetimeIndex from event dates
  Expected<epoch_frame::DataFrame>
  getTickerEvents(const std::string& ticker,
                  std::optional<std::string> types = std::nullopt) const;

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getTickerEventsAsync(std::string ticker,
                       std::optional<std::string> types = std::nullopt) const;

  /**
   * Get metadata about the Ticker Events endpoint including endpoint description,
   * query parameters, and output fields with their descriptions.
   * @return DataFrameMetadata struct with column types and descriptions
   */
  static data_sdk::DataFrameMetadata getMetadata();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
