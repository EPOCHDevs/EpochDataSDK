#pragma once

#include <optional>
#include <string>
#include <vector>

#include <epoch_frame/dataframe.h>
#include <expected>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::trading_economics {

template <typename T> using Expected = std::expected<T, HttpError>;

// CalendarClient - Handles economic calendar events
// Critical for event-driven strategies: NFP, FOMC, GDP releases, CPI, etc.
class CalendarClient {
public:
  explicit CalendarClient(Options options);
  ~CalendarClient();

  // Prevent copying
  CalendarClient(const CalendarClient&) = delete;
  CalendarClient& operator=(const CalendarClient&) = delete;

  // Allow moving
  CalendarClient(CalendarClient&&) = default;
  CalendarClient& operator=(CalendarClient&&) = default;

  // Get economic calendar events with flexible filtering
  // Parameters:
  //   country: Country filter (optional, e.g., "United States", "all")
  //   category: Indicator category filter (optional, e.g., "Inflation Rate", "GDP")
  //   from_date: Start date in YYYY-MM-DD format (optional)
  //   to_date: End date in YYYY-MM-DD format (optional)
  //   importance: Filter by importance level: "1", "2", or "3" (optional)
  //   ticker: Trading Economics ticker filter (optional, e.g., "IJCUSA")
  //   event: Specific event name filter (optional, e.g., "GDP Growth Rate QoQ Final GDP")
  //   values: Include actual/forecast values (optional, default true)
  // Returns: DataFrame with columns: Date, Country, Category, Event, Actual, Forecast, Previous, Importance, etc.
  Expected<epoch_frame::DataFrame>
  getCalendarData(std::optional<std::string> country = std::nullopt,
                  std::optional<std::string> category = std::nullopt,
                  std::optional<std::string> from_date = std::nullopt,
                  std::optional<std::string> to_date = std::nullopt,
                  std::optional<std::string> importance = std::nullopt,
                  std::optional<std::string> ticker = std::nullopt,
                  std::optional<std::string> event = std::nullopt,
                  std::optional<bool> values = true) const;

  // Get calendar events by specific calendar ID(s)
  // Parameters:
  //   id: Single calendar ID or comma-separated list of IDs
  // Returns: DataFrame with event details for the specified ID(s)
  Expected<epoch_frame::DataFrame>
  getCalendarById(const std::string& id) const;

  // Get latest calendar updates
  // Returns: DataFrame with recently updated calendar events
  Expected<epoch_frame::DataFrame>
  getCalendarUpdates() const;

  // Get calendar events by event group
  // Parameters:
  //   group: Event group (e.g., "bonds", "inflation")
  //   country: Country filter (optional)
  //   from_date: Start date in YYYY-MM-DD format (optional)
  //   to_date: End date in YYYY-MM-DD format (optional)
  // Returns: DataFrame with events in the specified group
  Expected<epoch_frame::DataFrame>
  getCalendarEventsByGroup(const std::string& group,
                           std::optional<std::string> country = std::nullopt,
                           std::optional<std::string> from_date = std::nullopt,
                           std::optional<std::string> to_date = std::nullopt) const;

  // Get all calendar events or by country
  // Parameters:
  //   country: Country filter (optional, can be list)
  // Returns: DataFrame with all available calendar events
  Expected<epoch_frame::DataFrame>
  getCalendarEvents(std::optional<std::string> country = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::trading_economics
