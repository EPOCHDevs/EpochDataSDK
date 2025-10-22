#include "epoch_data_sdk/trading_economics/calendar_client.hpp"

#include "base_client.hpp"

namespace data_sdk::trading_economics {

// Private implementation using direct composition
class CalendarClient::Impl {
public:
  explicit Impl(Options options) : base_client_(std::move(options)) {}

  BaseClient base_client_;
};

// Constructor
CalendarClient::CalendarClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

// Destructor
CalendarClient::~CalendarClient() = default;

// Get economic calendar events with flexible filtering
// API: GET /calendar
Expected<epoch_frame::DataFrame>
CalendarClient::getCalendarData(std::optional<std::string> country,
                                 std::optional<std::string> category,
                                 std::optional<std::string> from_date,
                                 std::optional<std::string> to_date,
                                 std::optional<std::string> importance,
                                 std::optional<std::string> ticker,
                                 std::optional<std::string> event,
                                 std::optional<bool> values) const {
  std::string path = "/calendar";
  std::map<std::string, std::string> params;

  if (country) {
    params["country"] = *country;
  }
  if (category) {
    params["category"] = *category;
  }
  if (from_date) {
    params["d1"] = *from_date;
  }
  if (to_date) {
    params["d2"] = *to_date;
  }
  if (importance) {
    params["importance"] = *importance;
  }
  if (ticker) {
    params["ticker"] = *ticker;
  }
  if (event) {
    params["event"] = *event;
  }
  if (values.has_value() && !*values) {
    params["values"] = "false";
  }

  return impl_->base_client_.httpGetDataFrame(path, params);
}

// Get calendar events by specific calendar ID(s)
// API: GET /calendar/calendarid/{id}
Expected<epoch_frame::DataFrame>
CalendarClient::getCalendarById(const std::string& id) const {
  std::string path = "/calendar/calendarid/" + id;
  std::map<std::string, std::string> params;

  return impl_->base_client_.httpGetDataFrame(path, params);
}

// Get latest calendar updates
// API: GET /calendar/updates
Expected<epoch_frame::DataFrame>
CalendarClient::getCalendarUpdates() const {
  std::string path = "/calendar/updates";
  std::map<std::string, std::string> params;

  return impl_->base_client_.httpGetDataFrame(path, params);
}

// Get calendar events by event group
// API: GET /calendar/group/{group}
Expected<epoch_frame::DataFrame>
CalendarClient::getCalendarEventsByGroup(
    const std::string& group, std::optional<std::string> country,
    std::optional<std::string> from_date,
    std::optional<std::string> to_date) const {

  std::string path = "/calendar/group/" + group;
  std::map<std::string, std::string> params;

  if (country) {
    params["country"] = *country;
  }
  if (from_date) {
    params["d1"] = *from_date;
  }
  if (to_date) {
    params["d2"] = *to_date;
  }

  return impl_->base_client_.httpGetDataFrame(path, params);
}

// Get all calendar events or by country
// API: GET /calendar/country/{country} or GET /calendar/events
Expected<epoch_frame::DataFrame>
CalendarClient::getCalendarEvents(std::optional<std::string> country) const {
  std::string path;
  if (country) {
    path = "/calendar/country/" + *country;
  } else {
    path = "/calendar/events";
  }

  std::map<std::string, std::string> params;
  return impl_->base_client_.httpGetDataFrame(path, params);
}

} // namespace data_sdk::trading_economics
