#include "trading_economics/calendar_client.hpp"
#include "trading_economics/options.hpp"
#include <catch2/catch_all.hpp>

using namespace data_sdk::trading_economics;

TEST_CASE("CalendarClient::getCalendarData with mock",
          "[trading_economics][calendar]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string& path,
         const std::vector<std::pair<std::string, std::string>>& /*q*/) {
        REQUIRE(path.find("/calendar") != std::string::npos);
        std::string body = R"([
          {
            "Country": "United States",
            "Category": "Inflation Rate",
            "Date": "2024-06-12",
            "Actual": 3.3,
            "Previous": 3.4,
            "Forecast": 3.4,
            "Unit": "%"
          }
        ])";
        return std::expected<std::string, HttpError>(body);
      };

  CalendarClient client(std::move(opt));
  auto res = client.getCalendarData("United States", "Inflation Rate");

  REQUIRE(res.has_value());
  auto& df = *res;
  REQUIRE(df.num_rows() == 1);
  REQUIRE(df.has_column("Country"));
  REQUIRE(df.has_column("Category"));
  REQUIRE(df.has_column("Date"));
  REQUIRE(df.has_column("Actual"));
  REQUIRE(df.has_column("Forecast"));
  REQUIRE(df.has_column("Previous"));
}

TEST_CASE("CalendarClient::getCalendarById with mock",
          "[trading_economics][calendar]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string& path,
         const std::vector<std::pair<std::string, std::string>>& /*q*/) {
        REQUIRE(path.find("/calendar/calendarid/") != std::string::npos);
        std::string body = R"([
          {
            "CalendarId": "12345",
            "Country": "United States",
            "Category": "GDP Growth Rate",
            "Date": "2024-07-30",
            "Actual": 2.8,
            "Forecast": 2.0,
            "Previous": 1.4
          }
        ])";
        return std::expected<std::string, HttpError>(body);
      };

  CalendarClient client(std::move(opt));
  auto res = client.getCalendarById("12345");

  REQUIRE(res.has_value());
  auto& df = *res;
  REQUIRE(df.num_rows() == 1);
  REQUIRE(df.has_column("CalendarId"));
  REQUIRE(df.has_column("Country"));
}

TEST_CASE("CalendarClient::getCalendarEventsByGroup with mock",
          "[trading_economics][calendar]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string& path,
         const std::vector<std::pair<std::string, std::string>>& /*q*/) {
        REQUIRE(path.find("/calendar/group/") != std::string::npos);
        std::string body = R"([
          {
            "Country": "United States",
            "Category": "10-Year Bond Auction",
            "Date": "2024-06-12",
            "Importance": 2
          }
        ])";
        return std::expected<std::string, HttpError>(body);
      };

  CalendarClient client(std::move(opt));
  auto res = client.getCalendarEventsByGroup("bonds");

  REQUIRE(res.has_value());
  auto& df = *res;
  REQUIRE(df.num_rows() == 1);
  REQUIRE(df.has_column("Category"));
}

TEST_CASE("CalendarClient handles HTTP error", "[trading_economics][calendar]") {
  Options opt;
  opt.api_key = "test";
  opt.http_get_override =
      [](const std::string& /*path*/,
         const std::vector<std::pair<std::string, std::string>>& /*q*/) {
        HttpError err;
        err.http_status = 401;
        err.message = "Unauthorized";
        return std::expected<std::string, HttpError>(std::unexpected(err));
      };

  CalendarClient client(std::move(opt));
  auto res = client.getCalendarData();

  REQUIRE(!res.has_value());
  REQUIRE(res.error().http_status == 401);
}
