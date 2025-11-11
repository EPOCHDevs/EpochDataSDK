#include <catch2/catch_test_macros.hpp>
#include "../src/sec/query_client.hpp"
#include "../src/sec/enums.hpp"

using namespace data_sdk::sec;

TEST_CASE("QueryClient - getFilingsByTicker", "[sec][query]") {
  // Create test options with mock override
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Mock successful response with actual API structure
    std::string json_response = R"({
      "filings": [
        {
          "id": "test123",
          "accessionNo": "0000320193-23-000006",
          "cik": "320193",
          "ticker": "AAPL",
          "companyName": "Apple Inc.",
          "companyNameLong": "Apple Inc.",
          "formType": "10-K",
          "filedAt": "2023-02-03T18:53:21-05:00",
          "periodOfReport": "2022-12-31",
          "description": "",
          "linkToFilingDetails": "https://www.sec.gov/...",
          "linkToTxt": "",
          "linkToHtml": "",
          "linkToXbrl": "",
          "filingUrl": "",
          "documentFormatFilesUrl": "",
          "documentFormatFiles": [],
          "dataFiles": [],
          "seriesAndClassesContractsInformation": [],
          "entities": []
        }
      ],
      "total": {"value": 1, "relation": "eq"},
      "query": {"from": 0, "size": 10}
    })";
    return Expected<std::string>(json_response);
  };

  QueryClient client(opts);

  SECTION("Successful query by ticker") {
    auto task = client.getFilingsByTicker("AAPL", "10-K", 10);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total.value == 1);
    REQUIRE(response.filings.size() == 1);
    REQUIRE(response.filings[0].ticker == "AAPL");
    REQUIRE(response.filings[0].formType == "10-K");
  }
}

TEST_CASE("QueryClient - getFilingsByCIK", "[sec][query]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    std::string json_response = R"({
      "filings": [],
      "total": {"value": 0, "relation": "eq"},
      "query": {"from": 0, "size": 10}
    })";
    return Expected<std::string>(json_response);
  };

  QueryClient client(opts);

  SECTION("Query by CIK") {
    auto task = client.getFilingsByCIK("320193", "", 10);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total.value == 0);
  }
}

TEST_CASE("QueryClient - FormType enum", "[sec][query][enum]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify the body contains the correct form type string
    REQUIRE(body.find("10-K") != std::string::npos);

    std::string json_response = R"({
      "filings": [{
        "id": "test123",
        "accessionNo": "0000320193-23-000006",
        "cik": "320193",
        "ticker": "AAPL",
        "companyName": "Apple Inc.",
        "companyNameLong": "Apple Inc.",
        "formType": "10-K",
        "filedAt": "2023-02-03T18:53:21-05:00",
        "periodOfReport": "2022-12-31",
        "description": "",
        "linkToFilingDetails": "https://www.sec.gov/...",
        "linkToTxt": "",
        "linkToHtml": "",
        "linkToXbrl": "",
        "filingUrl": "",
        "documentFormatFilesUrl": "",
        "documentFormatFiles": [],
        "dataFiles": [],
        "seriesAndClassesContractsInformation": [],
        "entities": []
      }],
      "total": {"value": 1, "relation": "eq"},
      "query": {"from": 0, "size": 10}
    })";
    return Expected<std::string>(json_response);
  };

  QueryClient client(opts);

  SECTION("Use enum FormType::TenK") {
    auto task = client.getFilingsByTicker("AAPL", epoch_core::FormType::TenK, 10);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.filings[0].formType == "10-K");
  }
}

TEST_CASE("FormType enum to string conversions", "[sec][enum]") {
  using epoch_core::FormType;

  SECTION("Common periodic reports") {
    REQUIRE(formTypeToString(FormType::TenK) == "10-K");
    REQUIRE(formTypeToString(FormType::TenKA) == "10-K/A");
    REQUIRE(formTypeToString(FormType::TenQ) == "10-Q");
    REQUIRE(formTypeToString(FormType::EightK) == "8-K");
  }

  SECTION("Registration statements") {
    REQUIRE(formTypeToString(FormType::S1) == "S-1");
    REQUIRE(formTypeToString(FormType::S3) == "S-3");
    REQUIRE(formTypeToString(FormType::S4) == "S-4");
  }

  SECTION("Proxy statements") {
    REQUIRE(formTypeToString(FormType::DEF14A) == "DEF 14A");
    REQUIRE(formTypeToString(FormType::SC13D) == "SC 13D");
  }

  SECTION("Insider trading") {
    REQUIRE(formTypeToString(FormType::Form3) == "3");
    REQUIRE(formTypeToString(FormType::Form4) == "4");
    REQUIRE(formTypeToString(FormType::Form144) == "144");
  }

  SECTION("Foreign issuers") {
    REQUIRE(formTypeToString(FormType::TwentyF) == "20-F");
    REQUIRE(formTypeToString(FormType::FortyF) == "40-F");
    REQUIRE(formTypeToString(FormType::SixK) == "6-K");
  }
}
