#include <catch2/catch_test_macros.hpp>
#include "../src/sec/form13f_client.hpp"

using namespace data_sdk::sec;

TEST_CASE("Form13FClient - getHoldersByTicker", "[sec][13f]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    std::string json_response = R"({
      "data": [
        {
          "filedAt": "2024-02-14T16:30:00-05:00",
          "periodOfReport": "2023-12-31",
          "cik": "1067983",
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": {"value": 1, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Get holders for a ticker") {
    auto task = client.getHoldersByTicker("AAPL", 100);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total.value == 1);
    REQUIRE(response.data.size() == 1);
    REQUIRE(response.data[0].cusip == "037833100");
    REQUIRE(response.data[0].shares == 400000000);
    REQUIRE(response.data[0].value == 70000000000.0);
  }
}

TEST_CASE("Form13FClient - getHoldersByCUSIP", "[sec][13f]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify CUSIP in query
    REQUIRE(body.find("cusip:037833100") != std::string::npos);

    std::string json_response = R"({
      "data": [
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 25000000000.0,
          "shares": 142857143,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "142857143",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": {"value": 2, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Get holders by CUSIP") {
    auto task = client.getHoldersByCUSIP("037833100", 100);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total.value == 2);
    REQUIRE(response.data.size() == 2);

    // Should be sorted by value descending
    REQUIRE(response.data[0].value >= response.data[1].value);
  }
}

TEST_CASE("Form13FClient - getLargePositions", "[sec][13f]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Mock response with positions of varying sizes
    std::string json_response = R"({
      "data": [
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 5000000.0,
          "shares": 28571,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "28571",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 25000000000.0,
          "shares": 142857143,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "142857143",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": {"value": 3, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Filter large positions above $10M") {
    auto task = client.getLargePositions("AAPL", 10000000.0, 50);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();

    // Should filter out the $5M position, keeping only $70B and $25B positions
    REQUIRE(response.data.size() == 2);

    for (const auto &holding : response.data) {
      REQUIRE(holding.value >= 10000000.0);
    }
  }
}

TEST_CASE("Form13FClient - getHoldingsByInstitution", "[sec][13f]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify institution CIK in query
    REQUIRE(body.find("cik:1067983") != std::string::npos);

    std::string json_response = R"({
      "data": [
        {
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "nameOfIssuer": "Coca Cola Co",
          "titleOfClass": "COM",
          "cusip": "191216100",
          "value": 25000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": {"value": 2, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Get Berkshire Hathaway's holdings") {
    auto task = client.getHoldingsByInstitution("1067983", 100);
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto response = result.value();
    REQUIRE(response.total.value == 2);
    REQUIRE(response.data.size() == 2);

    // Should have AAPL and KO
    REQUIRE(response.data[0].cusip == "037833100");  // AAPL
    REQUIRE(response.data[1].cusip == "191216100");  // KO
  }
}
#include <catch2/catch_test_macros.hpp>
#include "../src/sec/form13f_client.hpp"

using namespace data_sdk::sec;

TEST_CASE("Form13FClient - getHoldingsDataFrame", "[sec][13f][dataframe]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    // Verify date range and ticker in query
    REQUIRE(body.find("nameOfIssuer:*AAPL*") != std::string::npos);
    REQUIRE(body.find("filingDate:[2024-01-01 TO 2024-12-31]") != std::string::npos);

    std::string json_response = R"({
      "data": [
        {
          "filedAt": "2024-02-14T16:30:00-05:00",
          "periodOfReport": "2023-12-31",
          "cik": "1067983",
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "filedAt": "2024-05-15T10:00:00-04:00",
          "periodOfReport": "2024-03-31",
          "cik": "1324404",
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 25000000000.0,
          "shares": 142857143,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "142857143",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        },
        {
          "filedAt": "2024-08-14T14:30:00-04:00",
          "periodOfReport": "2024-06-30",
          "cik": "1649339",
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 5000000000.0,
          "shares": 28571429,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "DFND",
          "votingAuthoritySole": "28571429",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": {"value": 3, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Get holdings as DataFrame for systematic trading") {
    auto result = client.getHoldingsDataFrame("AAPL", "2024-01-01", "2024-12-31");

    REQUIRE(result.has_value());
    auto df = result.value();

    // Verify shape
    REQUIRE(df.num_rows() == 3);
    REQUIRE(df.num_cols() == 6);  // Now includes period_of_report, institution_cik

    // Verify exact columns for systematic trading (NO FORWARD BIAS)
    auto cols = df.column_names();
    REQUIRE(cols.size() == 6);
    std::vector<std::string> expected_cols = {
      "period_of_report",   // Quarter end date (historical only)
      "institution_cik",    // Who filed
      "shares", "value",    // Position details
      "security_type", "investment_discretion"
    };
    for (const auto& col : expected_cols) {
      REQUIRE(std::find(cols.begin(), cols.end(), col) != cols.end());
    }

    // Index is filed_at datetime (NO FORWARD BIAS)
    // You only knew about Q4 2023 holdings on Feb 14, 2024 (when filed)
    // NOT on Dec 31, 2023 (the period_of_report)
  }
}

TEST_CASE("Form13FClient - getHoldingsDataFrame with Form13FOptions", "[sec][13f][dataframe]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    std::string json_response = R"({
      "data": [
        {
          "filedAt": "2024-02-14T16:30:00-05:00",
          "periodOfReport": "2023-12-31",
          "cik": "1067983",
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": {"value": 1, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Use Form13FOptions struct") {
    Form13FOptions form_opts;
    form_opts.ticker = "AAPL";
    form_opts.from_date = "2024-01-01";
    form_opts.to_date = "2024-12-31";

    auto result = client.getHoldingsDataFrame(form_opts);

    REQUIRE(result.has_value());
    auto df = result.value();

    REQUIRE(df.num_rows() == 1);
    REQUIRE(df.num_cols() == 6);

    // Verify essential columns
    auto cols = df.column_names();
    REQUIRE(std::find(cols.begin(), cols.end(), "shares") != cols.end());
    REQUIRE(std::find(cols.begin(), cols.end(), "value") != cols.end());
    REQUIRE(std::find(cols.begin(), cols.end(), "period_of_report") != cols.end());
    REQUIRE(std::find(cols.begin(), cols.end(), "institution_cik") != cols.end());
  }
}

TEST_CASE("Form13FClient - getHoldingsDataFrameAsync", "[sec][13f][dataframe][async]") {
  Options opts;
  opts.api_key = "test_key";
  opts.enable_rate_limiting = false;
  opts.http_post_override = [](const std::string &path,
                                const std::string &body,
                                const std::vector<std::pair<std::string, std::string>> &query) {
    std::string json_response = R"({
      "data": [
        {
          "filedAt": "2024-02-14T16:30:00-05:00",
          "periodOfReport": "2023-12-31",
          "cik": "1067983",
          "nameOfIssuer": "Apple Inc",
          "titleOfClass": "COM",
          "cusip": "037833100",
          "value": 70000000000.0,
          "shares": 400000000,
          "shOrPrn": "SH",
          "putOrCall": "",
          "investmentDiscretion": "SOLE",
          "votingAuthoritySole": "400000000",
          "votingAuthorityShared": "0",
          "votingAuthorityNone": "0"
        }
      ],
      "total": {"value": 1, "relation": "eq"}
    })";
    return Expected<std::string>(json_response);
  };

  Form13FClient client(opts);

  SECTION("Async DataFrame retrieval") {
    auto task = client.getHoldingsDataFrameAsync("AAPL", "2024-01-01", "2024-12-31");
    auto result = drogon::sync_wait(task);

    REQUIRE(result.has_value());
    auto df = result.value();

    REQUIRE(df.num_rows() == 1);
    REQUIRE(df.num_cols() == 6);

    // Verify essential columns for systematic trading
    auto cols = df.column_names();
    REQUIRE(std::find(cols.begin(), cols.end(), "shares") != cols.end());
    REQUIRE(std::find(cols.begin(), cols.end(), "value") != cols.end());
  }
}
