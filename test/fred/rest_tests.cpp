#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <epoch_data_sdk/fred/series_client.hpp>

using namespace data_sdk::fred;

static std::string getApiKey() {
  const char *env = std::getenv("FRED_API_KEY");
  return env ? std::string(env) : "";
}

static Options makeOptions() {
  Options opts;
  opts.api_key = getApiKey();
  return opts;
}

// Helper to check if we have a valid API key
static bool hasApiKey() {
  return !getApiKey().empty();
}


TEST_CASE("FRED SeriesClient - convenience methods", "[fred][series][integration]") {
  if (!hasApiKey()) {
    SKIP("FRED_API_KEY not set");
  }

  SeriesClient client(makeOptions());

  SECTION("getCPI") {
    auto df = client.getCPI("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    // With ALFRED enabled by default, we should have observation_date and value
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getFedFunds") {
    auto df = client.getFedFunds("2023-01-01", "2023-01-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getGDP") {
    auto df = client.getGDP("2022-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getUnemployment") {
    auto df = client.getUnemployment("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury10Y") {
    auto df = client.getTreasury10Y("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getCoreCPI") {
    auto df = client.getCoreCPI("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);

    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getPCE") {
    auto df = client.getPCE("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getCorePCE") {
    auto df = client.getCorePCE("2023-01-01", "2023-03-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury2Y") {
    auto df = client.getTreasury2Y("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury5Y") {
    auto df = client.getTreasury5Y("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury30Y") {
    auto df = client.getTreasury30Y("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getTreasury3M") {
    auto df = client.getTreasury3M("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getInitialClaims") {
    auto df = client.getInitialClaims("2023-01-01", "2023-01-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getNonfarmPayrolls") {
    auto df = client.getNonfarmPayrolls("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getIndustrialProduction") {
    auto df = client.getIndustrialProduction("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getConsumerSentiment") {
    auto df = client.getConsumerSentiment("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getRetailSales") {
    auto df = client.getRetailSales("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getHousingStarts") {
    auto df = client.getHousingStarts("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getM2MoneySupply") {
    auto df = client.getM2MoneySupply("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    std::vector<std::string> expected_cols = {"observation_date", "value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getSP500") {
    auto df = client.getSP500("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    // SP500 not available in ALFRED, so only value column
    std::vector<std::string> expected_cols = {"value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }

  SECTION("getVIX") {
    auto df = client.getVIX("2023-01-01", "2023-12-31");
    if (!df.has_value()) {
      FAIL(df.error().message);
    }
    REQUIRE(df->num_rows() > 0);
    // VIX not available in ALFRED, so only value column
    std::vector<std::string> expected_cols = {"value"};
    for (const auto& col : expected_cols) {
      REQUIRE(df->contains(col));
    }
  }
}

TEST_CASE("FRED SeriesClient - error handling", "[fred][series][error]") {
  Options opts;
  opts.api_key = "invalid_key_12345";
  SeriesClient client(std::move(opts));

  SECTION("Invalid API key") {
    auto df = client.getCPI("2023-01-01", "2023-12-31");
    REQUIRE_FALSE(df.has_value());
    REQUIRE(df.error().http_status == 400);
  }
}
