#include <catch2/catch_test_macros.hpp>
#include "dataloader/dataloader_option.h"
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/datetime.h>

using namespace data_sdk::dataloader;
using namespace data_sdk;
using namespace epoch_frame;

class DataloaderOptionTestFixture {
public:
  DataloaderOptionTestFixture() {
    // Setup default valid option
    option.SetStartDate(DateTime::from_date_str("2024-01-01").date());
    option.SetEndDate(DateTime::from_date_str("2024-01-31").date());
  }

  DataloaderOption option;
};

TEST_CASE("DataloaderOption - Default values", "[dataloader_option]") {
  DataloaderOption option;

  SECTION("default primary category is MinuteBars") {
    REQUIRE(option.GetPrimaryCategory() == DataCategory::MinuteBars);
  }

  SECTION("default auxiliaries are empty") {
    REQUIRE(option.GetAuxiliaryCategories().empty());
  }

  SECTION("default is single category mode") {
    REQUIRE_FALSE(option.IsMultiCategory());
  }

  SECTION("backward compatibility - GetDataCategory returns primary") {
    option.SetPrimaryCategory(DataCategory::DailyBars);
    REQUIRE(option.GetDataCategory() == DataCategory::DailyBars);
  }
}

TEST_CASE("DataloaderOption - IsValid validation", "[dataloader_option]") {
  DataloaderOptionTestFixture fixture;

  SECTION("valid with MinuteBars primary, no auxiliaries") {
    fixture.option.SetPrimaryCategory(DataCategory::MinuteBars);
    REQUIRE(fixture.option.IsValid());
  }

  SECTION("valid with DailyBars primary, no auxiliaries") {
    fixture.option.SetPrimaryCategory(DataCategory::DailyBars);
    REQUIRE(fixture.option.IsValid());
  }

  SECTION("valid with MinuteBars primary and News auxiliary") {
    fixture.option.SetPrimaryCategory(DataCategory::MinuteBars);
    fixture.option.SetAuxiliaryCategories(
        std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::News)});
    REQUIRE(fixture.option.IsValid());
  }

  SECTION("valid with multiple auxiliary categories") {
    fixture.option.SetPrimaryCategory(DataCategory::MinuteBars);
    fixture.option.SetAuxiliaryCategories(std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::News), AuxiliaryCategoryConfig(DataCategory::Dividends), AuxiliaryCategoryConfig(DataCategory::Splits)});
    REQUIRE(fixture.option.IsValid());
  }

  SECTION("valid with DailyBars primary and Financials auxiliary") {
    fixture.option.SetPrimaryCategory(DataCategory::DailyBars);
    fixture.option.SetAuxiliaryCategories(
        std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::Financials)});
    REQUIRE(fixture.option.IsValid());
  }

  SECTION("invalid - News as primary (not a TimeSeries category)") {
    fixture.option.SetPrimaryCategory(DataCategory::News);
    REQUIRE_FALSE(fixture.option.IsValid());
  }

  SECTION("invalid - Dividends as primary") {
    fixture.option.SetPrimaryCategory(DataCategory::Dividends);
    REQUIRE_FALSE(fixture.option.IsValid());
  }

  SECTION("invalid - MinuteBars in auxiliaries") {
    fixture.option.SetPrimaryCategory(DataCategory::DailyBars);
    fixture.option.SetAuxiliaryCategories(
        std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::MinuteBars)});
    REQUIRE_FALSE(fixture.option.IsValid());
  }

  SECTION("invalid - DailyBars in auxiliaries") {
    fixture.option.SetPrimaryCategory(DataCategory::MinuteBars);
    fixture.option.SetAuxiliaryCategories(
        std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::DailyBars)});
    REQUIRE_FALSE(fixture.option.IsValid());
  }

  SECTION("invalid - mixing TimeSeries in auxiliaries") {
    fixture.option.SetPrimaryCategory(DataCategory::MinuteBars);
    fixture.option.SetAuxiliaryCategories(std::vector<AuxiliaryCategoryConfig>{
        AuxiliaryCategoryConfig(DataCategory::News),
        AuxiliaryCategoryConfig(DataCategory::DailyBars)  // Invalid!
    });
    REQUIRE_FALSE(fixture.option.IsValid());
  }
}

TEST_CASE("DataloaderOption - IsMultiCategory detection", "[dataloader_option]") {
  DataloaderOptionTestFixture fixture;

  SECTION("single category - no auxiliaries") {
    fixture.option.SetPrimaryCategory(DataCategory::MinuteBars);
    REQUIRE_FALSE(fixture.option.IsMultiCategory());
  }

  SECTION("multi category - one auxiliary") {
    fixture.option.SetPrimaryCategory(DataCategory::MinuteBars);
    fixture.option.SetAuxiliaryCategories(
        std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::News)});
    REQUIRE(fixture.option.IsMultiCategory());
  }

  SECTION("multi category - multiple auxiliaries") {
    fixture.option.SetPrimaryCategory(DataCategory::DailyBars);
    fixture.option.SetAuxiliaryCategories(std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::News), AuxiliaryCategoryConfig(DataCategory::Dividends), AuxiliaryCategoryConfig(DataCategory::Splits)});
    REQUIRE(fixture.option.IsMultiCategory());
  }

  SECTION("clearing auxiliaries returns to single category") {
    fixture.option.SetAuxiliaryCategories(
        std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::News)});
    REQUIRE(fixture.option.IsMultiCategory());

    fixture.option.SetAuxiliaryCategories(std::vector<AuxiliaryCategoryConfig>{});
    REQUIRE_FALSE(fixture.option.IsMultiCategory());
  }
}

TEST_CASE("DataloaderOption - GetAllCategories", "[dataloader_option]") {
  DataloaderOptionTestFixture fixture;

  SECTION("single category returns only primary") {
    fixture.option.SetPrimaryCategory(DataCategory::MinuteBars);
    auto all = fixture.option.GetAllCategories();

    REQUIRE(all.size() == 1);
    REQUIRE(all[0] == DataCategory::MinuteBars);
  }

  SECTION("multi category returns primary + auxiliaries") {
    fixture.option.SetPrimaryCategory(DataCategory::DailyBars);
    fixture.option.SetAuxiliaryCategories(std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::News), AuxiliaryCategoryConfig(DataCategory::Dividends)});

    auto all = fixture.option.GetAllCategories();

    REQUIRE(all.size() == 3);
    REQUIRE(all[0] == DataCategory::DailyBars);  // Primary first
    REQUIRE(all[1] == DataCategory::News);
    REQUIRE(all[2] == DataCategory::Dividends);
  }

  SECTION("order is primary first, then auxiliaries in order") {
    fixture.option.SetPrimaryCategory(DataCategory::MinuteBars);
    fixture.option.SetAuxiliaryCategories(std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::Splits), AuxiliaryCategoryConfig(DataCategory::News), AuxiliaryCategoryConfig(DataCategory::Dividends)});

    auto all = fixture.option.GetAllCategories();

    REQUIRE(all.size() == 4);
    REQUIRE(all[0] == DataCategory::MinuteBars);
    REQUIRE(all[1] == DataCategory::Splits);
    REQUIRE(all[2] == DataCategory::News);
    REQUIRE(all[3] == DataCategory::Dividends);
  }
}

TEST_CASE("DataloaderOption - Accessors and Mutators", "[dataloader_option]") {
  DataloaderOption option;

  SECTION("PrimaryCategory accessor/mutator") {
    option.SetPrimaryCategory(DataCategory::DailyBars);
    REQUIRE(option.GetPrimaryCategory() == DataCategory::DailyBars);

    option.SetPrimaryCategory(DataCategory::MinuteBars);
    REQUIRE(option.GetPrimaryCategory() == DataCategory::MinuteBars);
  }

  SECTION("AuxiliaryCategories accessor/mutator") {
    std::vector<AuxiliaryCategoryConfig> aux = {AuxiliaryCategoryConfig(DataCategory::News), AuxiliaryCategoryConfig(DataCategory::Dividends)};
    option.SetAuxiliaryCategories(aux);

    auto retrieved = option.GetAuxiliaryCategories();
    REQUIRE(retrieved.size() == 2);
    REQUIRE(retrieved[0].category == DataCategory::News);
    REQUIRE(retrieved[1].category == DataCategory::Dividends);
  }

  SECTION("can modify returned auxiliaries vector") {
    option.SetAuxiliaryCategories(
        std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::News)});

    // Note: GetAuxiliaryCategories() returns by value, not reference
    // So this test is checking mutator/accessor behavior
    auto retrieved = option.GetAuxiliaryCategories();
    REQUIRE(retrieved.size() == 1);
    REQUIRE(retrieved[0].category == DataCategory::News);
  }
}

TEST_CASE("DataloaderOption - Real-world scenarios", "[dataloader_option]") {

  SECTION("Intraday strategy with news and corporate actions") {
    DataloaderOption option;
    option.SetPrimaryCategory(DataCategory::MinuteBars);
    option.SetAuxiliaryCategories(std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::News), AuxiliaryCategoryConfig(DataCategory::Splits), AuxiliaryCategoryConfig(DataCategory::Dividends)});

    REQUIRE(option.IsValid());
    REQUIRE(option.IsMultiCategory());
    REQUIRE(option.GetAllCategories().size() == 4);
  }

  SECTION("Daily strategy with fundamental data") {
    DataloaderOption option;
    option.SetPrimaryCategory(DataCategory::DailyBars);
    option.SetAuxiliaryCategories(std::vector<AuxiliaryCategoryConfig>{AuxiliaryCategoryConfig(DataCategory::Financials), AuxiliaryCategoryConfig(DataCategory::ShortInterest), AuxiliaryCategoryConfig(DataCategory::ShortVolume)});

    REQUIRE(option.IsValid());
    REQUIRE(option.IsMultiCategory());
    REQUIRE(option.GetAllCategories().size() == 4);
  }

  SECTION("Simple intraday strategy - no auxiliaries") {
    DataloaderOption option;
    option.SetPrimaryCategory(DataCategory::MinuteBars);

    REQUIRE(option.IsValid());
    REQUIRE_FALSE(option.IsMultiCategory());
    REQUIRE(option.GetAllCategories().size() == 1);
  }

  SECTION("Invalid attempt to mix intraday and daily") {
    DataloaderOption option;
    option.SetPrimaryCategory(DataCategory::MinuteBars);
    option.SetAuxiliaryCategories(std::vector<AuxiliaryCategoryConfig>{
        AuxiliaryCategoryConfig(DataCategory::News),
        AuxiliaryCategoryConfig(DataCategory::DailyBars)  // Should fail validation
    });

    REQUIRE_FALSE(option.IsValid());
  }
}

TEST_CASE("DataloaderOption - Batch fetching configuration", "[dataloader_option]") {
  DataloaderOption option;

  SECTION("default batch fetching is enabled") {
    REQUIRE(option.GetUseBatchFetching() == true);
  }

  SECTION("default batch size is 10") {
    REQUIRE(option.GetBatchSize() == 10);
  }

  SECTION("can disable batch fetching") {
    option.SetUseBatchFetching(false);
    REQUIRE(option.GetUseBatchFetching() == false);
  }

  SECTION("can configure custom batch size") {
    option.SetBatchSize(25);
    REQUIRE(option.GetBatchSize() == 25);
  }

  SECTION("batch size can be set to 1 for fully sequential") {
    option.SetBatchSize(1);
    REQUIRE(option.GetBatchSize() == 1);
  }

  SECTION("batch size can be set to large value for near-concurrent") {
    option.SetBatchSize(1000);
    REQUIRE(option.GetBatchSize() == 1000);
  }
}

TEST_CASE("DataCategory helper functions", "[dataloader_option]") {

  SECTION("IsTimeSeriesCategory") {
    REQUIRE(IsTimeSeriesCategory(DataCategory::MinuteBars));
    REQUIRE(IsTimeSeriesCategory(DataCategory::DailyBars));
    REQUIRE_FALSE(IsTimeSeriesCategory(DataCategory::News));
    REQUIRE_FALSE(IsTimeSeriesCategory(DataCategory::Dividends));
    REQUIRE_FALSE(IsTimeSeriesCategory(DataCategory::Splits));
    REQUIRE_FALSE(IsTimeSeriesCategory(DataCategory::Financials));
    REQUIRE_FALSE(IsTimeSeriesCategory(DataCategory::ShortInterest));
    REQUIRE_FALSE(IsTimeSeriesCategory(DataCategory::ShortVolume));
  }

  SECTION("IsIntraday") {
    REQUIRE(IsIntraday(DataCategory::MinuteBars));
    REQUIRE_FALSE(IsIntraday(DataCategory::DailyBars));
    REQUIRE_FALSE(IsIntraday(DataCategory::News));
  }

  SECTION("IsDaily") {
    REQUIRE(IsDaily(DataCategory::DailyBars));
    REQUIRE_FALSE(IsDaily(DataCategory::MinuteBars));
    REQUIRE_FALSE(IsDaily(DataCategory::News));
  }

  SECTION("IsAuxiliaryCategory") {
    REQUIRE(IsAuxiliaryCategory(DataCategory::News));
    REQUIRE(IsAuxiliaryCategory(DataCategory::Dividends));
    REQUIRE(IsAuxiliaryCategory(DataCategory::Splits));
    REQUIRE(IsAuxiliaryCategory(DataCategory::Financials));
    REQUIRE(IsAuxiliaryCategory(DataCategory::ShortInterest));
    REQUIRE(IsAuxiliaryCategory(DataCategory::ShortVolume));
    REQUIRE_FALSE(IsAuxiliaryCategory(DataCategory::MinuteBars));
    REQUIRE_FALSE(IsAuxiliaryCategory(DataCategory::DailyBars));
  }

  SECTION("Categories are mutually exclusive") {
    // TimeSeries categories
    for (auto cat : {DataCategory::MinuteBars, DataCategory::DailyBars}) {
      REQUIRE(IsTimeSeriesCategory(cat));
      REQUIRE_FALSE(IsAuxiliaryCategory(cat));
    }

    // Auxiliary categories
    for (auto cat : {DataCategory::News, DataCategory::Dividends,
                     DataCategory::Splits, DataCategory::Financials,
                     DataCategory::ShortInterest, DataCategory::ShortVolume}) {
      REQUIRE(IsAuxiliaryCategory(cat));
      REQUIRE_FALSE(IsTimeSeriesCategory(cat));
    }
  }
}
