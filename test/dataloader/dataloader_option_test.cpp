#include <catch2/catch_test_macros.hpp>
#include "dataloader/dataloader_option.h"
#include <epoch_data_sdk/common/enums.hpp>
#include <epoch_frame/datetime.h>

// Don't import data_sdk namespace to avoid DataloaderOption conflict
using data_sdk::DataCategory;
using data_sdk::BenchmarkKind;
using data_sdk::AuxiliaryCategoryConfig;
using data_sdk::FinancialsConfig;
using data_sdk::MacroEconomicsConfig;
using data_sdk::AlternativeDataConfig;
using data_sdk::TickDataConfig;
using data_sdk::IsTimeSeriesCategory;
using data_sdk::IsIntraday;
using data_sdk::IsDaily;
using data_sdk::IsAuxiliaryCategory;
using namespace epoch_frame;

// Use the internal dataloader namespace version
using data_sdk::dataloader::DataloaderOption;

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

  SECTION("New auxiliary categories") {
    REQUIRE(IsAuxiliaryCategory(DataCategory::MacroEconomics));
    REQUIRE(IsAuxiliaryCategory(DataCategory::AlternativeData));
    REQUIRE(IsAuxiliaryCategory(DataCategory::TickData));
    REQUIRE_FALSE(IsTimeSeriesCategory(DataCategory::MacroEconomics));
    REQUIRE_FALSE(IsTimeSeriesCategory(DataCategory::AlternativeData));
    REQUIRE_FALSE(IsTimeSeriesCategory(DataCategory::TickData));
  }
}

TEST_CASE("AuxiliaryCategoryConfig - Typed configurations", "[auxiliary_config]") {
  using namespace data_sdk;

  SECTION("FinancialsConfig - Balance Sheet") {
    FinancialsConfig config{FinancialsStatementType::BalanceSheet};
    AuxiliaryCategoryConfig aux(DataCategory::Financials, config);

    REQUIRE(aux.category == DataCategory::Financials);
    REQUIRE(aux.HasTypedConfig());

    // Verify parameter conversion for fetcher compatibility
    auto params = aux.ToParameters();
    REQUIRE(params["statement_type"] == "balance_sheet");

    // Verify the typed config is accessible
    auto& financials_config = std::get<FinancialsConfig>(aux.config);
    REQUIRE(financials_config.type == FinancialsStatementType::BalanceSheet);
  }

  SECTION("FinancialsConfig - Income Statement") {
    FinancialsConfig config{FinancialsStatementType::IncomeStatement};
    AuxiliaryCategoryConfig aux(DataCategory::Financials, config);

    auto params = aux.ToParameters();
    REQUIRE(params["statement_type"] == "income_statement");

    auto& financials_config = std::get<FinancialsConfig>(aux.config);
    REQUIRE(financials_config.type == FinancialsStatementType::IncomeStatement);
  }

  SECTION("FinancialsConfig - Cash Flow") {
    FinancialsConfig config{FinancialsStatementType::CashFlow};
    AuxiliaryCategoryConfig aux(DataCategory::Financials, config);

    auto params = aux.ToParameters();
    REQUIRE(params["statement_type"] == "cash_flow");

    auto& financials_config = std::get<FinancialsConfig>(aux.config);
    REQUIRE(financials_config.type == FinancialsStatementType::CashFlow);
  }

  SECTION("FinancialsConfig - Financial Ratios") {
    FinancialsConfig config{FinancialsStatementType::FinancialRatios};
    AuxiliaryCategoryConfig aux(DataCategory::Financials, config);

    auto params = aux.ToParameters();
    REQUIRE(params["statement_type"] == "financial_ratios");

    auto& financials_config = std::get<FinancialsConfig>(aux.config);
    REQUIRE(financials_config.type == FinancialsStatementType::FinancialRatios);
  }

  SECTION("MacroEconomicsConfig - CPI") {
    MacroEconomicsConfig config{MacroEconomicsIndicator::CPI};
    AuxiliaryCategoryConfig aux(DataCategory::MacroEconomics, config);

    REQUIRE(aux.category == DataCategory::MacroEconomics);
    REQUIRE(aux.HasTypedConfig());

    auto params = aux.ToParameters();
    REQUIRE(params["indicator"] == "CPI");
  }

  SECTION("MacroEconomicsConfig - GDP") {
    MacroEconomicsConfig config{MacroEconomicsIndicator::GDP};
    AuxiliaryCategoryConfig aux(DataCategory::MacroEconomics, config);

    auto params = aux.ToParameters();
    REQUIRE(params["indicator"] == "GDP");
  }

  SECTION("MacroEconomicsConfig - Treasury 10Y") {
    MacroEconomicsConfig config{MacroEconomicsIndicator::Treasury10Y};
    AuxiliaryCategoryConfig aux(DataCategory::MacroEconomics, config);

    auto params = aux.ToParameters();
    REQUIRE(params["indicator"] == "Treasury10Y");
  }

  SECTION("AlternativeDataConfig - Form 13F") {
    AlternativeDataConfig config{AlternativeDataSource::SEC_Form13F};
    AuxiliaryCategoryConfig aux(DataCategory::AlternativeData, config);

    REQUIRE(aux.category == DataCategory::AlternativeData);
    REQUIRE(aux.HasTypedConfig());

    auto params = aux.ToParameters();
    REQUIRE(params["source"] == "form13f");
  }

  SECTION("AlternativeDataConfig - Insider Trading") {
    AlternativeDataConfig config{AlternativeDataSource::SEC_InsiderTrading};
    AuxiliaryCategoryConfig aux(DataCategory::AlternativeData, config);

    auto params = aux.ToParameters();
    REQUIRE(params["source"] == "insider_trading");
  }

  SECTION("TickDataConfig - Quotes") {
    TickDataConfig config{TickDataType::Quotes};
    AuxiliaryCategoryConfig aux(DataCategory::TickData, config);

    REQUIRE(aux.category == DataCategory::TickData);
    REQUIRE(aux.HasTypedConfig());

    auto params = aux.ToParameters();
    REQUIRE(params["tick_type"] == "quotes");
  }

  SECTION("TickDataConfig - Trades") {
    TickDataConfig config{TickDataType::Trades};
    AuxiliaryCategoryConfig aux(DataCategory::TickData, config);

    auto params = aux.ToParameters();
    REQUIRE(params["tick_type"] == "trades");
  }

  SECTION("Categories without typed config") {
    AuxiliaryCategoryConfig news(DataCategory::News);
    REQUIRE_FALSE(news.HasTypedConfig());
    REQUIRE(news.ToParameters().empty());

    AuxiliaryCategoryConfig dividends(DataCategory::Dividends);
    REQUIRE_FALSE(dividends.HasTypedConfig());
    REQUIRE(dividends.ToParameters().empty());
  }

  SECTION("Validation - correct category/config match") {
    // These should not throw
    REQUIRE_NOTHROW(AuxiliaryCategoryConfig(
        DataCategory::Financials,
        FinancialsConfig{FinancialsStatementType::BalanceSheet}));

    REQUIRE_NOTHROW(AuxiliaryCategoryConfig(
        DataCategory::MacroEconomics,
        MacroEconomicsConfig{MacroEconomicsIndicator::CPI}));

    REQUIRE_NOTHROW(AuxiliaryCategoryConfig(
        DataCategory::AlternativeData,
        AlternativeDataConfig{AlternativeDataSource::SEC_Form13F}));
  }

  SECTION("Validation - incorrect category/config match throws") {
    // Wrong config type for category
    REQUIRE_THROWS_AS(
        AuxiliaryCategoryConfig(
            DataCategory::Financials,
            MacroEconomicsConfig{MacroEconomicsIndicator::CPI}),
        std::invalid_argument);

    REQUIRE_THROWS_AS(
        AuxiliaryCategoryConfig(
            DataCategory::MacroEconomics,
            FinancialsConfig{FinancialsStatementType::BalanceSheet}),
        std::invalid_argument);

    REQUIRE_THROWS_AS(
        AuxiliaryCategoryConfig(
            DataCategory::News,
            FinancialsConfig{FinancialsStatementType::BalanceSheet}),
        std::invalid_argument);
  }
}

// Integration tests for new auxiliary categories
TEST_CASE("Integration - New auxiliary categories with fetcher",
          "[auxiliary_config][integration]") {
  SECTION("FinancialsConfig parameter conversion") {
    AuxiliaryCategoryConfig config(
        DataCategory::Financials,
        FinancialsConfig{FinancialsStatementType::IncomeStatement});

    auto params = config.ToParameters();

    REQUIRE(params.count("statement_type") == 1);
    REQUIRE(params.at("statement_type") == "income_statement");
  }

  SECTION("MacroEconomicsConfig parameter conversion") {
    AuxiliaryCategoryConfig config(
        DataCategory::MacroEconomics,
        MacroEconomicsConfig{MacroEconomicsIndicator::GDP});

    auto params = config.ToParameters();

    REQUIRE(params.count("indicator") == 1);
    REQUIRE(params.at("indicator") == "GDP");
  }

  SECTION("AlternativeDataConfig parameter conversion for Form13F") {
    AuxiliaryCategoryConfig config(
        DataCategory::AlternativeData,
        AlternativeDataConfig{AlternativeDataSource::SEC_Form13F});

    auto params = config.ToParameters();

    REQUIRE(params.count("source") == 1);
    REQUIRE(params.at("source") == "sec_form13f");
  }

  SECTION("AlternativeDataConfig parameter conversion for InsiderTrading") {
    AuxiliaryCategoryConfig config(
        DataCategory::AlternativeData,
        AlternativeDataConfig{AlternativeDataSource::SEC_InsiderTrading});

    auto params = config.ToParameters();

    REQUIRE(params.count("source") == 1);
    REQUIRE(params.at("source") == "sec_insider_trading");
  }

  SECTION("TickDataConfig parameter conversion for Quotes") {
    AuxiliaryCategoryConfig config(
        DataCategory::TickData,
        TickDataConfig{TickDataType::Quotes});

    auto params = config.ToParameters();

    REQUIRE(params.count("tick_type") == 1);
    REQUIRE(params.at("tick_type") == "quotes");
  }

  SECTION("TickDataConfig parameter conversion for Trades") {
    AuxiliaryCategoryConfig config(
        DataCategory::TickData,
        TickDataConfig{TickDataType::Trades});

    auto params = config.ToParameters();

    REQUIRE(params.count("tick_type") == 1);
    REQUIRE(params.at("tick_type") == "trades");
  }
}

TEST_CASE("Integration - Multi-category with new categories",
          "[auxiliary_config][integration]") {
  SECTION("Can combine primary DailyBars with Financials auxiliary") {
    DataloaderOption opt;
    opt.SetPrimaryCategory(DataCategory::DailyBars);
    std::vector<AuxiliaryCategoryConfig> aux_configs = {
        AuxiliaryCategoryConfig(
            DataCategory::Financials,
            FinancialsConfig{FinancialsStatementType::BalanceSheet})
    };
    opt.SetAuxiliaryCategories(aux_configs);

    REQUIRE(opt.IsMultiCategory());
    REQUIRE(opt.GetAllCategories().size() == 2);

    auto categories = opt.GetAllCategories();
    REQUIRE(std::find(categories.begin(), categories.end(), DataCategory::DailyBars)
            != categories.end());
    REQUIRE(std::find(categories.begin(), categories.end(), DataCategory::Financials)
            != categories.end());
  }

  SECTION("Can combine primary DailyBars with MacroEconomics auxiliary") {
    DataloaderOption opt;
    opt.SetPrimaryCategory(DataCategory::DailyBars);
    std::vector<AuxiliaryCategoryConfig> aux_configs = {
        AuxiliaryCategoryConfig(
            DataCategory::MacroEconomics,
            MacroEconomicsConfig{MacroEconomicsIndicator::CPI})
    };
    opt.SetAuxiliaryCategories(aux_configs);

    REQUIRE(opt.IsMultiCategory());
    REQUIRE(opt.GetAllCategories().size() == 2);
  }

  SECTION("Can combine primary DailyBars with AlternativeData auxiliary") {
    DataloaderOption opt;
    opt.SetPrimaryCategory(DataCategory::DailyBars);
    std::vector<AuxiliaryCategoryConfig> aux_configs = {
        AuxiliaryCategoryConfig(
            DataCategory::AlternativeData,
            AlternativeDataConfig{AlternativeDataSource::SEC_Form13F})
    };
    opt.SetAuxiliaryCategories(aux_configs);

    REQUIRE(opt.IsMultiCategory());
    REQUIRE(opt.GetAllCategories().size() == 2);
  }

  SECTION("Can combine primary DailyBars with TickData auxiliary") {
    DataloaderOption opt;
    opt.SetPrimaryCategory(DataCategory::DailyBars);
    std::vector<AuxiliaryCategoryConfig> aux_configs = {
        AuxiliaryCategoryConfig(
            DataCategory::TickData,
            TickDataConfig{TickDataType::Quotes})
    };
    opt.SetAuxiliaryCategories(aux_configs);

    REQUIRE(opt.IsMultiCategory());
    REQUIRE(opt.GetAllCategories().size() == 2);
  }

  SECTION("Can combine multiple new auxiliary categories") {
    DataloaderOption opt;
    opt.SetPrimaryCategory(DataCategory::DailyBars);
    std::vector<AuxiliaryCategoryConfig> aux_configs = {
        AuxiliaryCategoryConfig(
            DataCategory::Financials,
            FinancialsConfig{FinancialsStatementType::IncomeStatement}),
        AuxiliaryCategoryConfig(
            DataCategory::MacroEconomics,
            MacroEconomicsConfig{MacroEconomicsIndicator::GDP}),
        AuxiliaryCategoryConfig(
            DataCategory::AlternativeData,
            AlternativeDataConfig{AlternativeDataSource::SEC_Form13F})
    };
    opt.SetAuxiliaryCategories(aux_configs);

    REQUIRE(opt.IsMultiCategory());
    REQUIRE(opt.GetAllCategories().size() == 4);

    auto categories = opt.GetAllCategories();
    REQUIRE(std::find(categories.begin(), categories.end(), DataCategory::DailyBars)
            != categories.end());
    REQUIRE(std::find(categories.begin(), categories.end(), DataCategory::Financials)
            != categories.end());
    REQUIRE(std::find(categories.begin(), categories.end(), DataCategory::MacroEconomics)
            != categories.end());
    REQUIRE(std::find(categories.begin(), categories.end(), DataCategory::AlternativeData)
            != categories.end());
  }
}

TEST_CASE("Integration - All FinancialsConfig statement types",
          "[auxiliary_config][integration]") {
  SECTION("Balance Sheet") {
    AuxiliaryCategoryConfig config(
        DataCategory::Financials,
        FinancialsConfig{FinancialsStatementType::BalanceSheet});
    auto params = config.ToParameters();
    REQUIRE(params.at("statement_type") == "balance_sheet");
  }

  SECTION("Income Statement") {
    AuxiliaryCategoryConfig config(
        DataCategory::Financials,
        FinancialsConfig{FinancialsStatementType::IncomeStatement});
    auto params = config.ToParameters();
    REQUIRE(params.at("statement_type") == "income_statement");
  }

  SECTION("Cash Flow") {
    AuxiliaryCategoryConfig config(
        DataCategory::Financials,
        FinancialsConfig{FinancialsStatementType::CashFlow});
    auto params = config.ToParameters();
    REQUIRE(params.at("statement_type") == "cash_flow");
  }

  SECTION("Financial Ratios") {
    AuxiliaryCategoryConfig config(
        DataCategory::Financials,
        FinancialsConfig{FinancialsStatementType::FinancialRatios});
    auto params = config.ToParameters();
    REQUIRE(params.at("statement_type") == "financial_ratios");
  }
}

TEST_CASE("Integration - All MacroEconomicsConfig indicators",
          "[auxiliary_config][integration]") {
  SECTION("GDP") {
    AuxiliaryCategoryConfig config(
        DataCategory::MacroEconomics,
        MacroEconomicsConfig{MacroEconomicsIndicator::GDP});
    auto params = config.ToParameters();
    REQUIRE(params.at("indicator") == "GDP");
  }

  SECTION("CPI") {
    AuxiliaryCategoryConfig config(
        DataCategory::MacroEconomics,
        MacroEconomicsConfig{MacroEconomicsIndicator::CPI});
    auto params = config.ToParameters();
    REQUIRE(params.at("indicator") == "CPI");
  }

  SECTION("Core CPI") {
    AuxiliaryCategoryConfig config(
        DataCategory::MacroEconomics,
        MacroEconomicsConfig{MacroEconomicsIndicator::CoreCPI});
    auto params = config.ToParameters();
    REQUIRE(params.at("indicator") == "CoreCPI");
  }

  SECTION("Fed Funds Rate") {
    AuxiliaryCategoryConfig config(
        DataCategory::MacroEconomics,
        MacroEconomicsConfig{MacroEconomicsIndicator::FedFunds});
    auto params = config.ToParameters();
    REQUIRE(params.at("indicator") == "FedFunds");
  }

  SECTION("Treasury 10Y") {
    AuxiliaryCategoryConfig config(
        DataCategory::MacroEconomics,
        MacroEconomicsConfig{MacroEconomicsIndicator::Treasury10Y});
    auto params = config.ToParameters();
    REQUIRE(params.at("indicator") == "Treasury10Y");
  }

  SECTION("Unemployment Rate") {
    AuxiliaryCategoryConfig config(
        DataCategory::MacroEconomics,
        MacroEconomicsConfig{MacroEconomicsIndicator::Unemployment});
    auto params = config.ToParameters();
    REQUIRE(params.at("indicator") == "Unemployment");
  }
}
