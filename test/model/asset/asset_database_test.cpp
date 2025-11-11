//
// Created by dewe on 7/22/23.
//
#include <catch2/catch_test_macros.hpp>
#include <epoch_data_sdk/model/asset/asset_database.hpp>
#include <epoch_data_sdk/common/symbol.hpp>

using namespace data_sdk;
TEST_CASE("Asset Specification Database Test", "[asset_spec_db]") {
    // Load the AssetSpecificationDatabase
    const auto &db =
        asset::AssetSpecificationDatabase::GetInstance();

    REQUIRE_NOTHROW(db.GetAssetSpecification(
        Symbol{"AAPL"}, epoch_core::AssetClass::Stocks,
        epoch_core::Exchange::NASDAQ, epoch_core::CountryCurrency::USD));
    REQUIRE_NOTHROW(db.GetAssetSpecification(Symbol{"ES"},
    epoch_core::AssetClass::Futures, epoch_core::Exchange::GBLX,
                                             epoch_core::CountryCurrency::USD));
    REQUIRE_THROWS(db.GetAssetSpecification(
        Symbol{"AAPL"}, epoch_core::AssetClass::Stocks,
        epoch_core::Exchange::CME, epoch_core::CountryCurrency::USD));
}
