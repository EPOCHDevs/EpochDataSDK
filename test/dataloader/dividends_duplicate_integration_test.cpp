#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <iostream>

#include <epoch_data_sdk/dataloader/options.hpp>
#include <epoch_data_sdk/dataloader/factory.hpp>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_frame/serialization.h>

#include "../src/dataloader/api_cache_dataloader.h"

using namespace data_sdk;
using namespace data_sdk::dataloader;

TEST_CASE("Full integration: DataLoader with 30 assets - Dividends duplicate detection",
          "[integration][dataloader][dividends][duplicates]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  std::cout << "\n=== FULL INTEGRATION TEST: DataLoader + Database + 30 Assets ===\n";

  // STEP 1: Delete cache completely
  std::filesystem::path cache_dir = "test_cache_integration_dividends";
  std::filesystem::remove_all(cache_dir);
  std::cout << "1. Deleted cache directory: " << cache_dir << "\n";

  // STEP 2: Create Database with 30 DOW stocks
  std::vector<std::string> symbols = {
    "AAPL", "MSFT", "JPM", "V", "UNH", "WMT", "JNJ", "PG", "NVDA", "HD",
    "CVX", "MRK", "CSCO", "VZ", "KO", "PFE", "DIS", "AMGN", "NKE", "CRM",
    "MCD", "IBM", "CAT", "GS", "AXP", "BA", "TRV", "HON", "INTC", "DOW"
  };

  asset::AssetHashSet all_assets;
  for (const auto& symbol : symbols) {
    all_assets.insert(asset::MakeAsset({symbol + "-Stocks"}));
  }

  std::cout << "2. Created asset database with " << all_assets.size() << " assets\n";

  // STEP 3: Create DataloaderOption with ALL assets
  DataloaderOption options;
  options.startDate = epoch_frame::DateTime::from_date_str("2022-01-01").date();
  options.endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
  options.cacheDir = cache_dir;
  options.enableCache = true;
  options.categories = {DataCategory::Dividends};
  options.dataloaderAssets = all_assets;
  options.strategyAssets = all_assets;

  std::cout << "3. Created DataloaderOption:\n";
  std::cout << "   - Date range: " << options.startDate.repr() << " to " << options.endDate.repr() << "\n";
  std::cout << "   - Assets: " << options.dataloaderAssets.size() << "\n";
  std::cout << "   - Categories: Dividends\n";
  std::cout << "   - Cache: " << cache_dir << "\n";

  // STEP 4: Create DataLoader using factory function
  auto loader = CreateApiCacheDataLoader(std::move(options));
  auto* api_loader = dynamic_cast<ApiCacheDataloader*>(loader.get());
  REQUIRE(api_loader != nullptr);

  std::cout << "4. Created ApiCacheDataloader using factory function\n";

  // STEP 5: Run the full pipeline (LoadData)
  std::cout << "5. Running LoadData() pipeline...\n";
  api_loader->LoadData();
  std::cout << "   ✓ Pipeline completed\n";

  // STEP 6: Get loaded data (transformed/merged data from GetStoredData)
  auto loaded_data = api_loader->GetStoredData();
  std::cout << "6. GetStoredData() returned " << loaded_data.size() << " assets\n";

  // STEP 7: Check loaded data for duplicates
  std::cout << "\n7. Checking LOADED DATA for duplicates:\n";
  size_t loaded_duplicates = 0;

  for (const auto& [asset, df] : loaded_data) {
    std::unordered_map<int64_t, int> timestamp_counts;
    auto timestamp_view = df.index()->array().to_timestamp_view();

    for (size_t i = 0; i < df.num_rows(); ++i) {
      auto ts = timestamp_view->Value(static_cast<int64_t>(i));
      timestamp_counts[ts]++;
    }

    size_t dup_count = 0;
    for (const auto& [ts, count] : timestamp_counts) {
      if (count > 1) {
        dup_count++;
      }
    }

    if (dup_count > 0) {
      std::cout << "   ❌ " << asset.GetSymbolStr() << ": " << dup_count
                << " duplicates in LOADED data (total=" << df.num_rows() << ")\n";
      loaded_duplicates++;
    }
  }

  if (loaded_duplicates == 0) {
    std::cout << "   ✓ No duplicates in loaded data across all " << loaded_data.size() << " assets\n";
  }

  // STEP 8: Check CACHE FILES directly
  std::cout << "\n8. Checking CACHE FILES for duplicates:\n";
  size_t cache_duplicates = 0;
  size_t cache_row_mismatches = 0;

  for (const auto& [asset, loaded_df] : loaded_data) {
    auto cache_path = cache_dir / "Dividends" / "Stocks" / (asset.GetID() + ".arrow");

    if (!std::filesystem::exists(cache_path)) {
      std::cout << "   ⚠️  " << asset.GetSymbolStr() << ": Cache file not found\n";
      continue;
    }

    auto cached_result = epoch_frame::read_arrow(cache_path.string(), {.index_column = "t"});
    if (!cached_result.ok()) {
      std::cout << "   ⚠️  " << asset.GetSymbolStr() << ": Failed to read cache\n";
      continue;
    }

    auto cached_df = cached_result.MoveValueUnsafe();

    // Check for duplicates in cache
    std::unordered_map<int64_t, int> timestamp_counts;
    auto timestamp_view = cached_df.index()->array().to_timestamp_view();

    for (size_t i = 0; i < cached_df.num_rows(); ++i) {
      auto ts = timestamp_view->Value(static_cast<int64_t>(i));
      timestamp_counts[ts]++;
    }

    size_t dup_count = 0;
    for (const auto& [ts, count] : timestamp_counts) {
      if (count > 1) {
        dup_count++;
      }
    }

    if (dup_count > 0) {
      std::cout << "   ❌ " << asset.GetSymbolStr() << ": " << dup_count
                << " duplicates in CACHE (total=" << cached_df.num_rows() << ")\n";
      cache_duplicates++;
    }

    // Check if loaded data matches cache
    if (loaded_df.num_rows() != cached_df.num_rows()) {
      std::cout << "   ⚠️  " << asset.GetSymbolStr() << ": Row mismatch (loaded="
                << loaded_df.num_rows() << ", cache=" << cached_df.num_rows() << ")\n";
      cache_row_mismatches++;
    }
  }

  if (cache_duplicates == 0) {
    std::cout << "   ✓ No duplicates in cache files across all assets\n";
  }

  if (cache_row_mismatches == 0) {
    std::cout << "   ✓ All loaded data matches cache row counts\n";
  }

  // STEP 9: Summary
  std::cout << "\n9. FINAL SUMMARY:\n";
  std::cout << "   - Assets processed: " << loaded_data.size() << "\n";
  std::cout << "   - Duplicates in loaded data: " << loaded_duplicates << "\n";
  std::cout << "   - Duplicates in cache files: " << cache_duplicates << "\n";
  std::cout << "   - Row count mismatches: " << cache_row_mismatches << "\n";

  // Cleanup
  std::filesystem::remove_all(cache_dir);
  std::cout << "\n=== Test complete ===\n\n";

  // Fail if we found any issues
  if (loaded_duplicates > 0) {
    FAIL("Found duplicates in LOADED data for " << loaded_duplicates << " assets!");
  }
  if (cache_duplicates > 0) {
    FAIL("Found duplicates in CACHE files for " << cache_duplicates << " assets!");
  }
}
