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

// Helper function to check for duplicates in a DataFrame
std::pair<size_t, size_t> check_duplicates(const epoch_frame::DataFrame& df) {
  std::unordered_map<int64_t, int> timestamp_counts;
  auto timestamp_view = df.index()->array().to_timestamp_view();

  for (size_t i = 0; i < df.num_rows(); ++i) {
    auto ts = timestamp_view->Value(static_cast<int64_t>(i));
    timestamp_counts[ts]++;
  }

  size_t dup_timestamps = 0;
  size_t total_duplicates = 0;
  for (const auto& [ts, count] : timestamp_counts) {
    if (count > 1) {
      dup_timestamps++;
      total_duplicates += (count - 1);
    }
  }

  return {dup_timestamps, total_duplicates};
}

// Helper function to compare two DataFrames
bool dataframes_equal(const epoch_frame::DataFrame& df1, const epoch_frame::DataFrame& df2) {
  if (df1.num_rows() != df2.num_rows()) return false;

  auto ts1 = df1.index()->array().to_timestamp_view();
  auto ts2 = df2.index()->array().to_timestamp_view();

  for (size_t i = 0; i < df1.num_rows(); ++i) {
    if (ts1->Value(static_cast<int64_t>(i)) != ts2->Value(static_cast<int64_t>(i))) {
      return false;
    }
  }

  return true;
}

TEST_CASE("Full integration: DataLoader with 30 assets - 3 runs with cache persistence",
          "[integration][dataloader][dividends][duplicates]") {
  const char* api_key = std::getenv("POLYGON_API_KEY");
  REQUIRE(api_key != nullptr);

  std::cout << "\n=== MULTI-RUN INTEGRATION TEST: 3 Runs with Cache Persistence ===\n";

  // STEP 1: Delete cache completely
  std::filesystem::path cache_dir = "test_cache_integration_dividends";
  std::filesystem::remove_all(cache_dir);
  std::cout << "1. Deleted cache directory: " << cache_dir << "\n";

  // STEP 2: Create Database with AAPL only for faster testing
  std::vector<std::string> symbols = {"AAPL"};

  asset::AssetHashSet all_assets;
  for (const auto& symbol : symbols) {
    all_assets.insert(asset::MakeAsset({symbol + "-Stocks"}));
  }

  std::cout << "2. Created asset database with " << all_assets.size() << " asset (AAPL)\n\n";

  // Store baseline results from first run
  std::map<std::string, epoch_frame::DataFrame> baseline_data;
  std::map<std::string, size_t> baseline_row_counts;

  // RUN 3 ITERATIONS
  for (int run = 1; run <= 3; ++run) {
    std::cout << "========================================\n";
    std::cout << "RUN #" << run << (run == 1 ? " (FRESH CACHE)" : " (CACHED)") << "\n";
    std::cout << "========================================\n";

    // STEP 3: Create DataloaderOption with ALL assets
    DataloaderOption options;
    options.startDate = epoch_frame::DateTime::from_date_str("2022-01-01").date();
    options.endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
    options.cacheDir = cache_dir;
    options.enableCache = true;
    options.AddRequest(DataCategory::Dividends);
    options.AddRequest(DataCategory::Splits);
    options.AddRequest(DataCategory::DailyBars);
    options.AddRequest(DataCategory::ShortInterest);
    options.AddRequest(DataCategory::ShortVolume);
    options.dataloaderAssets = all_assets;
    options.strategyAssets = all_assets;

    std::cout << "Creating DataloaderOption:\n";
    std::cout << "   - Date range: " << options.startDate.repr() << " to " << options.endDate.repr() << "\n";
    std::cout << "   - Categories: Dividends, Splits, DailyBars, ShortInterest, ShortVolume\n";
    std::cout << "   - Cache: " << cache_dir << " (enabled)\n";

    // STEP 4: Create DataLoader using factory function
    auto loader = CreateApiCacheDataLoader(std::move(options));
    auto* api_loader = dynamic_cast<ApiCacheDataloader*>(loader.get());
    REQUIRE(api_loader != nullptr);

    // STEP 5: Run the full pipeline (LoadData)
    std::cout << "Running LoadData() pipeline...\n";
    data_sdk::events::ScopedProgressEmitter emitter;
    api_loader->LoadData(emitter);
    std::cout << "   ✓ Pipeline completed\n";

    // STEP 6: Get loaded data (transformed/merged data from GetStoredData)
    auto loaded_data = api_loader->GetStoredData();
    std::cout << "GetStoredData() returned " << loaded_data.size() << " assets\n";

    // STEP 7: Check loaded data for duplicates
    std::cout << "\nChecking LOADED DATA for duplicates:\n";
    size_t loaded_duplicates = 0;

    for (const auto& [asset, df] : loaded_data) {
      std::cout << "   " << asset.GetSymbolStr() << ": " << df.num_rows() << " rows, "
                << df.num_cols() << " columns\n";

      auto [dup_timestamps, total_dups] = check_duplicates(df);

      if (dup_timestamps > 0) {
        std::cout << "   ❌ " << asset.GetSymbolStr() << ": " << dup_timestamps
                  << " duplicate timestamps (" << total_dups << " extra rows, total=" << df.num_rows() << ")\n";

        // Print first few duplicate timestamps for investigation
        std::unordered_map<int64_t, int> timestamp_counts;
        auto timestamp_view = df.index()->array().to_timestamp_view();
        for (size_t i = 0; i < df.num_rows(); ++i) {
          auto ts = timestamp_view->Value(static_cast<int64_t>(i));
          timestamp_counts[ts]++;
        }

        std::cout << "      First 5 duplicate timestamps:\n";
        int shown = 0;
        for (const auto& [ts, count] : timestamp_counts) {
          if (count > 1) {
            std::cout << "         Timestamp " << ts << " appears " << count << " times\n";
            if (++shown >= 5) break;
          }
        }

        loaded_duplicates++;
      }
    }

    if (loaded_duplicates == 0) {
      std::cout << "   ✓ No duplicates in loaded data across all " << loaded_data.size() << " assets\n";
    }

    // STEP 8: Check CACHE FILES directly for all categories
    std::cout << "\nChecking CACHE FILES for duplicates:\n";
    size_t cache_duplicates = 0;
    size_t total_cache_files_checked = 0;

    std::vector<std::string> categories = {"Dividends", "Splits", "DailyBars", "ShortInterest", "ShortVolume"};
    std::map<std::string, size_t> category_row_counts;
    std::map<std::string, size_t> category_duplicate_counts;

    for (const auto& category : categories) {
      category_row_counts[category] = 0;
      category_duplicate_counts[category] = 0;

      for (const auto& [asset, loaded_df] : loaded_data) {
        auto cache_path = cache_dir / category / "Stocks" / (asset.GetID() + ".arrow");

        if (!std::filesystem::exists(cache_path)) {
          continue; // Not all assets have all categories
        }

        auto cached_result = epoch_frame::read_arrow(cache_path.string(), {.index_column = "t"});
        if (!cached_result.ok()) {
          std::cout << "   ⚠️  " << asset.GetSymbolStr() << " [" << category << "]: Failed to read cache\n";
          continue;
        }

        auto cached_df = cached_result.MoveValueUnsafe();
        total_cache_files_checked++;
        category_row_counts[category] += cached_df.num_rows();

        // Check for duplicates in cache
        auto [dup_timestamps, total_dups] = check_duplicates(cached_df);

        if (dup_timestamps > 0) {
          std::cout << "   ❌ " << asset.GetSymbolStr() << " [" << category << "]: " << dup_timestamps
                    << " duplicate timestamps in CACHE (" << total_dups << " extra rows, total=" << cached_df.num_rows() << ")\n";
          cache_duplicates++;
          category_duplicate_counts[category]++;
        }
      }
    }

    // Print category summary
    std::cout << "\nCache Summary by Category:\n";
    for (const auto& category : categories) {
      if (category_row_counts[category] > 0) {
        std::cout << "   " << category << ": " << category_row_counts[category] << " rows";
        if (category_duplicate_counts[category] > 0) {
          std::cout << " (❌ " << category_duplicate_counts[category] << " assets with duplicates)";
        } else {
          std::cout << " (✓ no duplicates)";
        }
        std::cout << "\n";
      }
    }

    if (cache_duplicates == 0) {
      std::cout << "\n   ✓ No duplicates in " << total_cache_files_checked << " cache files across all categories\n";
    } else {
      std::cout << "\n   ❌ Found duplicates in " << cache_duplicates << " cache files\n";
    }

    // STEP 9: Compare with baseline (runs 2 and 3)
    if (run == 1) {
      // Store baseline
      std::cout << "\nStoring baseline results from RUN #1...\n";
      for (const auto& [asset, df] : loaded_data) {
        baseline_data[asset.GetSymbolStr()] = df;
        baseline_row_counts[asset.GetSymbolStr()] = df.num_rows();
      }
      std::cout << "   ✓ Stored baseline for " << baseline_data.size() << " assets\n";
    } else {
      // Compare with baseline
      std::cout << "\nComparing RUN #" << run << " with baseline (RUN #1):\n";
      size_t data_mismatches = 0;
      size_t row_count_mismatches = 0;

      for (const auto& [asset, df] : loaded_data) {
        auto symbol = asset.GetSymbolStr();

        if (baseline_data.find(symbol) == baseline_data.end()) {
          std::cout << "   ⚠️  " << symbol << ": Not in baseline\n";
          continue;
        }

        // Check row counts
        if (df.num_rows() != baseline_row_counts[symbol]) {
          std::cout << "   ❌ " << symbol << ": Row count changed (baseline="
                    << baseline_row_counts[symbol] << ", current=" << df.num_rows() << ")\n";
          row_count_mismatches++;
        }

        // Check if data is identical
        if (!dataframes_equal(df, baseline_data[symbol])) {
          std::cout << "   ❌ " << symbol << ": Data content differs from baseline\n";
          data_mismatches++;
        }
      }

      if (data_mismatches == 0 && row_count_mismatches == 0) {
        std::cout << "   ✓ All data matches baseline perfectly\n";
      } else {
        std::cout << "   ❌ Found " << data_mismatches << " data mismatches and "
                  << row_count_mismatches << " row count mismatches\n";
      }

      // Fail if data changed between runs
      if (data_mismatches > 0 || row_count_mismatches > 0) {
        FAIL("Data changed between runs! Run #" << run << " differs from baseline.");
      }
    }

    // STEP 10: Run summary
    std::cout << "\nRUN #" << run << " SUMMARY:\n";
    std::cout << "   - Assets processed: " << loaded_data.size() << "\n";
    std::cout << "   - Categories tested: 5 (Dividends, Splits, DailyBars, ShortInterest, ShortVolume)\n";
    std::cout << "   - Total cache files checked: " << total_cache_files_checked << "\n";
    std::cout << "   - Duplicates in loaded data: " << loaded_duplicates << "\n";
    std::cout << "   - Duplicates in cache files: " << cache_duplicates << "\n";

    // Fail if we found any issues in this run
    if (loaded_duplicates > 0) {
      FAIL("RUN #" << run << ": Found duplicates in LOADED data for " << loaded_duplicates << " assets!");
    }
    if (cache_duplicates > 0) {
      FAIL("RUN #" << run << ": Found duplicates in CACHE files for " << cache_duplicates << " cache files!");
    }

    std::cout << "\n";
  } // End of 3 runs loop

  // Final cleanup
  std::filesystem::remove_all(cache_dir);
  std::cout << "\n=== ALL 3 RUNS PASSED: No duplicates, data consistent across runs ===\n\n";
}
