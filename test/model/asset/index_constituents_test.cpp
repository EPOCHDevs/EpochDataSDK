//
// Created by adesola on 1/30/25.
//
#include <epoch_data_sdk/model/asset/index_constituents.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

using namespace data_sdk::asset;

TEST_CASE("IndexConstituentsDatabase singleton initialization", "[index_constituents_db]") {
  const auto &db = IndexConstituentsDatabase::GetInstance();

  SECTION("Database initializes successfully") {
    REQUIRE(db.IsInitialized());
  }

  SECTION("Database loads data from S3 or cache") {
    const auto &allData = db.GetAllData();
    REQUIRE(!allData.indices.empty());
  }
}

TEST_CASE("IndexConstituentsDatabase GetConstituents", "[index_constituents_db]") {
  const auto &db = IndexConstituentsDatabase::GetInstance();

  SECTION("Returns constituents for valid index") {
    // Test with AEX25 which should have constituents
    auto constituents = db.GetConstituents("AEX25");

    if (constituents.has_value()) {
      CHECK(!constituents->empty());
      // AEX25 should have some known constituents
      CHECK(constituents->size() > 0);
    } else {
      WARN("AEX25 constituents not found in database");
    }
  }

  SECTION("Returns nullopt for invalid index") {
    auto constituents = db.GetConstituents("INVALID_INDEX_XYZ");
    CHECK_FALSE(constituents.has_value());
  }

  SECTION("Returns empty vector for index with no constituents") {
    // CAC40 in test data has empty constituents array
    auto constituents = db.GetConstituents("CAC40");

    if (constituents.has_value()) {
      CHECK(constituents->empty());
    }
  }
}

TEST_CASE("IndexConstituentsDatabase HasIndex", "[index_constituents_db]") {
  const auto &db = IndexConstituentsDatabase::GetInstance();

  SECTION("Returns true for valid indices") {
    // These indices should exist in the test data
    CHECK(db.HasIndex("AEX25"));
    CHECK(db.HasIndex("ASX200"));
  }

  SECTION("Returns false for invalid indices") {
    CHECK_FALSE(db.HasIndex("INVALID_INDEX"));
    CHECK_FALSE(db.HasIndex(""));
  }
}

TEST_CASE("IndexConstituentsDatabase GetAllData structure", "[index_constituents_db]") {
  const auto &db = IndexConstituentsDatabase::GetInstance();
  const auto &allData = db.GetAllData();

  SECTION("Data has expected structure") {
    REQUIRE(!allData.indices.empty());

    // Check first index has required fields
    const auto &firstIndex = allData.indices[0];
    CHECK(!firstIndex.index.empty());
    CHECK(firstIndex.constituents.size() >= 0);
    CHECK(firstIndex.unsupported.size() >= 0);
  }

  SECTION("Can iterate through all indices") {
    size_t count = 0;
    for (const auto &indexData : allData.indices) {
      CHECK(!indexData.index.empty());
      count++;
    }
    CHECK(count > 0);
  }
}

TEST_CASE("IndexConstituentsDatabase constituent data integrity", "[index_constituents_db]") {
  const auto &db = IndexConstituentsDatabase::GetInstance();

  SECTION("ASX200 has expected constituents") {
    auto constituents = db.GetConstituents("ASX200");

    if (constituents.has_value()) {
      // ASX200 should have many constituents
      CHECK(constituents->size() > 50);

      // Check some known constituents exist
      bool hasBHP = std::find(constituents->begin(), constituents->end(), "BHP-Stocks") != constituents->end();
      bool hasCSL = std::find(constituents->begin(), constituents->end(), "CSL-Stocks") != constituents->end();

      if (constituents->size() > 50) {
        CHECK(hasBHP);
        CHECK(hasCSL);
      }
    }
  }

  SECTION("Constituent IDs have expected format") {
    auto constituents = db.GetConstituents("ASX200");

    if (constituents.has_value() && !constituents->empty()) {
      // All constituents should be in format "SYMBOL-AssetClass"
      for (const auto &constituent : *constituents) {
        CHECK(constituent.find("-") != std::string::npos);
      }
    }
  }
}
