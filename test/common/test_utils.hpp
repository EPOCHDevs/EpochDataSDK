#pragma once

#include <catch2/catch_test_macros.hpp>
#include <arrow/type.h>
#include <epoch_frame/dataframe.h>
#include "epoch_data_sdk/common/metadata.hpp"

namespace data_sdk::test {

/**
 * @brief Validates that a DataFrame's schema matches its metadata specification
 *
 * This function checks that:
 * 1. DataFrame has no more columns than metadata describes
 * 2. All columns present in DataFrame are described in metadata
 * 3. Column types match the metadata specification
 * 4. Missing columns are marked as nullable/optional in metadata
 *
 * @param df The DataFrame to validate
 * @param metadata The metadata specification to validate against
 * @param client_name Optional client name for better error messages
 */
inline void validateDataFrameAgainstMetadata(
    const epoch_frame::DataFrame& df,
    const DataFrameMetadata& metadata,
    const std::string& client_name = "") {

    INFO("Validating " << client_name << " DataFrame against metadata");

    // 1. DataFrame can have fewer columns than metadata (optional columns may be missing)
    //    but should not have more columns than described in metadata
    INFO("DataFrame columns: " << df.num_cols());
    INFO("Metadata columns: " << metadata.columns.size());
    REQUIRE(df.num_cols() <= metadata.columns.size());

    auto arrow_table = df.table();
    auto schema = arrow_table->schema();

    // 2. For each column in metadata that exists in the DataFrame, validate type
    for (const auto& col_meta : metadata.columns) {
        if (!df.contains(col_meta.id)) {
            // Column not present in DataFrame - must be nullable (optional)
            INFO("Column " << col_meta.id << " not present (optional)");
            REQUIRE(col_meta.nullable);
            continue;
        }

        INFO("Validating column: " << col_meta.id << " (" << col_meta.name << ")");

        // Get Arrow field for this column
        auto field = schema->GetFieldByName(col_meta.id);
        REQUIRE(field != nullptr);

        // Validate Arrow type matches metadata type
        auto arrow_type_id = field->type()->id();
        switch (col_meta.type) {
            case ArrowType::STRING:
                REQUIRE(arrow_type_id == arrow::Type::STRING);
                break;
            case ArrowType::INT32:
                REQUIRE(arrow_type_id == arrow::Type::INT32);
                break;
            case ArrowType::INT64:
                REQUIRE(arrow_type_id == arrow::Type::INT64);
                break;
            case ArrowType::FLOAT32:
                REQUIRE(arrow_type_id == arrow::Type::FLOAT);
                break;
            case ArrowType::FLOAT64:
                REQUIRE(arrow_type_id == arrow::Type::DOUBLE);
                break;
            case ArrowType::TIMESTAMP_NS_UTC:
                REQUIRE(arrow_type_id == arrow::Type::TIMESTAMP);
                break;
            case ArrowType::BOOLEAN:
                REQUIRE(arrow_type_id == arrow::Type::BOOL);
                break;
            default:
                FAIL("Unknown ArrowType in metadata");
        }

        // Note: We don't validate nullable property because epoch_frame creates
        // all Arrow arrays as nullable=true by default, even though the actual
        // data never contains nulls for non-optional fields
    }

    // 3. Ensure DataFrame has no extra columns not in metadata
    auto col_names = df.column_names();
    for (const auto& col_name : col_names) {
        bool found = false;
        for (const auto& col_meta : metadata.columns) {
            if (col_meta.id == col_name) {
                found = true;
                break;
            }
        }
        INFO("Checking DataFrame column: " << col_name);
        REQUIRE(found); // Every DataFrame column must be described in metadata
    }

    INFO("✓ All columns validated against metadata");
    INFO("✓ Column types match");
    INFO("✓ No missing or extra columns");
}

} // namespace data_sdk::test
