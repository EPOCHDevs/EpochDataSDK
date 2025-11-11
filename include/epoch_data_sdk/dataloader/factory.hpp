#pragma once
#include <epoch_data_sdk/dataloader/dataloader.hpp>
#include <epoch_data_sdk/dataloader/options.hpp>
#include <memory>
#include <string>

namespace data_sdk::dataloader {

// Factory function to create the standard API+cache dataloader
// Uses DefaultFetcherProvider + DayBucketCacheProvider + ApiCacheDataloader internally
std::unique_ptr<IDataLoader> CreateApiCacheDataLoader(
    DataLoaderOptions options,
    std::string s3Path = "s3://epoch-db"
);

} // namespace data_sdk::dataloader
