#include <epoch_data_sdk/dataloader/factory.hpp>
#include "api_cache_dataloader.h"
#include "cache/day_bucket_cache_provider.h"
#include "fetcher_provider_default.h"

namespace data_sdk::dataloader {

std::unique_ptr<IDataLoader> CreateApiCacheDataLoader(
    DataLoaderOptions options,
    std::string s3Path) {

    auto fetcherProvider = std::make_shared<DefaultFetcherProvider>(s3Path);
    auto cacheProvider = std::make_shared<cache::DayBucketCacheProvider>();

    return std::make_unique<ApiCacheDataloader>(
        std::move(options),
        cacheProvider,
        fetcherProvider
    );
}

} // namespace data_sdk::dataloader
