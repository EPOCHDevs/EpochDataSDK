target_sources(epoch_data_sdk
    PRIVATE
        # Core dataloader
        api_cache_dataloader.cpp

        # Cache infrastructure
        cache/cache_manifest.cpp
        cache/cache_orchestrator.cpp
        cache/cache_utils.cpp
        cache/day_bucket_cache_provider.cpp
        cache/merge_strategy.cpp
)
