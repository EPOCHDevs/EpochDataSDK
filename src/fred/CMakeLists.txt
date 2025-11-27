target_sources(epoch_data_sdk
    PRIVATE
        # Base infrastructure
        base_client.cpp

        # Shared series implementation (chunking, fallback, pagination)
        series_impl.cpp

        # ALFRED client (with fallback to FRED mode)
        alfred_client.cpp
)
