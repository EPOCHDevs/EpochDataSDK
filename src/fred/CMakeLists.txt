target_sources(epoch_data_sdk
    PRIVATE
        # Base infrastructure
        base_client.cpp

        # Shared series implementation (DRY)
        series_impl.cpp

        # Simple FRED client (current/latest data only)
        fred_client.cpp

        # ALFRED client (revision tracking for backtesting)
        alfred_client.cpp
)
