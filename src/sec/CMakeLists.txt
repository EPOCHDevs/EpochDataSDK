target_sources(epoch_data_sdk
    PRIVATE
        # Base infrastructure
        base_client.cpp

        # API clients
        query_client.cpp
        mapping_client.cpp
        render_client.cpp
        extractor_client.cpp
        insider_trading_client.cpp
        form13f_client.cpp
)
