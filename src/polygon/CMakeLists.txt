target_sources(epoch_data_sdk
    PRIVATE
        # Base infrastructure
        base_client.cpp
        client_factory.cpp

        # Specialized clients
        aggs_client.cpp
        quotes_client.cpp
        trades_client.cpp
        financials_client.cpp
        short_volume_client.cpp
        short_interest_client.cpp
        ipo_client.cpp
        splits_client.cpp
        dividends_client.cpp
        news_client.cpp

        # WebSocket
        websocket_client.cpp
)