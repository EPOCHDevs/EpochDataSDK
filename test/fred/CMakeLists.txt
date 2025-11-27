target_sources(epoch_data_sdk_test PRIVATE
        # AlfredClient tests (includes fallback to FRED mode)
        alfred_client_tests.cpp
        cross_sectional_tests.cpp
)
