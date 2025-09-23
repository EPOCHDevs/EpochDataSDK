CPMAddPackage(
    NAME Catch2
    GITHUB_REPOSITORY catchorg/Catch2
    VERSION 3.7.1
)

add_executable(epoch_data_sdk_test
    polygon/rest_tests.cpp
    polygon/websocket_tests.cpp
    polygon/financials_live_test.cpp
)

target_link_libraries(epoch_data_sdk_test
    PRIVATE
        epoch_data_sdk
        Catch2::Catch2WithMain
)

include(CTest)
list(APPEND CMAKE_MODULE_PATH ${Catch2_SOURCE_DIR}/extras)
include(Catch)
catch_discover_tests(epoch_data_sdk_test)