
add_executable(epoch_data_sdk_test catch_main.cpp)

target_link_libraries(epoch_data_sdk_test
    PRIVATE
        epoch_data_sdk
        Catch2::Catch2
)

add_subdirectory(polygon)
add_subdirectory(tradingeconomics)