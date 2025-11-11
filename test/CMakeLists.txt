
add_executable(epoch_data_sdk_test catch_main.cpp)

target_link_libraries(epoch_data_sdk_test
    PRIVATE
        epoch::data_sdk
        Catch2::Catch2
)

# Add src directory to test includes for accessing private headers in tests
target_include_directories(epoch_data_sdk_test
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

add_subdirectory(model)
add_subdirectory(dataloader)
add_subdirectory(polygon)
add_subdirectory(fred)
add_subdirectory(sec)
#add_subdirectory(trading_economics)