
add_executable(epoch_data_sdk_test
        catch_main.cpp
        # factory_index_expansion_test.cpp - Commented out: belongs to epoch_stratifyx project
        # auxiliary_category_detection_test.cpp - Commented out: belongs to epoch_stratifyx project
)

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

add_subdirectory(common)
add_subdirectory(model)
add_subdirectory(dataloader)
add_subdirectory(polygon)
add_subdirectory(fred)
add_subdirectory(sec)
