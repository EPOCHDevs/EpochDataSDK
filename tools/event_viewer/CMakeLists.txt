# Console Event Viewer - FTXUI-based terminal UI for event monitoring

find_package(ftxui CONFIG REQUIRED)

add_library(epoch_event_viewer STATIC
    console_event_viewer.cpp
    console_event_viewer.h
)

target_include_directories(epoch_event_viewer PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(epoch_event_viewer PUBLIC
    epoch_data_sdk
    ftxui::screen
    ftxui::dom
    ftxui::component
)

# Standalone demo executable
add_executable(event_viewer_demo
    main.cpp
)

target_link_libraries(event_viewer_demo PRIVATE
    epoch_event_viewer
)
