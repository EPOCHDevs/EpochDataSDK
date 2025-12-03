cmake_minimum_required(VERSION 3.26 FATAL_ERROR) # 3.26 For now until XGBOOST Removes FindCUDA

set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")
set(VCPKG_MANIFEST_MODE ON)
set(VCPKG_FEATURE_FLAGS "versions")

option(BUILD_SHARED_LIBS "Build libraries as shared instead of static" OFF)
option(BUILD_TEST OFF)
option(BUILD_TOOLS "Build development tools (event viewer, etc.)" ON)

set(PROJECT_VERSION 0.1.1)
set(PROJECT_VERSION_MAJOR 0.1.1)

project(EpochDataSDK LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set binary and library output directories to keep build files separate from source
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)


if (BUILD_TEST)
    # Find Catch2 for both tests and benchmarks
    find_package(Catch2 REQUIRED)
    find_package(trompeloeil REQUIRED)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 --coverage")
endif()

set(GIT_TOKEN "" CACHE STRING "Default GIT TOKEN")

if (GIT_TOKEN)
    SET(REPO_URL "https://${GIT_TOKEN}:@github.com")
else ()
    SET(REPO_URL "https://github.com")
endif ()


find_package(Drogon CONFIG REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(Boost REQUIRED)

set(DEPENDENCY_DIR "/usr/local" CACHE STRING "Dependency directory")

include(${PROJECT_SOURCE_DIR}/cmake/EpochFrame.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/MPDecimal.cmake)

add_library(epoch_data_sdk STATIC)
add_library(epoch::data_sdk ALIAS epoch_data_sdk)

target_include_directories(epoch_data_sdk
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(epoch_data_sdk
    PUBLIC
        epoch::frame
        Drogon::Drogon
        libmpdec++
        libmpdec
        yaml-cpp::yaml-cpp
        Boost::boost
)

add_subdirectory(src)

if(BUILD_TEST)
    enable_testing()
    add_subdirectory(test)
endif()

if(BUILD_TOOLS)
    add_subdirectory(tools/event_viewer)
endif()
