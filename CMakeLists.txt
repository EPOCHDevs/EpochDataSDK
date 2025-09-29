cmake_minimum_required(VERSION 3.20)

set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")
set(VCPKG_MANIFEST_MODE ON)
set(VCPKG_FEATURE_FLAGS "versions")

option(BUILD_SHARED_LIBS "Build libraries as shared instead of static" OFF)
option(BUILD_TEST OFF)

project(EpochDataSDK VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
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
find_package(glaze CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

include(${PROJECT_SOURCE_DIR}/cmake/EpochFrame.cmake)

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
        glaze::glaze
)

add_subdirectory(src)

if(BUILD_TEST)
    enable_testing()
    add_subdirectory(test)
endif()

