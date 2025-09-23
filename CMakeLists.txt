cmake_minimum_required(VERSION 3.20)

set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")
set(VCPKG_MANIFEST_MODE ON)
set(VCPKG_FEATURE_FLAGS "versions")

project(EpochDataSDK VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(GIT_TOKEN "" CACHE STRING "Default GIT TOKEN")

if (GIT_TOKEN)
    SET(REPO_URL "https://${GIT_TOKEN}:@github.com")
else ()
    SET(REPO_URL "https://github.com")
endif ()

option(EPOCH_DATA_SDK_BUILD_TESTS "Build tests" ON)

set(CPM_DOWNLOAD_VERSION 0.40.2)
set(CPM_DOWNLOAD_LOCATION "${CMAKE_BINARY_DIR}/cmake/CPM_${CPM_DOWNLOAD_VERSION}.cmake")

if(NOT (EXISTS ${CPM_DOWNLOAD_LOCATION}))
    message(STATUS "Downloading CPM.cmake...")
    file(DOWNLOAD
            https://github.com/cpm-cmake/CPM.cmake/releases/download/v${CPM_DOWNLOAD_VERSION}/CPM.cmake
            ${CPM_DOWNLOAD_LOCATION}
    )
endif()

include(${CPM_DOWNLOAD_LOCATION})

find_package(Drogon CONFIG REQUIRED)
find_package(glaze CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

if(TARGET Drogon::Drogon)
    set(DROGON_LIB Drogon::Drogon)
elseif(TARGET drogon)
    set(DROGON_LIB drogon)
endif()

include(${CMAKE_SOURCE_DIR}/cmake/EpochCore.cmake)
include(${CMAKE_SOURCE_DIR}/cmake/EpochFrame.cmake)

add_library(epoch_data_sdk STATIC)

target_include_directories(epoch_data_sdk
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(epoch_data_sdk
    PUBLIC
        epoch_frame
        epoch_core
        ${DROGON_LIB}
        glaze::glaze
        spdlog::spdlog
)

add_subdirectory(src)

if(EPOCH_DATA_SDK_BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()

