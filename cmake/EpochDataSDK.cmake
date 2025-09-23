# EpochDataSDK.cmake
#
# This is a helper file to include EpochDataSDK via CPM
include(CPM)

set(EPOCH_DATA_SDK_REPOSITORY "${REPO_URL}/EPOCHDevs/EpochDataSDK.git" CACHE STRING "EpochDataSDK repository URL")
set(EPOCH_DATA_SDK_TAG "master" CACHE STRING "EpochDataSDK Git tag to use")

CPMAddPackage(
    NAME EpochDataSDK
    GIT_REPOSITORY ${EPOCH_DATA_SDK_REPOSITORY}
    GIT_TAG ${EPOCH_DATA_SDK_TAG}
)

message(STATUS "EpochDataSDK fetched and built from source")