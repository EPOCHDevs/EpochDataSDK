# EpochCore.cmake
#
# This is a helper file to include EpochCore via CPM

set(EPOCH_CORE_REPOSITORY "${REPO_URL}/EPOCHDevs/EpochCore.git" CACHE STRING "EpochCore repository URL")
set(EPOCH_CORE_TAG "master" CACHE STRING "EpochCore Git tag to use")

CPMAddPackage(
    NAME EpochCore
    GIT_REPOSITORY ${EPOCH_CORE_REPOSITORY}
    GIT_TAG ${EPOCH_CORE_TAG}
)

message(STATUS "EpochCore fetched and built from source")