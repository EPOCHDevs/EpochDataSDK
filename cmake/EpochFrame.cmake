# EpochFrame.cmake
#
# This is a helper file to include EpochFrame
include(FetchContent)

set(EPOCH_FRAME_REPOSITORY "${REPO_URL}/EPOCHDevs/EpochFrame.git" CACHE STRING "EpochFrame repository URL")
set(EPOCH_FRAME_TAG "master" CACHE STRING "EpochFrame Git tag to use")

FetchContent_Declare(
        EpochFrame
        GIT_REPOSITORY ${EPOCH_FRAME_REPOSITORY}
        GIT_TAG ${EPOCH_FRAME_TAG}
)

FetchContent_MakeAvailable(EpochFrame)

message(STATUS "EpochFrame fetched and built from source")