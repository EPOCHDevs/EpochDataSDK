# EpochFrame.cmake
#
# This is a helper file to include EpochFrame via CPM

set(EPOCH_FRAME_REPOSITORY "${REPO_URL}/EPOCHDevs/EpochFrame.git" CACHE STRING "EpochFrame repository URL")
set(EPOCH_FRAME_TAG "master" CACHE STRING "EpochFrame Git tag to use")

CPMAddPackage(
    NAME EpochFrame
    GIT_REPOSITORY ${EPOCH_FRAME_REPOSITORY}
    GIT_TAG ${EPOCH_FRAME_TAG}
)

message(STATUS "EpochFrame fetched and built from source")