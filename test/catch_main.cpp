//
// Created by adesola on 3/21/25.
//
#include <catch2/catch_session.hpp>
#include <epoch_frame/serialization.h>
#include <arrow/compute/api.h>
#include <sstream>
#include <stdexcept>

int main(int argc, char *argv[])
{
    // your setup ...
    epoch_frame::ScopedS3 scoped_s3;

    auto arrowComputeStatus = arrow::compute::Initialize();
    if (!arrowComputeStatus.ok())
    {
        std::stringstream errorMsg;
        errorMsg << "arrow compute initialized failed: " << arrowComputeStatus
                 << std::endl;
        throw std::runtime_error(errorMsg.str());
    }

    setenv("POLYGON_API_KEY", "ptMp4LUoa1sgSpTFS7v8diiVtnimqH46", 1);
    setenv("FRED_API_KEY", "b6561c96d3615458fcae0b57580664f3", 1);
    const int result = Catch::Session().run(argc, argv);

    // your clean-up...

    return result;
}
