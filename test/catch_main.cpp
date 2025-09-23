//
// Created by adesola on 3/21/25.
//
#include <catch2/catch_session.hpp>
#include <epoch_frame/serialization.h>

int main(int argc, char *argv[]) {
    // your setup ...
    epoch_frame::ScopedS3 scoped_s3;
    setenv("ALPACA_API_KEY", "PKNYP4EL7ZETUV5HMZ83", 0);
    setenv("ALPACA_API_SECRET", "27MeWNNUknErGXfShLNFcdK9m57Kr7JoyVfmzUI3", 0);
    setenv("POLYGON_API_KEY", "ptMp4LUoa1sgSpTFS7v8diiVtnimqH46", 0);

    const int result = Catch::Session().run(argc, argv);

    // your clean-up...


    return result;
}
