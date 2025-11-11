#include <epoch_data_sdk/common/s3_loader.hpp>

#include <aws/core/Aws.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <spdlog/spdlog.h>
#include <epoch_frame/serialization.h>

namespace data_sdk::common {

const S3Loader& S3Loader::Instance() {
  static S3Loader instance;
  return instance;
}

S3Loader::S3Loader() {
  // Ensure S3 is initialized through the Arrow interface
  auto fs_result = epoch_frame::get_s3_filesystem();
  if (!fs_result.ok()) {
    SPDLOG_WARN("S3 filesystem initialization warning: {}",
                fs_result.status().ToString());
  }

  // Configure S3 client
  Aws::Client::ClientConfiguration config;
  config.region = "us-west-1";
  config.requestTimeoutMs = 120000; // 2 minutes
  config.connectTimeoutMs = 60000;  // 1 minute
  config.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(
      10, 5000); // max retries: 10, backoff base delay: 5 seconds

  m_s3Client = Aws::MakeShared<Aws::S3::S3Client>("S3Client", config);
  SPDLOG_DEBUG("S3Loader initialized successfully");
}

S3Loader::~S3Loader() {
  m_s3Client.reset();
  SPDLOG_DEBUG("S3Loader released successfully");
}

std::expected<std::string, std::string>
S3Loader::GetObject(const std::string& bucketName,
                    const std::string& objectKey) const {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucketName.c_str());
  request.SetKey(objectKey.c_str());

  auto outcome = m_s3Client->GetObject(request);
  if (!outcome.IsSuccess()) {
    const auto& error = outcome.GetError();
    return std::unexpected(
        std::format("Failed to get S3 object s3://{}/{}: {} - {}",
                    bucketName, objectKey,
                    error.GetExceptionName(),
                    error.GetMessage()));
  }

  // Read the object data into a string
  auto& retrievedFile = outcome.GetResultWithOwnership().GetBody();
  std::stringstream buffer;
  buffer << retrievedFile.rdbuf();

  SPDLOG_DEBUG("Successfully loaded S3 object s3://{}/{} ({} bytes)",
               bucketName, objectKey, buffer.str().size());

  return buffer.str();
}

std::vector<std::string>
S3Loader::ListObjects(const std::string& bucketName,
                      const std::string& prefix) const {
  Aws::S3::Model::ListObjectsV2Request request;
  request.SetBucket(bucketName.c_str());
  request.SetPrefix(prefix.c_str());

  auto outcome = m_s3Client->ListObjectsV2(request);
  std::vector<std::string> result;

  if (outcome.IsSuccess()) {
    const auto& objects = outcome.GetResult().GetContents();
    result.reserve(objects.size());

    for (const auto& object : objects) {
      result.emplace_back(object.GetKey());
    }

    SPDLOG_DEBUG("Listed {} objects from s3://{}/{}",
                 result.size(), bucketName, prefix);
  } else {
    const auto& error = outcome.GetError();
    SPDLOG_ERROR("Failed to list S3 objects s3://{}/{}: {} - {}",
                 bucketName, prefix,
                 error.GetExceptionName(),
                 error.GetMessage());
  }

  return result;
}

} // namespace data_sdk::common
