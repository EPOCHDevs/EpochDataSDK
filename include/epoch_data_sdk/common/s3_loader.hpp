#pragma once

#include <aws/s3/S3Client.h>
#include <expected>
#include <string>
#include <memory>

namespace data_sdk::common {

/**
 * @brief Simple S3 loader for downloading JSON files from S3
 *
 * This is a lightweight utility for loading data files from S3.
 * It's designed for SDK use cases (loading asset specs, index constituents, etc.)
 * and doesn't include server-specific concepts like user IDs.
 */
class S3Loader {
public:
  /**
   * @brief Get singleton instance
   */
  static const S3Loader& Instance();

  /**
   * @brief Load an object from S3 as a string
   *
   * @param bucketName The S3 bucket name
   * @param objectKey The object key (e.g., "asset_specifications.json")
   * @return Expected containing the file contents or error message
   */
  std::expected<std::string, std::string>
  GetObject(const std::string& bucketName, const std::string& objectKey) const;

  /**
   * @brief List object keys in an S3 folder (prefix)
   *
   * @param bucketName The S3 bucket name
   * @param prefix The folder prefix to list
   * @return Vector of object keys
   */
  std::vector<std::string>
  ListObjects(const std::string& bucketName, const std::string& prefix) const;

private:
  S3Loader();
  ~S3Loader();

  // Non-copyable, non-movable
  S3Loader(const S3Loader&) = delete;
  S3Loader& operator=(const S3Loader&) = delete;
  S3Loader(S3Loader&&) = delete;
  S3Loader& operator=(S3Loader&&) = delete;

  std::shared_ptr<Aws::S3::S3Client> m_s3Client;
};

} // namespace data_sdk::common
