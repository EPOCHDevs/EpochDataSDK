//
// Created by adesola on 1/30/25.
//
#include <epoch_data_sdk/model/asset/index_constituents.hpp>
#include <filesystem>
#include <epoch_data_sdk/model/asset/constants.hpp>
#include <epoch_data_sdk/common/s3_loader.hpp>
#include <epoch_core/macros.h>
#include "spdlog/spdlog.h"
#include "glaze/glaze.hpp"

namespace data_sdk::asset {

const IndexConstituentsDatabase &IndexConstituentsDatabase::GetInstance() {
  static IndexConstituentsDatabase database;
  return database;
}

IndexConstituentsDatabase::IndexConstituentsDatabase() {
  IndexConstituentsData obj;
  glz::error_ctx ec;

  const auto cache =
      std::filesystem::temp_directory_path() / "index_constituents.json";

  if (std::filesystem::exists(cache)) {
    std::string buffer;
    ec = glz::read_file_json(obj, cache.string(), buffer);
  } else {
    // Try to load from S3 first
    auto result = common::S3Loader::Instance().GetObject(
        AssetDataS3Config::BUCKET_NAME,
        AssetDataS3Config::INDEX_CONSTITUENTS_KEY);

    AssertFromFormat(result.has_value(),
                     "Failed to retrieve index constituents database from S3: {}",
                     result.error());

    ec = glz::read_json(obj, result.value());
    std::string buffer;
    auto write_ec = glz::write_file_json(obj, cache.string(), buffer);
    if (write_ec) {
      SPDLOG_ERROR("Failed to write index_constituents.json cache: {}",
                   glz::format_error(write_ec, buffer));
    }
  }

  AssertFromFormat(!ec, "Failed to parse index constituents database from S3: {}",
                   glz::format_error(ec));

  SPDLOG_INFO("Successfully loaded index constituents database from S3");
  processIndexConstituents(obj);
}

void IndexConstituentsDatabase::processIndexConstituents(
    const IndexConstituentsData &data) {
  m_data = data;

  for (auto const &indexConstituent : data.indices) {
    m_storage[indexConstituent.index] = indexConstituent.constituents;
  }

  SPDLOG_INFO("Processed {} index constituents", m_storage.size());
}

std::optional<std::vector<std::string>>
IndexConstituentsDatabase::GetConstituents(std::string const &indexId) const {
  auto it = m_storage.find(indexId);
  if (it != m_storage.end()) {
    return it->second;
  }
  return std::nullopt;
}

bool IndexConstituentsDatabase::HasIndex(std::string const &indexId) const {
  return m_storage.contains(indexId);
}

} // namespace data_sdk::asset
