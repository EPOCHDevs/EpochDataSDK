#pragma once
//
// Created by adesola on 1/30/25.
//
#include <epoch_data_sdk/common/symbol.hpp>
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>

namespace data_sdk::asset {

struct IndexConstituent {
  std::string index;
  std::vector<std::string> constituents;
  std::vector<std::string> unsupported;
};

struct IndexConstituentsData {
  std::vector<IndexConstituent> indices;
};

class IndexConstituentsDatabase {
public:
  bool IsInitialized() const noexcept { return !m_storage.empty(); }

  static const IndexConstituentsDatabase &GetInstance();

  // Get constituents for a given index ID
  std::optional<std::vector<std::string>> GetConstituents(std::string const &indexId) const;

  // Check if an index exists in the database
  bool HasIndex(std::string const &indexId) const;

  // Get all index constituent data (for metadata endpoint)
  const IndexConstituentsData &GetAllData() const { return m_data; }

private:
  IndexConstituentsDatabase(IndexConstituentsDatabase const &) = delete;
  IndexConstituentsDatabase &operator=(IndexConstituentsDatabase const &) = delete;

  IndexConstituentsDatabase();

  void processIndexConstituents(const IndexConstituentsData &data);

  IndexConstituentsData m_data;
  std::unordered_map<std::string, std::vector<std::string>> m_storage;
};

} // namespace data_sdk::asset
