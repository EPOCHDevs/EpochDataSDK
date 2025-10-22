#pragma once

#include <memory>
#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

class IPOClient {
public:
  explicit IPOClient(Options options);
  ~IPOClient();

  // Non-copyable
  IPOClient(const IPOClient &) = delete;
  IPOClient &operator=(const IPOClient &) = delete;

  // Movable
  IPOClient(IPOClient &&) noexcept = default;
  IPOClient &operator=(IPOClient &&) noexcept = default;

  /**
   * Get IPO listings within a date range.
   *
   * @param from_date Start date (YYYY-MM-DD) for listing_date filter
   * @param to_date End date (YYYY-MM-DD) for listing_date filter
   * @param ticker Optional ticker symbol filter (use std::nullopt for all IPOs)
   * @param limit Optional page size limit (pagination will fetch all pages)
   * @return DataFrame indexed by listing_date with IPO data
   */
  Expected<epoch_frame::DataFrame>
  getIPOs(const std::string &from_date,
          const std::string &to_date,
          std::optional<std::string> ticker = std::nullopt,
          std::optional<int> limit = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
