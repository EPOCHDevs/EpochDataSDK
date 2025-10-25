#pragma once

#include <memory>
#include <optional>
#include <string>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// Options for IPO data requests
struct IPOOptions {
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  std::optional<std::string> ticker = std::nullopt;
  std::optional<int> limit = std::nullopt;
};

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

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getIPOs(const IPOOptions &opts) const {
    return getIPOs(opts.from_date, opts.to_date, opts.ticker, opts.limit);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getIPOsAsync(std::string from_date, std::string to_date,
               std::optional<std::string> ticker = std::nullopt,
               std::optional<int> limit = std::nullopt) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getIPOsAsync(IPOOptions opts) const {
    return getIPOsAsync(std::move(opts.from_date), std::move(opts.to_date),
                        std::move(opts.ticker), opts.limit);
  }

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
