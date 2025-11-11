#pragma once

#include <optional>
#include <string>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// Options for splits data requests
struct SplitsOptions {
  std::optional<std::string> ticker = std::nullopt;
  std::optional<std::string> execution_date = std::nullopt;
  std::optional<std::string> execution_date_gte = std::nullopt;
  std::optional<std::string> execution_date_lte = std::nullopt;
  std::optional<bool> reverse_split = std::nullopt;
  std::optional<int> limit = std::nullopt;
  std::optional<std::string> sort = std::string("execution_date");
  std::optional<std::string> order = std::string("desc");
};

// SplitsClient - Handles stock split data
// CRITICAL for backtesting: adjusts historical prices for splits
class SplitsClient {
public:
  explicit SplitsClient(Options options);
  ~SplitsClient();

  // Prevent copying
  SplitsClient(const SplitsClient&) = delete;
  SplitsClient& operator=(const SplitsClient&) = delete;

  // Allow moving
  SplitsClient(SplitsClient&&) = default;
  SplitsClient& operator=(SplitsClient&&) = default;

  // Get historical stock splits
  // Essential for adjusting historical prices
  Expected<epoch_frame::DataFrame>
  getSplits(std::optional<std::string> ticker = std::nullopt,
            std::optional<std::string> execution_date = std::nullopt,
            std::optional<std::string> execution_date_gte = std::nullopt,
            std::optional<std::string> execution_date_lte = std::nullopt,
            std::optional<bool> reverse_split = std::nullopt,
            std::optional<int> limit = std::nullopt,
            std::optional<std::string> sort = std::string("execution_date"),
            std::optional<std::string> order = std::string("desc")) const;

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getSplits(const SplitsOptions &opts) const {
    return getSplits(opts.ticker, opts.execution_date, opts.execution_date_gte,
                     opts.execution_date_lte, opts.reverse_split, opts.limit,
                     opts.sort, opts.order);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getSplitsAsync(std::optional<std::string> ticker = std::nullopt,
                 std::optional<std::string> execution_date = std::nullopt,
                 std::optional<std::string> execution_date_gte = std::nullopt,
                 std::optional<std::string> execution_date_lte = std::nullopt,
                 std::optional<bool> reverse_split = std::nullopt,
                 std::optional<int> limit = std::nullopt,
                 std::optional<std::string> sort = std::string("execution_date"),
                 std::optional<std::string> order = std::string("desc")) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getSplitsAsync(SplitsOptions opts) const {
    return getSplitsAsync(std::move(opts.ticker), std::move(opts.execution_date),
                          std::move(opts.execution_date_gte), std::move(opts.execution_date_lte),
                          opts.reverse_split, opts.limit, std::move(opts.sort), std::move(opts.order));
  }

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
