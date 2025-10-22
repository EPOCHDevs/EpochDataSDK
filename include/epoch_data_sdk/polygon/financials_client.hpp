#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// FinancialsClient - Handles financial statements and ratios data
// Stocks only
class FinancialsClient {
public:
  explicit FinancialsClient(Options options);
  ~FinancialsClient();

  // Prevent copying
  FinancialsClient(const FinancialsClient&) = delete;
  FinancialsClient& operator=(const FinancialsClient&) = delete;

  // Allow moving
  FinancialsClient(FinancialsClient&&) = default;
  FinancialsClient& operator=(FinancialsClient&&) = default;

  // Get balance sheet data (backtest-friendly interface)
  // Filters by period_end date range for the specified ticker
  Expected<epoch_frame::DataFrame>
  getBalanceSheets(const std::string &ticker,
                   const std::string &from_date,
                   const std::string &to_date,
                   std::optional<int> limit = std::nullopt) const;

  // Get cash flow statements (backtest-friendly interface)
  Expected<epoch_frame::DataFrame>
  getCashFlowStatements(const std::string &ticker,
                        const std::string &from_date,
                        const std::string &to_date,
                        std::optional<int> limit = std::nullopt) const;

  // Get income statements (backtest-friendly interface)
  Expected<epoch_frame::DataFrame>
  getIncomeStatements(const std::string &ticker,
                      const std::string &from_date,
                      const std::string &to_date,
                      std::optional<int> limit = std::nullopt) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
