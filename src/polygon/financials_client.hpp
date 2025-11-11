#pragma once

#include <optional>
#include <string>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// Options for financial statements requests
struct BalanceSheetOptions {
  std::string ticker;
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

struct CashFlowOptions {
  std::string ticker;
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

struct IncomeStatementOptions {
  std::string ticker;
  std::string from_date;  // "YYYY-MM-DD"
  std::string to_date;    // "YYYY-MM-DD"
  std::optional<int> limit = std::nullopt;
};

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

  Expected<epoch_frame::DataFrame>
  getBalanceSheets(const BalanceSheetOptions &opts) const {
    return getBalanceSheets(opts.ticker, opts.from_date, opts.to_date, opts.limit);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getBalanceSheetsAsync(std::string ticker, std::string from_date,
                        std::string to_date, std::optional<int> limit = std::nullopt) const;

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getBalanceSheetsAsync(BalanceSheetOptions opts) const {
    return getBalanceSheetsAsync(std::move(opts.ticker), std::move(opts.from_date),
                                 std::move(opts.to_date), opts.limit);
  }

  // Get cash flow statements (backtest-friendly interface)
  Expected<epoch_frame::DataFrame>
  getCashFlowStatements(const std::string &ticker,
                        const std::string &from_date,
                        const std::string &to_date,
                        std::optional<int> limit = std::nullopt) const;

  Expected<epoch_frame::DataFrame>
  getCashFlowStatements(const CashFlowOptions &opts) const {
    return getCashFlowStatements(opts.ticker, opts.from_date, opts.to_date, opts.limit);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getCashFlowStatementsAsync(std::string ticker, std::string from_date,
                             std::string to_date, std::optional<int> limit = std::nullopt) const;

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getCashFlowStatementsAsync(CashFlowOptions opts) const {
    return getCashFlowStatementsAsync(std::move(opts.ticker), std::move(opts.from_date),
                                      std::move(opts.to_date), opts.limit);
  }

  // Get income statements (backtest-friendly interface)
  Expected<epoch_frame::DataFrame>
  getIncomeStatements(const std::string &ticker,
                      const std::string &from_date,
                      const std::string &to_date,
                      std::optional<int> limit = std::nullopt) const;

  Expected<epoch_frame::DataFrame>
  getIncomeStatements(const IncomeStatementOptions &opts) const {
    return getIncomeStatements(opts.ticker, opts.from_date, opts.to_date, opts.limit);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getIncomeStatementsAsync(std::string ticker, std::string from_date,
                           std::string to_date, std::optional<int> limit = std::nullopt) const;

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getIncomeStatementsAsync(IncomeStatementOptions opts) const {
    return getIncomeStatementsAsync(std::move(opts.ticker), std::move(opts.from_date),
                                    std::move(opts.to_date), opts.limit);
  }

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
