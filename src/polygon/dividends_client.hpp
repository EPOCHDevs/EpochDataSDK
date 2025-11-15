#pragma once

#include <optional>
#include <string>

#include <drogon/drogon.h>
#include <epoch_frame/dataframe.h>

#include "epoch_data_sdk/common/metadata.hpp"
#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// Options for dividends data requests
struct DividendsOptions {
  std::optional<std::string> ticker = std::nullopt;
  std::optional<std::string> ex_dividend_date = std::nullopt;
  std::optional<std::string> ex_dividend_date_gte = std::nullopt;
  std::optional<std::string> ex_dividend_date_lte = std::nullopt;
  std::optional<std::string> declaration_date = std::nullopt;
  std::optional<std::string> record_date = std::nullopt;
  std::optional<std::string> pay_date = std::nullopt;
  std::optional<int> frequency = std::nullopt;
  std::optional<double> cash_amount = std::nullopt;
  std::optional<std::string> dividend_type = std::nullopt;
  std::optional<int> limit = std::nullopt;
  std::optional<std::string> sort = std::string("ex_dividend_date");
  std::optional<std::string> order = std::string("desc");
};

// DividendsClient - Handles dividend data
// CRITICAL for backtesting: calculate total returns (price + dividends)
class DividendsClient {
public:
  explicit DividendsClient(Options options);
  ~DividendsClient();

  // Prevent copying
  DividendsClient(const DividendsClient&) = delete;
  DividendsClient& operator=(const DividendsClient&) = delete;

  // Allow moving
  DividendsClient(DividendsClient&&) = default;
  DividendsClient& operator=(DividendsClient&&) = default;

  // Get historical dividends
  // Essential for calculating total returns in backtesting
  Expected<epoch_frame::DataFrame>
  getDividends(std::optional<std::string> ticker = std::nullopt,
               std::optional<std::string> ex_dividend_date = std::nullopt,
               std::optional<std::string> ex_dividend_date_gte = std::nullopt,
               std::optional<std::string> ex_dividend_date_lte = std::nullopt,
               std::optional<std::string> declaration_date = std::nullopt,
               std::optional<std::string> record_date = std::nullopt,
               std::optional<std::string> pay_date = std::nullopt,
               std::optional<int> frequency = std::nullopt,
               std::optional<double> cash_amount = std::nullopt,
               std::optional<std::string> dividend_type = std::nullopt,
               std::optional<int> limit = std::nullopt,
               std::optional<std::string> sort = std::string("ex_dividend_date"),
               std::optional<std::string> order = std::string("desc")) const;

  // Struct-based overload
  Expected<epoch_frame::DataFrame>
  getDividends(const DividendsOptions &opts) const {
    return getDividends(opts.ticker, opts.ex_dividend_date, opts.ex_dividend_date_gte,
                        opts.ex_dividend_date_lte, opts.declaration_date, opts.record_date,
                        opts.pay_date, opts.frequency, opts.cash_amount, opts.dividend_type,
                        opts.limit, opts.sort, opts.order);
  }

  // Async variant - pass-by-value to avoid coroutine lifetime issues
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getDividendsAsync(std::optional<std::string> ticker = std::nullopt,
                    std::optional<std::string> ex_dividend_date = std::nullopt,
                    std::optional<std::string> ex_dividend_date_gte = std::nullopt,
                    std::optional<std::string> ex_dividend_date_lte = std::nullopt,
                    std::optional<std::string> declaration_date = std::nullopt,
                    std::optional<std::string> record_date = std::nullopt,
                    std::optional<std::string> pay_date = std::nullopt,
                    std::optional<int> frequency = std::nullopt,
                    std::optional<double> cash_amount = std::nullopt,
                    std::optional<std::string> dividend_type = std::nullopt,
                    std::optional<int> limit = std::nullopt,
                    std::optional<std::string> sort = std::string("ex_dividend_date"),
                    std::optional<std::string> order = std::string("desc")) const;

  // Struct-based async overload
  drogon::Task<Expected<epoch_frame::DataFrame>>
  getDividendsAsync(DividendsOptions opts) const {
    return getDividendsAsync(std::move(opts.ticker), std::move(opts.ex_dividend_date),
                             std::move(opts.ex_dividend_date_gte), std::move(opts.ex_dividend_date_lte),
                             std::move(opts.declaration_date), std::move(opts.record_date),
                             std::move(opts.pay_date), opts.frequency, opts.cash_amount,
                             std::move(opts.dividend_type), opts.limit, std::move(opts.sort),
                             std::move(opts.order));
  }

  /**
   * Get metadata about the Dividends endpoint including endpoint description,
   * query parameters, and output fields with their descriptions.
   * @return DataFrameMetadata struct with column types and descriptions
   */
  static data_sdk::DataFrameMetadata getMetadata();

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
