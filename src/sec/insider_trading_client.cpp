#include "epoch_data_sdk/sec/insider_trading_client.hpp"

#include <chrono>
#include <spdlog/spdlog.h>
#include <glaze/glaze.hpp>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

namespace data_sdk::sec {

InsiderTradingClient::InsiderTradingClient(Options options)
    : BaseClient(std::move(options)) {}

drogon::Task<Expected<ListResponse<InsiderTransaction>>>
InsiderTradingClient::getTransactions(const std::string &query_json) {
  try {
    auto result = co_await httpAsyncPost("/", query_json);

    if (!result) {
      co_return std::unexpected(result.error());
    }

    // Parse JSON response using glaze
    ListResponse<InsiderTransaction> response;
    auto parse_error = glz::read_json(response, *result);

    if (parse_error) {
      SPDLOG_ERROR("JSON parsing error in getTransactions: {}",
                   glz::format_error(parse_error, *result));
      co_return std::unexpected(HttpError{0, "JSON parsing error"});
    }

    co_return response;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getTransactions: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<ListResponse<InsiderTransaction>>>
InsiderTradingClient::getTransactionsByTicker(const std::string &ticker,
                                               const std::string &transaction_type,
                                               int size) {
  try {
    // Build query string
    std::string query_string = "issuerTicker:" + ticker;
    if (!transaction_type.empty()) {
      query_string += " AND transactionCode:" + transaction_type;
    }

    // Build query JSON
    std::string query_json = R"({"query": ")" + query_string +
                            R"(", "from": "0", "size": ")" + std::to_string(size) +
                            R"(", "sort": [{"filedAt": {"order": "desc"}}]})";

    SPDLOG_DEBUG("Insider trading query JSON: {}", query_json);
    co_return co_await getTransactions(query_json);
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getTransactionsByTicker: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<ListResponse<InsiderTransaction>>>
InsiderTradingClient::getTransactionsByCIK(const std::string &cik,
                                           const std::string &transaction_type,
                                           int size) {
  try {
    // Build query string
    std::string query_string = "issuerCik:" + cik;
    if (!transaction_type.empty()) {
      query_string += " AND transactionCode:" + transaction_type;
    }

    // Build query JSON
    std::string query_json = R"({"query": ")" + query_string +
                            R"(", "from": "0", "size": ")" + std::to_string(size) +
                            R"(", "sort": [{"filedAt": {"order": "desc"}}]})";

    co_return co_await getTransactions(query_json);
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getTransactionsByCIK: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<ListResponse<InsiderTransaction>>>
InsiderTradingClient::getTransactionsByInsider(const std::string &owner_name,
                                               int size) {
  try {
    // Build query string with quoted name for exact match
    std::string query_string = "ownerName:\\\"" + owner_name + "\\\"";

    // Build query JSON
    std::string query_json = R"({"query": ")" + query_string +
                            R"(", "from": "0", "size": ")" + std::to_string(size) +
                            R"(", "sort": [{"filedAt": {"order": "desc"}}]})";

    co_return co_await getTransactions(query_json);
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getTransactionsByInsider: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<ListResponse<InsiderTransaction>>>
InsiderTradingClient::getLargePurchases(const std::string &ticker,
                                        double min_value,
                                        int size) {
  try {
    // Build query string for purchases (code P) above minimum value
    std::string query_string = "transactionCode:P";

    if (!ticker.empty()) {
      query_string += " AND issuerTicker:" + ticker;
    }

    // Note: The API might not support direct value filtering,
    // so we fetch larger results and can filter client-side if needed
    // For now, just get purchases sorted by filing date
    std::string query_json = R"({"query": ")" + query_string +
                            R"(", "from": "0", "size": ")" + std::to_string(size * 2) +
                            R"(", "sort": [{"filedAt": {"order": "desc"}}]})";

    auto result = co_await getTransactions(query_json);

    if (!result.has_value()) {
      co_return std::unexpected(result.error());
    }

    // Filter by minimum value
    ListResponse<InsiderTransaction> filtered_response;
    filtered_response.data.reserve(size);

    for (const auto &txn : result->data) {
      double value = txn.transactionShares * txn.transactionPricePerShare;
      if (value >= min_value && filtered_response.data.size() < static_cast<size_t>(size)) {
        filtered_response.data.push_back(txn);
      }
    }

    filtered_response.total = static_cast<int>(filtered_response.data.size());

    SPDLOG_DEBUG("Filtered {} large purchases (>= ${}) from {} total transactions",
                filtered_response.data.size(), min_value, result->data.size());

    co_return filtered_response;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getLargePurchases: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

// ========== DataFrame Methods Implementation ==========

Expected<epoch_frame::DataFrame>
InsiderTradingClient::getTransactionsDataFrame(const std::string &ticker,
                                               const std::string &from_date,
                                               const std::string &to_date,
                                               std::optional<epoch_core::TransactionCode> transaction_code) const {
  try {
    //Build query with date range
    std::string query_string = "issuerTicker:" + ticker;

    // Add date range filter (filedAt is the filing timestamp field)
    query_string += " AND filedAt:[" + from_date + " TO " + to_date + "]";

    if (transaction_code.has_value()) {
      query_string += " AND transactionCode:" + transactionCodeToString(*transaction_code);
    }

    // Build query JSON with ascending sort for chronological order
    std::string query_json = R"({"query": ")" + query_string +
                            R"(", "from": "0", "size": "1000",)" +
                            R"( "sort": [{"filedAt": {"order": "asc"}}]})";

    SPDLOG_DEBUG("Insider trading DataFrame query: {}", query_json);

    // Get transactions using existing method (synchronous version)
    auto task = const_cast<InsiderTradingClient*>(this)->getTransactions(query_json);
    auto result = drogon::sync_wait(task);

    if (!result.has_value()) {
      return std::unexpected(result.error());
    }

    const auto &transactions = result->data;

    // Build column vectors (only fundamental data)
    std::vector<std::string> filed_at_strings, transaction_dates;
    std::vector<std::string> owner_names, transaction_codes;
    std::vector<double> shares, prices, ownership_after;

    filed_at_strings.reserve(transactions.size());
    transaction_dates.reserve(transactions.size());
    owner_names.reserve(transactions.size());
    transaction_codes.reserve(transactions.size());
    shares.reserve(transactions.size());
    prices.reserve(transactions.size());
    ownership_after.reserve(transactions.size());

    for (const auto &txn : transactions) {
      filed_at_strings.push_back(txn.filedAt);
      transaction_dates.push_back(txn.transactionDate);
      owner_names.push_back(txn.ownerName);
      transaction_codes.push_back(txn.transactionCode);
      shares.push_back(txn.transactionShares);
      prices.push_back(txn.transactionPricePerShare);
      ownership_after.push_back(txn.sharesOwnedFollowingTransaction);
    }

    // Parse ISO 8601 timestamps to nanoseconds
    auto filed_at_ns = parseISO8601ToNanoseconds(filed_at_strings);

    // Build DataFrame columns (ticker removed - user already provided it)
    std::vector<std::string> columns = {
      "transaction_date", "owner_name", "transaction_code",
      "shares", "price", "ownership_after"
    };

    std::vector<arrow::ChunkedArrayPtr> data{
      epoch_frame::factory::array::make_array(transaction_dates),
      epoch_frame::factory::array::make_array(owner_names),
      epoch_frame::factory::array::make_array(transaction_codes),
      epoch_frame::factory::array::make_array(shares),
      epoch_frame::factory::array::make_array(prices),
      epoch_frame::factory::array::make_array(ownership_after)
    };

    // Create datetime index from filed_at timestamps
    auto index = epoch_frame::factory::index::make_datetime_index(filed_at_ns, "filed_at", "UTC");
    auto df = epoch_frame::make_dataframe(index, data, columns);

    SPDLOG_INFO("Built insider trading DataFrame: {} rows for ticker={} from {} to {}",
                transactions.size(), ticker, from_date, to_date);

    return df;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getTransactionsDataFrame: {}", e.what());
    return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

Expected<epoch_frame::DataFrame>
InsiderTradingClient::getTransactionsDataFrame(const InsiderTradingOptions &opts) const {
  std::optional<epoch_core::TransactionCode> code = std::nullopt;
  if (opts.transaction_code.has_value()) {
    // Convert string to enum if provided
    const std::string &code_str = *opts.transaction_code;
    // Simple mapping - could be improved with reverse lookup
    if (code_str == "P") code = epoch_core::TransactionCode::P;
    else if (code_str == "S") code = epoch_core::TransactionCode::S;
    else if (code_str == "A") code = epoch_core::TransactionCode::A;
    else if (code_str == "M") code = epoch_core::TransactionCode::M;
    // Add more as needed
  }

  return getTransactionsDataFrame(
    opts.ticker.value_or(""),
    opts.from_date,
    opts.to_date,
    code
  );
}

drogon::Task<Expected<epoch_frame::DataFrame>>
InsiderTradingClient::getTransactionsDataFrameAsync(std::string ticker,
                                                    std::string from_date,
                                                    std::string to_date,
                                                    std::optional<epoch_core::TransactionCode> transaction_code) const {
  // For async, just wrap the synchronous version
  // Could be optimized to use async query methods internally
  co_return getTransactionsDataFrame(ticker, from_date, to_date, transaction_code);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
InsiderTradingClient::getTransactionsDataFrameAsync(InsiderTradingOptions opts) const {
  co_return getTransactionsDataFrame(opts);
}

} // namespace data_sdk::sec
