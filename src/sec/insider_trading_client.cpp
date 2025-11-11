#include "sec/insider_trading_client.hpp"

#include <chrono>
#include <numeric>
#include <map>
#include <set>
#include <spdlog/spdlog.h>
#include <glaze/glaze.hpp>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/date_offset_factory.h>

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

    filtered_response.total.value = static_cast<int>(filtered_response.data.size());
    filtered_response.total.relation = "eq";

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
                                               std::optional<epoch_core::TransactionCode> transaction_code,
                                               bool is_eod) const {
  try {
    //Build query with date range
    std::string query_string = "issuerTicker:" + ticker;

    // Add date range filter (filedAt is the filing timestamp field)
    query_string += " AND filedAt:[" + from_date + " TO " + to_date + "]";

    if (transaction_code.has_value()) {
      query_string += " AND transactionCode:" + transactionCodeToString(*transaction_code);
    }

    // Pagination loop to fetch all results
    std::vector<InsiderTransaction> all_transactions;
    int from = 0;
    const int page_size = 50;  // SEC API maximum
    bool has_more = true;
    int total_count = 0;

    while (has_more && from < 10000) {
      // Build query JSON with current offset
      std::string query_json = R"({"query": ")" + query_string +
                              R"(", "from": ")" + std::to_string(from) +
                              R"(", "size": ")" + std::to_string(page_size) +
                              R"(", "sort": [{"filedAt": {"order": "asc"}}]})";

      SPDLOG_DEBUG("Insider trading DataFrame query (page from={}): {}", from, query_json);

      // Get transactions for this page
      auto task = const_cast<InsiderTradingClient*>(this)->getTransactions(query_json);
      auto result = drogon::sync_wait(task);

      if (!result.has_value()) {
        return std::unexpected(result.error());
      }

      // Append results from this page
      all_transactions.insert(all_transactions.end(),
                             result->data.begin(),
                             result->data.end());

      // Check if more pages exist
      total_count = result->total.value;
      has_more = (from + page_size) < total_count;
      from += page_size;

      // Warn if hitting 10k API limit
      if (result->total.relation == "gte" && total_count >= 10000) {
        SPDLOG_WARN("Query returned 10,000+ insider transactions (API limit reached). "
                   "Consider narrowing date range: {} to {}",from_date, to_date);
        break;
      }

      // Stop if we got fewer results than requested (end of data)
      if (result->data.size() < static_cast<size_t>(page_size)) {
        break;
      }
    }

    SPDLOG_INFO("Fetched {} insider transactions across {} page(s) for ticker={} from {} to {}",
               all_transactions.size(), (from / page_size), ticker, from_date, to_date);

    const auto &transactions = all_transactions;

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

    // Apply daily aggregation if is_eod=true to guarantee unique index
    if (is_eod) {
      // Manual daily aggregation with comma-separated concatenation for string columns
      // Group rows by day (truncate timestamp to midnight)
      std::map<int64_t, std::vector<size_t>> day_to_rows;

      for (size_t i = 0; i < filed_at_ns.size(); i++) {
        // Truncate to start of day (86400000000000 ns = 1 day)
        int64_t day_ns = (filed_at_ns[i] / 86400000000000LL) * 86400000000000LL;
        day_to_rows[day_ns].push_back(i);
      }

      // Build aggregated columns
      std::vector<int64_t> agg_index_ns;
      std::vector<std::string> agg_transaction_dates, agg_owner_names, agg_transaction_codes;
      std::vector<double> agg_shares_vec, agg_prices, agg_ownership_after;

      // Helper to join unique strings with commas
      auto join_strings = [](const std::set<std::string>& strings) -> std::string {
        if (strings.empty()) return "";
        if (strings.size() == 1) return *strings.begin();
        return std::accumulate(std::next(strings.begin()), strings.end(),
                              *strings.begin(),
                              [](const std::string& a, const std::string& b) { return a + "," + b; });
      };

      for (const auto& [day_ns, row_indices] : day_to_rows) {
        agg_index_ns.push_back(day_ns);

        // Collect unique strings and aggregate numerics
        std::set<std::string> unique_dates, unique_owners, unique_codes;
        double total_shares = 0.0;
        double sum_prices = 0.0;
        double last_ownership = 0.0;

        for (size_t idx : row_indices) {
          unique_dates.insert(transaction_dates[idx]);
          unique_owners.insert(owner_names[idx]);
          unique_codes.insert(transaction_codes[idx]);
          total_shares += shares[idx];
          sum_prices += prices[idx];
          last_ownership = ownership_after[idx];
        }

        agg_transaction_dates.push_back(join_strings(unique_dates));
        agg_owner_names.push_back(join_strings(unique_owners));
        agg_transaction_codes.push_back(join_strings(unique_codes));
        agg_shares_vec.push_back(total_shares);
        agg_prices.push_back(sum_prices / static_cast<double>(row_indices.size()));
        agg_ownership_after.push_back(last_ownership);
      }

      // Build aggregated DataFrame
      std::vector<arrow::ChunkedArrayPtr> agg_data{
        epoch_frame::factory::array::make_array(agg_transaction_dates),
        epoch_frame::factory::array::make_array(agg_owner_names),
        epoch_frame::factory::array::make_array(agg_transaction_codes),
        epoch_frame::factory::array::make_array(agg_shares_vec),
        epoch_frame::factory::array::make_array(agg_prices),
        epoch_frame::factory::array::make_array(agg_ownership_after)
      };

      auto agg_index = epoch_frame::factory::index::make_datetime_index(agg_index_ns, "filed_at", "UTC");
      auto result_df = epoch_frame::make_dataframe(agg_index, agg_data, columns);

      SPDLOG_INFO("Aggregated to {} daily rows (is_eod=true) with comma-separated string columns", result_df.num_rows());
      return result_df;
    }

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
    code,
    opts.is_eod
  );
}

drogon::Task<Expected<epoch_frame::DataFrame>>
InsiderTradingClient::getTransactionsDataFrameAsync(std::string ticker,
                                                    std::string from_date,
                                                    std::string to_date,
                                                    std::optional<epoch_core::TransactionCode> transaction_code,
                                                    bool is_eod) const {
  // For async, just wrap the synchronous version
  // Could be optimized to use async query methods internally
  co_return getTransactionsDataFrame(ticker, from_date, to_date, transaction_code, is_eod);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
InsiderTradingClient::getTransactionsDataFrameAsync(InsiderTradingOptions opts) const {
  co_return getTransactionsDataFrame(opts);
}

} // namespace data_sdk::sec
