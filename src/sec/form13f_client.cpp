#include "epoch_data_sdk/sec/form13f_client.hpp"

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

Form13FClient::Form13FClient(Options options)
    : BaseClient(std::move(options)) {}

drogon::Task<Expected<ListResponse<Form13FHolding>>>
Form13FClient::getHoldings(const std::string &query_json) {
  try {
    auto result = co_await httpAsyncPost("/", query_json);

    if (!result) {
      co_return std::unexpected(result.error());
    }

    // Parse JSON response using glaze
    ListResponse<Form13FHolding> response;
    auto parse_error = glz::read_json(response, *result);

    if (parse_error) {
      SPDLOG_ERROR("JSON parsing error in getHoldings: {}",
                   glz::format_error(parse_error, *result));
      co_return std::unexpected(HttpError{0, "JSON parsing error"});
    }

    co_return response;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getHoldings: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<ListResponse<Form13FHolding>>>
Form13FClient::getHoldersByTicker(const std::string &ticker, int size) {
  try {
    // First, we need to get the CUSIP for the ticker using MappingClient
    // For now, we'll search by nameOfIssuer which often matches the ticker
    // In production, you'd want to resolve ticker -> CUSIP first

    std::string query_string = "nameOfIssuer:*" + ticker + "*";

    // Build query JSON
    std::string query_json = R"({"query": ")" + query_string +
                            R"(", "from": "0", "size": ")" + std::to_string(size) +
                            R"(", "sort": [{"value": {"order": "desc"}}]})";

    SPDLOG_DEBUG("13F holders query JSON: {}", query_json);
    co_return co_await getHoldings(query_json);
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getHoldersByTicker: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<ListResponse<Form13FHolding>>>
Form13FClient::getHoldersByCUSIP(const std::string &cusip, int size) {
  try {
    std::string query_string = "cusip:" + cusip;

    // Build query JSON
    std::string query_json = R"({"query": ")" + query_string +
                            R"(", "from": "0", "size": ")" + std::to_string(size) +
                            R"(", "sort": [{"value": {"order": "desc"}}]})";

    co_return co_await getHoldings(query_json);
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getHoldersByCUSIP: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<ListResponse<Form13FHolding>>>
Form13FClient::getHoldingsByInstitution(const std::string &institution_cik, int size) {
  try {
    // Note: The actual field name for institution CIK in the API may vary
    // This might need to be adjusted based on actual API response structure
    std::string query_string = "cik:" + institution_cik;

    // Build query JSON
    std::string query_json = R"({"query": ")" + query_string +
                            R"(", "from": "0", "size": ")" + std::to_string(size) +
                            R"(", "sort": [{"value": {"order": "desc"}}]})";

    co_return co_await getHoldings(query_json);
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getHoldingsByInstitution: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<ListResponse<Form13FHolding>>>
Form13FClient::getLargePositions(const std::string &ticker,
                                 double min_value,
                                 int size) {
  try {
    // Get all holders and filter by value
    // Fetch more than needed since we'll filter
    auto result = co_await getHoldersByTicker(ticker, size * 2);

    if (!result.has_value()) {
      co_return std::unexpected(result.error());
    }

    // Filter by minimum value
    ListResponse<Form13FHolding> filtered_response;
    filtered_response.data.reserve(size);

    for (const auto &holding : result->data) {
      if (holding.value >= min_value &&
          filtered_response.data.size() < static_cast<size_t>(size)) {
        filtered_response.data.push_back(holding);
      }
    }

    filtered_response.total.value = static_cast<int>(filtered_response.data.size());
    filtered_response.total.relation = "eq";

    SPDLOG_DEBUG("Filtered {} large positions (>= ${}) for {} from {} total holders",
                filtered_response.data.size(), min_value, ticker, result->data.size());

    co_return filtered_response;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getLargePositions: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

// ========== DataFrame Methods Implementation ==========

Expected<epoch_frame::DataFrame>
Form13FClient::getHoldingsDataFrame(const std::string &ticker,
                                    const std::string &from_date,
                                    const std::string &to_date,
                                    bool is_eod) const {
  try {
    // Build query with date range
    std::string query_string = "nameOfIssuer:*" + ticker + "*";

    // Add date range filter (assuming filingDate field exists)
    query_string += " AND filingDate:[" + from_date + " TO " + to_date + "]";

    // Pagination loop to fetch all results
    std::vector<Form13FHolding> all_holdings;
    int from = 0;
    const int page_size = 50;  // SEC API maximum
    bool has_more = true;
    int total_count = 0;

    while (has_more && from < 10000) {
      // Build query JSON with current offset
      std::string query_json = R"({"query": ")" + query_string +
                              R"(", "from": ")" + std::to_string(from) +
                              R"(", "size": ")" + std::to_string(page_size) +
                              R"(", "sort": [{"filingDate": {"order": "asc"}}]})";

      SPDLOG_DEBUG("Form 13F DataFrame query (page from={}): {}", from, query_json);

      // Get holdings for this page
      auto task = const_cast<Form13FClient*>(this)->getHoldings(query_json);
      auto result = drogon::sync_wait(task);

      if (!result.has_value()) {
        return std::unexpected(result.error());
      }

      // Append results from this page
      all_holdings.insert(all_holdings.end(),
                         result->data.begin(),
                         result->data.end());

      // Check if more pages exist
      total_count = result->total.value;
      has_more = (from + page_size) < total_count;
      from += page_size;

      // Warn if hitting 10k API limit
      if (result->total.relation == "gte" && total_count >= 10000) {
        SPDLOG_WARN("Query returned 10,000+ 13F holdings (API limit reached). "
                   "Consider narrowing date range: {} to {}", from_date, to_date);
        break;
      }

      // Stop if we got fewer results than requested (end of data)
      if (result->data.size() < static_cast<size_t>(page_size)) {
        break;
      }
    }

    SPDLOG_INFO("Fetched {} 13F holdings across {} page(s) for ticker={} from {} to {}",
               all_holdings.size(), (from / page_size), ticker, from_date, to_date);

    const auto &holdings = all_holdings;

    // Build column vectors
    std::vector<std::string> filed_at_strings, period_of_reports, institution_ciks;
    std::vector<std::string> security_types, investment_discretions;
    std::vector<int> shares_vec;
    std::vector<double> values;

    filed_at_strings.reserve(holdings.size());
    period_of_reports.reserve(holdings.size());
    institution_ciks.reserve(holdings.size());
    shares_vec.reserve(holdings.size());
    values.reserve(holdings.size());
    security_types.reserve(holdings.size());
    investment_discretions.reserve(holdings.size());

    for (const auto &holding : holdings) {
      filed_at_strings.push_back(holding.filedAt);
      period_of_reports.push_back(holding.periodOfReport);
      institution_ciks.push_back(holding.cik);
      shares_vec.push_back(holding.shares);
      values.push_back(holding.value);
      security_types.push_back(holding.shOrPrn);
      investment_discretions.push_back(holding.investmentDiscretion);
    }

    // Parse ISO 8601 timestamps to nanoseconds (filed_at = as-of date for backtesting)
    auto filed_at_ns = parseISO8601ToNanoseconds(filed_at_strings);

    // Build DataFrame columns
    std::vector<std::string> columns = {
      "period_of_report", "institution_cik", "shares", "value",
      "security_type", "investment_discretion"
    };

    std::vector<arrow::ChunkedArrayPtr> data{
      epoch_frame::factory::array::make_array(period_of_reports),
      epoch_frame::factory::array::make_array(institution_ciks),
      epoch_frame::factory::array::make_array(shares_vec),
      epoch_frame::factory::array::make_array(values),
      epoch_frame::factory::array::make_array(security_types),
      epoch_frame::factory::array::make_array(investment_discretions)
    };

    // Create datetime index from filed_at timestamps (NO FORWARD BIAS)
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
      std::vector<std::string> agg_period_of_reports, agg_institution_ciks;
      std::vector<std::string> agg_security_types, agg_investment_discretions;
      std::vector<int> agg_shares_vec;
      std::vector<double> agg_values;

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
        std::set<std::string> unique_periods, unique_ciks, unique_types, unique_discretions;
        int total_shares = 0;
        double total_value = 0.0;

        for (size_t idx : row_indices) {
          unique_periods.insert(period_of_reports[idx]);
          unique_ciks.insert(institution_ciks[idx]);
          unique_types.insert(security_types[idx]);
          unique_discretions.insert(investment_discretions[idx]);
          total_shares += shares_vec[idx];
          total_value += values[idx];
        }

        agg_period_of_reports.push_back(join_strings(unique_periods));
        agg_institution_ciks.push_back(join_strings(unique_ciks));
        agg_security_types.push_back(join_strings(unique_types));
        agg_investment_discretions.push_back(join_strings(unique_discretions));
        agg_shares_vec.push_back(total_shares);
        agg_values.push_back(total_value);
      }

      // Build aggregated DataFrame
      std::vector<arrow::ChunkedArrayPtr> agg_data{
        epoch_frame::factory::array::make_array(agg_period_of_reports),
        epoch_frame::factory::array::make_array(agg_institution_ciks),
        epoch_frame::factory::array::make_array(agg_shares_vec),
        epoch_frame::factory::array::make_array(agg_values),
        epoch_frame::factory::array::make_array(agg_security_types),
        epoch_frame::factory::array::make_array(agg_investment_discretions)
      };

      auto agg_index = epoch_frame::factory::index::make_datetime_index(agg_index_ns, "filed_at", "UTC");
      auto result_df = epoch_frame::make_dataframe(agg_index, agg_data, columns);

      SPDLOG_INFO("Aggregated to {} daily rows (is_eod=true) with comma-separated string columns", result_df.num_rows());
      return result_df;
    }

    return df;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getHoldingsDataFrame: {}", e.what());
    return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

Expected<epoch_frame::DataFrame>
Form13FClient::getHoldingsDataFrame(const Form13FOptions &opts) const {
  if (opts.ticker.has_value()) {
    return getHoldingsDataFrame(*opts.ticker, opts.from_date, opts.to_date, opts.is_eod);
  } else if (opts.cusip.has_value()) {
    // Use CUSIP-based query
    return getHoldingsDataFrame(*opts.cusip, opts.from_date, opts.to_date, opts.is_eod);
  } else {
    return std::unexpected(HttpError{0, "Must specify either ticker or cusip"});
  }
}

drogon::Task<Expected<epoch_frame::DataFrame>>
Form13FClient::getHoldingsDataFrameAsync(std::string ticker,
                                         std::string from_date,
                                         std::string to_date,
                                         bool is_eod) const {
  co_return getHoldingsDataFrame(ticker, from_date, to_date, is_eod);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
Form13FClient::getHoldingsDataFrameAsync(Form13FOptions opts) const {
  co_return getHoldingsDataFrame(opts);
}

} // namespace data_sdk::sec
