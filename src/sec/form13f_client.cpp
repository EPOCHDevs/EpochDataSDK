#include "epoch_data_sdk/sec/form13f_client.hpp"

#include <chrono>
#include <spdlog/spdlog.h>
#include <glaze/glaze.hpp>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

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

    filtered_response.total = static_cast<int>(filtered_response.data.size());

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
                                    const std::string &to_date) const {
  try {
    // Build query with date range
    std::string query_string = "nameOfIssuer:*" + ticker + "*";

    // Add date range filter (assuming filingDate field exists)
    query_string += " AND filingDate:[" + from_date + " TO " + to_date + "]";

    // Build query JSON with ascending sort for chronological order
    std::string query_json = R"({"query": ")" + query_string +
                            R"(", "from": "0", "size": "1000",)" +
                            R"( "sort": [{"filingDate": {"order": "asc"}}]})";

    SPDLOG_DEBUG("Form 13F DataFrame query: {}", query_json);

    // Get holdings using existing method (synchronous version)
    auto task = const_cast<Form13FClient*>(this)->getHoldings(query_json);
    auto result = drogon::sync_wait(task);

    if (!result.has_value()) {
      return std::unexpected(result.error());
    }

    const auto &holdings = result->data;

    // Build column vectors (only fundamental data)
    std::vector<std::string> security_types, investment_discretions;
    std::vector<int> shares_vec;
    std::vector<double> values;

    shares_vec.reserve(holdings.size());
    values.reserve(holdings.size());
    security_types.reserve(holdings.size());
    investment_discretions.reserve(holdings.size());

    for (const auto &holding : holdings) {
      shares_vec.push_back(holding.shares);
      values.push_back(holding.value);
      security_types.push_back(holding.shOrPrn);
      investment_discretions.push_back(holding.investmentDiscretion);
    }

    // Build DataFrame columns (ticker/cusip/security_name removed - user already provided ticker)
    std::vector<std::string> columns = {
      "shares", "value", "security_type", "investment_discretion"
    };

    std::vector<arrow::ChunkedArrayPtr> data{
      epoch_frame::factory::array::make_array(shares_vec),
      epoch_frame::factory::array::make_array(values),
      epoch_frame::factory::array::make_array(security_types),
      epoch_frame::factory::array::make_array(investment_discretions)
    };

    // Create Arrow table and DataFrame with default range index
    auto schema = arrow::schema({
      arrow::field("shares", arrow::int32()),
      arrow::field("value", arrow::float64()),
      arrow::field("security_type", arrow::utf8()),
      arrow::field("investment_discretion", arrow::utf8())
    });
    auto table = arrow::Table::Make(schema, data);
    auto df = epoch_frame::make_dataframe(table);

    SPDLOG_INFO("Built 13F holdings DataFrame: {} rows for ticker={} from {} to {}",
                holdings.size(), ticker, from_date, to_date);

    return df;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getHoldingsDataFrame: {}", e.what());
    return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

Expected<epoch_frame::DataFrame>
Form13FClient::getHoldingsDataFrame(const Form13FOptions &opts) const {
  if (opts.ticker.has_value()) {
    return getHoldingsDataFrame(*opts.ticker, opts.from_date, opts.to_date);
  } else if (opts.cusip.has_value()) {
    // Use CUSIP-based query
    return getHoldingsDataFrame(*opts.cusip, opts.from_date, opts.to_date);
  } else {
    return std::unexpected(HttpError{0, "Must specify either ticker or cusip"});
  }
}

drogon::Task<Expected<epoch_frame::DataFrame>>
Form13FClient::getHoldingsDataFrameAsync(std::string ticker,
                                         std::string from_date,
                                         std::string to_date) const {
  co_return getHoldingsDataFrame(ticker, from_date, to_date);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
Form13FClient::getHoldingsDataFrameAsync(Form13FOptions opts) const {
  co_return getHoldingsDataFrame(opts);
}

} // namespace data_sdk::sec
