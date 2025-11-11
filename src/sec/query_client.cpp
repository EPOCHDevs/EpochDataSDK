#include "sec/query_client.hpp"
#include "sec/enums.hpp"

#include <spdlog/spdlog.h>
#include <glaze/glaze.hpp>

namespace data_sdk::sec {

QueryClient::QueryClient(Options options)
    : BaseClient(std::move(options)) {}

drogon::Task<Expected<QueryResponse>>
QueryClient::getFilings(const std::string &query_json) {
  try {
    auto result = co_await httpAsyncPost("/", query_json);

    if (!result) {
      co_return std::unexpected(result.error());
    }

    // Parse JSON response using glaze
    QueryResponse response;
    auto parse_error = glz::read_json(response, *result);

    if (parse_error) {
      SPDLOG_ERROR("JSON parsing error in getFilings: {}", glz::format_error(parse_error, *result));
      co_return std::unexpected(HttpError{0, "JSON parsing error"});
    }

    co_return response;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getFilings: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<QueryResponse>>
QueryClient::getFilingsByTicker(const std::string &ticker,
                                const std::string &form_type,
                                int size) {
  try {
    // Build query string - use escaped quotes for proper JSON encoding
    std::string query_string = "ticker:" + ticker;
    if (!form_type.empty()) {
      query_string += " AND formType:\\\"" + form_type + "\\\"";
    }

    // Build query JSON manually (compact format, matching Python example)
    std::string query_json = R"({"query": ")" + query_string + R"(", "from": "0", "size": ")" + std::to_string(size) + R"(", "sort": [{"filedAt": {"order": "desc"}}]})";

    SPDLOG_DEBUG("Sending query JSON for ticker: {}", query_json);
    co_return co_await getFilings(query_json);
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getFilingsByTicker: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<QueryResponse>>
QueryClient::getFilingsByCIK(const std::string &cik,
                             const std::string &form_type,
                             int size) {
  try {
    // Build query string - use escaped quotes for proper JSON encoding
    std::string query_string = "cik:" + cik;
    if (!form_type.empty()) {
      query_string += " AND formType:\\\"" + form_type + "\\\"";
    }

    // Build query JSON manually (compact format, matching Python example)
    std::string query_json = R"({"query": ")" + query_string + R"(", "from": "0", "size": ")" + std::to_string(size) + R"(", "sort": [{"filedAt": {"order": "desc"}}]})";

    co_return co_await getFilings(query_json);
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getFilingsByCIK: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<QueryResponse>>
QueryClient::getFilingsByTicker(const std::string &ticker,
                                epoch_core::FormType form_type,
                                int size) {
  // Convert enum to string and call string overload
  co_return co_await getFilingsByTicker(ticker, formTypeToString(form_type), size);
}

drogon::Task<Expected<QueryResponse>>
QueryClient::getFilingsByCIK(const std::string &cik,
                             epoch_core::FormType form_type,
                             int size) {
  // Convert enum to string and call string overload
  co_return co_await getFilingsByCIK(cik, formTypeToString(form_type), size);
}

} // namespace data_sdk::sec
