#include "epoch_data_sdk/sec/mapping_client.hpp"

#include <spdlog/spdlog.h>
#include <glaze/glaze.hpp>

namespace data_sdk::sec {

MappingClient::MappingClient(Options options)
    : BaseClient(std::move(options)) {
  // Mapping API uses different base URL
  options_.base_url = "https://api.sec-api.io";
  httpClient_ = drogon::HttpClient::newHttpClient(options_.base_url, httpClient_->getLoop());
}

drogon::Task<Expected<MappingData>>
MappingClient::resolveByTicker(const std::string &ticker) {
  co_return co_await resolve("ticker", ticker);
}

drogon::Task<Expected<MappingData>>
MappingClient::resolveByCIK(const std::string &cik) {
  co_return co_await resolve("cik", cik);
}

drogon::Task<Expected<MappingData>>
MappingClient::resolveByCUSIP(const std::string &cusip) {
  co_return co_await resolve("cusip", cusip);
}

drogon::Task<Expected<std::vector<MappingData>>>
MappingClient::resolveByName(const std::string &name) {
  try {
    auto result = co_await httpAsyncGet("/mapping/name/" + name);

    if (!result) {
      co_return std::unexpected(result.error());
    }

    // Parse JSON response using glaze
    std::vector<MappingData> mappings;
    auto parse_error = glz::read_json(mappings, *result);

    if (parse_error) {
      SPDLOG_ERROR("JSON parsing error in resolveByName: {}", glz::format_error(parse_error, *result));
      co_return std::unexpected(HttpError{0, "JSON parsing error"});
    }

    co_return mappings;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in resolveByName: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<MappingData>>
MappingClient::resolve(const std::string &parameter, const std::string &value) {
  try {
    // Build path: /mapping/{parameter}/{value}
    std::string path = "/mapping/" + parameter + "/" + value;
    auto result = co_await httpAsyncGet(path);

    if (!result) {
      co_return std::unexpected(result.error());
    }

    // Log the raw response for debugging
    SPDLOG_DEBUG("Mapping API response: {}", *result);

    // Parse JSON response - API returns array with single object
    std::vector<MappingData> mappings;
    auto parse_error = glz::read_json(mappings, *result);

    if (parse_error) {
      SPDLOG_ERROR("JSON parsing error in resolve: {}", glz::format_error(parse_error, *result));
      SPDLOG_ERROR("Full response: {}", *result);
      co_return std::unexpected(HttpError{0, "JSON parsing error"});
    }

    if (mappings.empty()) {
      co_return std::unexpected(HttpError{404, "No mapping found"});
    }

    co_return mappings[0];
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in resolve: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

} // namespace data_sdk::sec
