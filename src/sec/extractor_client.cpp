#include "epoch_data_sdk/sec/extractor_client.hpp"

#include <spdlog/spdlog.h>

namespace data_sdk::sec {

ExtractorClient::ExtractorClient(Options options)
    : BaseClient(std::move(options)) {
  // Extractor API uses different base URL
  options_.base_url = "https://api.sec-api.io";
  httpClient_ = drogon::HttpClient::newHttpClient(options_.base_url, httpClient_->getLoop());
}

drogon::Task<Expected<std::string>>
ExtractorClient::getSection(const std::string &filing_url,
                            const std::string &section,
                            const std::string &return_type) {
  try {
    // Build query parameters
    std::vector<std::pair<std::string, std::string>> query = {
        {"url", filing_url},
        {"item", section},
        {"type", return_type}
    };

    auto result = co_await httpAsyncGet("/extractor", query);

    if (!result) {
      co_return std::unexpected(result.error());
    }

    co_return *result;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getSection: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

} // namespace data_sdk::sec
