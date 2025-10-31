#include "epoch_data_sdk/sec/render_client.hpp"

#include <regex>
#include <spdlog/spdlog.h>

namespace data_sdk::sec {

RenderClient::RenderClient(Options options)
    : BaseClient(std::move(options)) {
  // Archive API uses different base URL
  options_.base_url = "https://archive.sec-api.io";
  httpClient_ = drogon::HttpClient::newHttpClient(options_.base_url, httpClient_->getLoop());
}

drogon::Task<Expected<std::string>>
RenderClient::getFiling(const std::string &filing_url) {
  try {
    // Remove "ix?doc=/" from URL
    std::string filename = std::regex_replace(filing_url, std::regex(R"(ix\?doc=/)"), "");

    // Remove SEC base URL prefix
    filename = std::regex_replace(filename,
                                 std::regex(R"(https://www\.sec\.gov/Archives/edgar/data)"),
                                 "");

    // Build path with token
    auto result = co_await httpAsyncGet(filename);

    if (!result) {
      co_return std::unexpected(result.error());
    }

    co_return *result;
  } catch (const std::exception &e) {
    SPDLOG_ERROR("Error in getFiling: {}", e.what());
    co_return std::unexpected(HttpError{0, std::string("Error: ") + e.what()});
  }
}

drogon::Task<Expected<std::string>>
RenderClient::getFile(const std::string &file_url) {
  co_return co_await getFiling(file_url);
}

} // namespace data_sdk::sec
