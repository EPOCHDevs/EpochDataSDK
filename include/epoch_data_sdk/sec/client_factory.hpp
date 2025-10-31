#pragma once

#include <memory>
#include <string>
#include <fstream>
#include <sstream>

#include "query_client.hpp"
#include "mapping_client.hpp"
#include "render_client.hpp"
#include "extractor_client.hpp"
#include "insider_trading_client.hpp"
#include "form13f_client.hpp"
#include "options.hpp"

namespace data_sdk::sec {

/**
 * @brief Factory for creating SEC API clients with common configuration
 */
class ClientFactory {
public:
  /**
   * @brief Create a ClientFactory with options
   */
  explicit ClientFactory(Options options) : options_(std::move(options)) {}

  /**
   * @brief Create a ClientFactory loading API key from .env file
   *
   * @param env_path Path to .env file (default: ".env")
   * @param use_drogon_main_loop Whether to use Drogon's main event loop
   */
  static ClientFactory fromEnv(const std::string &env_path = ".env",
                               bool use_drogon_main_loop = false) {
    Options options;
    options.api_key = loadApiKeyFromEnv(env_path);
    options.use_drogon_main_loop = use_drogon_main_loop;
    return ClientFactory(options);
  }

  /**
   * @brief Create QueryClient
   */
  std::unique_ptr<QueryClient> createQueryClient() const {
    return std::make_unique<QueryClient>(options_);
  }

  /**
   * @brief Create MappingClient
   */
  std::unique_ptr<MappingClient> createMappingClient() const {
    return std::make_unique<MappingClient>(options_);
  }

  /**
   * @brief Create RenderClient
   */
  std::unique_ptr<RenderClient> createRenderClient() const {
    return std::make_unique<RenderClient>(options_);
  }

  /**
   * @brief Create ExtractorClient
   */
  std::unique_ptr<ExtractorClient> createExtractorClient() const {
    return std::make_unique<ExtractorClient>(options_);
  }

  /**
   * @brief Create InsiderTradingClient
   */
  std::unique_ptr<InsiderTradingClient> createInsiderTradingClient() const {
    return std::make_unique<InsiderTradingClient>(options_);
  }

  /**
   * @brief Create Form13FClient
   */
  std::unique_ptr<Form13FClient> createForm13FClient() const {
    return std::make_unique<Form13FClient>(options_);
  }

  /**
   * @brief Get the current options
   */
  const Options& getOptions() const { return options_; }

private:
  Options options_;

  /**
   * @brief Load SEC_API_KEY from .env file
   */
  static std::string loadApiKeyFromEnv(const std::string &env_path) {
    std::ifstream file(env_path);
    if (!file.is_open()) {
      throw std::runtime_error("Failed to open .env file: " + env_path);
    }

    std::string line;
    while (std::getline(file, line)) {
      // Skip empty lines and comments
      if (line.empty() || line[0] == '#') {
        continue;
      }

      // Find SEC_API_KEY
      if (line.find("SEC_API_KEY") != std::string::npos) {
        auto pos = line.find('=');
        if (pos != std::string::npos) {
          std::string key = line.substr(pos + 1);
          // Remove quotes if present
          if (!key.empty() && key.front() == '"') {
            key = key.substr(1);
          }
          if (!key.empty() && key.back() == '"') {
            key = key.substr(0, key.size() - 1);
          }
          return key;
        }
      }
    }

    throw std::runtime_error("SEC_API_KEY not found in .env file");
  }
};

} // namespace data_sdk::sec
