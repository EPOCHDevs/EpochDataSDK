#pragma once

#include <optional>
#include <string>

#include <epoch_frame/dataframe.h>

#include "error.hpp"
#include "options.hpp"

namespace data_sdk::polygon {

template <typename T> using Expected = std::expected<T, HttpError>;

// NewsClient - Handles news article data
// OPTIONAL for backtesting: sentiment analysis, event-driven strategies
class NewsClient {
public:
  explicit NewsClient(Options options);
  ~NewsClient();

  // Prevent copying
  NewsClient(const NewsClient&) = delete;
  NewsClient& operator=(const NewsClient&) = delete;

  // Allow moving
  NewsClient(NewsClient&&) = default;
  NewsClient& operator=(NewsClient&&) = default;

  // Get historical news articles
  // Useful for sentiment analysis and event-driven backtesting
  // ticker: Filter by ticker symbol (e.g., "AAPL")
  // from/to: Date range for published_utc in YYYY-MM-DD format
  // limit: Maximum number of results (default 10)
  Expected<epoch_frame::DataFrame>
  getNews(std::optional<std::string> ticker = std::nullopt,
          std::optional<std::string> from = std::nullopt,
          std::optional<std::string> to = std::nullopt,
          std::optional<int> limit = 10) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
