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
  Expected<epoch_frame::DataFrame>
  getNews(std::optional<std::string> ticker = std::nullopt,
          std::optional<std::string> published_utc = std::nullopt,
          std::optional<std::string> published_utc_gte = std::nullopt,
          std::optional<std::string> published_utc_lte = std::nullopt,
          std::optional<int> limit = std::nullopt,
          std::optional<std::string> sort = std::string("published_utc"),
          std::optional<std::string> order = std::string("desc")) const;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace data_sdk::polygon
