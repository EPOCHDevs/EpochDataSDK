#include "polygon/news_client.hpp"

#include <glaze/glaze.hpp>

#include <arrow/array.h>
#include <arrow/builder.h>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "polygon/base_client.hpp"

namespace data_sdk::polygon {

namespace {

// News JSON structure
struct Publisher {
  std::optional<std::string> name;
  std::optional<std::string> homepage_url;
  std::optional<std::string> logo_url;
  std::optional<std::string> favicon_url;
};

struct Insight {
  std::optional<std::string> ticker;
  std::optional<std::string> sentiment;
  std::optional<std::string> sentiment_reasoning;
};

struct NewsArticle {
  std::optional<std::string> id;
  std::optional<Publisher> publisher;
  std::optional<std::string> title;
  std::optional<std::string> author;
  std::optional<std::string> published_utc;
  std::optional<std::string> article_url;
  std::optional<std::vector<std::string>> tickers;
  std::optional<std::string> amp_url;
  std::optional<std::string> image_url;
  std::optional<std::string> description;
  std::optional<std::vector<std::string>> keywords;
  std::optional<std::vector<Insight>> insights;
};

struct NewsResponse {
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<NewsArticle> results;
  std::string status;
  int count;
};

// Helper function to create nullable string array from optional values
inline arrow::ChunkedArrayPtr makeNullableStringArray(
    const std::vector<std::optional<std::string>>& values) {
  arrow::StringBuilder builder;
  auto status = builder.Reserve(values.size());
  if (!status.ok()) {
    throw std::runtime_error("Failed to reserve string builder: " + status.ToString());
  }

  for (const auto& val : values) {
    if (val.has_value()) {
      auto append_status = builder.Append(*val);
      if (!append_status.ok()) {
        throw std::runtime_error("Failed to append string: " + append_status.ToString());
      }
    } else {
      auto append_status = builder.AppendNull();
      if (!append_status.ok()) {
        throw std::runtime_error("Failed to append null: " + append_status.ToString());
      }
    }
  }

  auto result = builder.Finish();
  if (!result.ok()) {
    throw std::runtime_error("Failed to finish string array: " + result.status().ToString());
  }

  auto chunked = arrow::ChunkedArray::Make({*result});
  if (!chunked.ok()) {
    throw std::runtime_error("Failed to create chunked array: " + chunked.status().ToString());
  }

  return *chunked;
}

} // namespace

// Private implementation
class NewsClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getNews(std::optional<std::string> ticker,
          std::optional<std::string> from,
          std::optional<std::string> to,
          std::optional<int> limit) const {
    return drogon::sync_wait(getNewsAsync(ticker, from, to, limit));
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getNewsAsync(std::optional<std::string> ticker,
               std::optional<std::string> from,
               std::optional<std::string> to,
               std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    if (ticker.has_value())
      q.emplace_back("ticker", *ticker);
    if (from.has_value())
      q.emplace_back("published_utc.gte", *from);
    if (to.has_value())
      q.emplace_back("published_utc.lte", *to);
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    // Always sort by published_utc ascending for consistent backtesting
    q.emplace_back("sort", "published_utc");
    q.emplace_back("order", "asc");

    const std::string path = "/v2/reference/news";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    NewsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse news JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::optional<std::string>> ids, titles, authors, urls, amp_urls, image_urls;
    std::vector<std::optional<std::string>> publisher_names, publisher_homepages, publisher_logos, publisher_favicons;
    std::vector<std::optional<std::string>> descriptions, tickers_str, keywords_str;
    std::vector<std::string> published_times; // Still need strings for timestamp parsing

    ids.reserve(N);
    published_times.reserve(N);
    titles.reserve(N);
    authors.reserve(N);
    urls.reserve(N);
    amp_urls.reserve(N);
    image_urls.reserve(N);
    publisher_names.reserve(N);
    publisher_homepages.reserve(N);
    publisher_logos.reserve(N);
    publisher_favicons.reserve(N);
    descriptions.reserve(N);
    tickers_str.reserve(N);
    keywords_str.reserve(N);

    for (const auto &r : parsed.results) {
      ids.push_back(r.id);
      published_times.push_back(r.published_utc.value_or(""));  // Need string for parsing
      titles.push_back(r.title);
      authors.push_back(r.author);
      urls.push_back(r.article_url);
      amp_urls.push_back(r.amp_url);
      image_urls.push_back(r.image_url);
      descriptions.push_back(r.description);

      // Extract all publisher fields
      std::optional<std::string> pub_name = std::nullopt;
      std::optional<std::string> pub_homepage = std::nullopt;
      std::optional<std::string> pub_logo = std::nullopt;
      std::optional<std::string> pub_favicon = std::nullopt;
      if (r.publisher.has_value()) {
        if (r.publisher->name.has_value()) pub_name = *r.publisher->name;
        if (r.publisher->homepage_url.has_value()) pub_homepage = *r.publisher->homepage_url;
        if (r.publisher->logo_url.has_value()) pub_logo = *r.publisher->logo_url;
        if (r.publisher->favicon_url.has_value()) pub_favicon = *r.publisher->favicon_url;
      }
      publisher_names.push_back(pub_name);
      publisher_homepages.push_back(pub_homepage);
      publisher_logos.push_back(pub_logo);
      publisher_favicons.push_back(pub_favicon);

      // Join tickers into comma-separated string, or nullopt if none
      std::optional<std::string> tickers_joined = std::nullopt;
      if (r.tickers.has_value() && !r.tickers->empty()) {
        std::string joined = "";
        for (size_t i = 0; i < r.tickers->size(); ++i) {
          if (i > 0) joined += ",";
          joined += (*r.tickers)[i];
        }
        tickers_joined = joined;
      }
      tickers_str.push_back(tickers_joined);

      // Join keywords into comma-separated string, or nullopt if none
      std::optional<std::string> keywords_joined = std::nullopt;
      if (r.keywords.has_value() && !r.keywords->empty()) {
        std::string joined = "";
        for (size_t i = 0; i < r.keywords->size(); ++i) {
          if (i > 0) joined += ",";
          joined += (*r.keywords)[i];
        }
        keywords_joined = joined;
      }
      keywords_str.push_back(keywords_joined);
    }

    // Normalize published_utc timestamps to midnight UTC (date-only)
    // Extract date portion (YYYY-MM-DD) from RFC3339 timestamps
    std::vector<std::string> published_dates;
    published_dates.reserve(published_times.size());
    for (const auto& ts_str : published_times) {
      if (ts_str.size() >= 10) {
        // Extract "YYYY-MM-DD" from "YYYY-MM-DDTHH:MM:SSZ"
        published_dates.push_back(ts_str.substr(0, 10));
      } else {
        published_dates.push_back("");
      }
    }

    // Convert to midnight UTC timestamps (normalized index)
    auto timestamps = parseDateStringsToMidnightUTC(published_dates);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "published_utc", "UTC");

    std::vector<std::string> columns = {
        "id", "title", "author", "description",
        "article_url", "amp_url", "image_url",
        "tickers", "keywords",
        "publisher_name", "publisher_homepage", "publisher_logo", "publisher_favicon"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        makeNullableStringArray(ids),
        makeNullableStringArray(titles),
        makeNullableStringArray(authors),
        makeNullableStringArray(descriptions),
        makeNullableStringArray(urls),
        makeNullableStringArray(amp_urls),
        makeNullableStringArray(image_urls),
        makeNullableStringArray(tickers_str),
        makeNullableStringArray(keywords_str),
        makeNullableStringArray(publisher_names),
        makeNullableStringArray(publisher_homepages),
        makeNullableStringArray(publisher_logos),
        makeNullableStringArray(publisher_favicons)};

    auto df = epoch_frame::make_dataframe(index, arrays, columns);

    // Drop duplicates: if multiple news articles on same day, keep the last one
    // (API returns sorted by published_utc ascending, so last = most recent)
    auto df_deduped = df.drop_duplicates(epoch_frame::DropDuplicatesKeepPolicy::Last);

    co_return df_deduped;
  }
};

// Public API
NewsClient::NewsClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

NewsClient::~NewsClient() = default;

Expected<epoch_frame::DataFrame>
NewsClient::getNews(std::optional<std::string> ticker,
                    std::optional<std::string> from,
                    std::optional<std::string> to,
                    std::optional<int> limit) const {
  return impl_->getNews(ticker, from, to, limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
NewsClient::getNewsAsync(std::optional<std::string> ticker,
                         std::optional<std::string> from,
                         std::optional<std::string> to,
                         std::optional<int> limit) const {
  return impl_->getNewsAsync(ticker, from, to, limit);
}

DataFrameMetadata NewsClient::getMetadata() {
  return DataFrameMetadata{
      .data_type = "news",
      .description = "Retrieve the most recent news articles related to a specified ticker, along with summaries, source details, and sentiment analysis. This endpoint consolidates relevant financial news in one place, extracting associated tickers, assigning sentiment, and providing direct links to the original sources. By incorporating publisher information, article metadata, and sentiment reasoning, users can quickly gauge market sentiment, stay informed on company developments, and integrate news insights into their trading or research workflows. Use Cases: Market sentiment analysis, investment research, automated monitoring, and portfolio strategy refinement.",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,  // News index normalized to midnight UTC (date-only); if multiple articles per day, keeps last (most recent)
      .category_prefix = "N:",
      .columns = {
          {.id = "id",
           .name = "Article ID",
           .description = "Unique identifier for the article",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "title",
           .name = "Title",
           .description = "The title of the news article",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "author",
           .name = "Author",
           .description = "The article's author",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "description",
           .name = "Description",
           .description = "A description of the article",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "article_url",
           .name = "Article URL",
           .description = "A link to the news article",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "amp_url",
           .name = "AMP URL",
           .description = "The mobile friendly Accelerated Mobile Page (AMP) URL",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "image_url",
           .name = "Image URL",
           .description = "The article's image URL",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "tickers",
           .name = "Tickers",
           .description = "The ticker symbols associated with the article (comma-separated)",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "keywords",
           .name = "Keywords",
           .description = "The keywords associated with the article which will vary depending on the publishing source (comma-separated)",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "publisher_name",
           .name = "Publisher Name",
           .description = "The name of the publisher",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "publisher_homepage",
           .name = "Publisher Homepage",
           .description = "The homepage URL of the publisher",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "publisher_logo",
           .name = "Publisher Logo",
           .description = "The logo URL of the publisher",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "publisher_favicon",
           .name = "Publisher Favicon",
           .description = "The favicon URL of the publisher",
           .type = ArrowType::STRING,
           .nullable = true},
      }};
}

} // namespace data_sdk::polygon
