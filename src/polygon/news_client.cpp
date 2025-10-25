#include "epoch_data_sdk/polygon/news_client.hpp"

#include <glaze/glaze.hpp>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "epoch_data_sdk/polygon/base_client.hpp"

namespace data_sdk::polygon {

namespace {

// News JSON structure
struct Publisher {
  std::optional<std::string> name;
  std::optional<std::string> homepage_url;
  std::optional<std::string> logo_url;
  std::optional<std::string> favicon_url;
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
};

struct NewsResponse {
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<NewsArticle> results;
  std::string status;
  int count;
};

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
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    NewsResponse parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse news JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::string> ids, published_times, titles, authors, urls;
    std::vector<std::string> publisher_names, descriptions, tickers_str;

    ids.reserve(N);
    published_times.reserve(N);
    titles.reserve(N);
    authors.reserve(N);
    urls.reserve(N);
    publisher_names.reserve(N);
    descriptions.reserve(N);
    tickers_str.reserve(N);

    for (const auto &r : parsed.results) {
      ids.push_back(r.id.value_or(""));
      published_times.push_back(r.published_utc.value_or(""));
      titles.push_back(r.title.value_or(""));
      authors.push_back(r.author.value_or(""));
      urls.push_back(r.article_url.value_or(""));
      descriptions.push_back(r.description.value_or(""));

      // Extract publisher name
      std::string pub_name = "";
      if (r.publisher.has_value() && r.publisher->name.has_value()) {
        pub_name = *r.publisher->name;
      }
      publisher_names.push_back(pub_name);

      // Join tickers into comma-separated string
      std::string tickers_joined = "";
      if (r.tickers.has_value() && !r.tickers->empty()) {
        for (size_t i = 0; i < r.tickers->size(); ++i) {
          if (i > 0) tickers_joined += ",";
          tickers_joined += (*r.tickers)[i];
        }
      }
      tickers_str.push_back(tickers_joined);
    }

    // Convert RFC3339 published_utc strings to nanosecond timestamps
    auto timestamps = parseRFC3339ToNanoseconds(published_times);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "published_utc", "UTC");

    std::vector<std::string> columns = {
        "id", "tickers", "title", "author", "publisher",
        "article_url", "description"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(ids),
        epoch_frame::factory::array::make_array(tickers_str),
        epoch_frame::factory::array::make_array(titles),
        epoch_frame::factory::array::make_array(authors),
        epoch_frame::factory::array::make_array(publisher_names),
        epoch_frame::factory::array::make_array(urls),
        epoch_frame::factory::array::make_array(descriptions)};

    return epoch_frame::make_dataframe(index, arrays, columns);
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
    std::vector<std::string> ids, published_times, titles, authors, urls;
    std::vector<std::string> publisher_names, descriptions, tickers_str;

    ids.reserve(N);
    published_times.reserve(N);
    titles.reserve(N);
    authors.reserve(N);
    urls.reserve(N);
    publisher_names.reserve(N);
    descriptions.reserve(N);
    tickers_str.reserve(N);

    for (const auto &r : parsed.results) {
      ids.push_back(r.id.value_or(""));
      published_times.push_back(r.published_utc.value_or(""));
      titles.push_back(r.title.value_or(""));
      authors.push_back(r.author.value_or(""));
      urls.push_back(r.article_url.value_or(""));
      descriptions.push_back(r.description.value_or(""));

      // Extract publisher name
      std::string pub_name = "";
      if (r.publisher.has_value() && r.publisher->name.has_value()) {
        pub_name = *r.publisher->name;
      }
      publisher_names.push_back(pub_name);

      // Join tickers into comma-separated string
      std::string tickers_joined = "";
      if (r.tickers.has_value() && !r.tickers->empty()) {
        for (size_t i = 0; i < r.tickers->size(); ++i) {
          if (i > 0) tickers_joined += ",";
          tickers_joined += (*r.tickers)[i];
        }
      }
      tickers_str.push_back(tickers_joined);
    }

    // Convert RFC3339 published_utc strings to nanosecond timestamps
    auto timestamps = parseRFC3339ToNanoseconds(published_times);
    auto index = epoch_frame::factory::index::make_datetime_index(
        timestamps, "published_utc", "UTC");

    std::vector<std::string> columns = {
        "id", "tickers", "title", "author", "publisher",
        "article_url", "description"};
    std::vector<arrow::ChunkedArrayPtr> arrays{
        epoch_frame::factory::array::make_array(ids),
        epoch_frame::factory::array::make_array(tickers_str),
        epoch_frame::factory::array::make_array(titles),
        epoch_frame::factory::array::make_array(authors),
        epoch_frame::factory::array::make_array(publisher_names),
        epoch_frame::factory::array::make_array(urls),
        epoch_frame::factory::array::make_array(descriptions)};

    co_return epoch_frame::make_dataframe(index, arrays, columns);
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

} // namespace data_sdk::polygon
