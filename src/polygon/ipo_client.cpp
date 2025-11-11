#include "polygon/ipo_client.hpp"

#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "polygon/base_client.hpp"
#include "polygon/models.hpp"

namespace data_sdk::polygon {

namespace {
// Helper to parse date string (YYYY-MM-DD) to nanoseconds since epoch
std::int64_t parseDateToNs(const std::string &date_str) {
  if (date_str.size() < 10) return 0;

  int y_val = std::atoi(date_str.substr(0, 4).c_str());
  int m_val = std::atoi(date_str.substr(5, 2).c_str());
  int d_val = std::atoi(date_str.substr(8, 2).c_str());

  using namespace std::chrono;
  auto ymd = year_month_day{year{y_val}, month{static_cast<unsigned>(m_val)}, day{static_cast<unsigned>(d_val)}};
  auto dp = sys_days{ymd};
  return duration_cast<nanoseconds>(dp.time_since_epoch()).count();
}
} // namespace

// Private implementation
class IPOClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getIPOs(const std::string &from_date, const std::string &to_date,
          std::optional<std::string> ticker, std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    if (ticker.has_value() && !ticker->empty())
      q.emplace_back("ticker", *ticker);
    q.emplace_back("listing_date.gte", from_date);
    q.emplace_back("listing_date.lte", to_date);
    // Always fetch in ascending order for backtesting consistency
    q.emplace_back("order", "asc");
    q.emplace_back("sort", "listing_date");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/vX/reference/ipos";
    auto bodyRes = httpGetWithRetry(path, q, 3);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    IPOListingResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getIPOs parse failed: path={} "
                   "ec={} body_prefix={}",
                   path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse JSON response", nullptr);
    }

    // Build DataFrame
    std::vector<std::int64_t> dates;
    std::vector<std::string> tickers_col, issuer_names, listing_dates, announced_dates,
                              ipo_statuses, exchanges, us_codes, isins;
    std::vector<double> final_prices, highest_prices, lowest_prices, total_offer_sizes;
    std::vector<int> shares_outstanding, min_shares, max_shares;

    const auto sz = parsed.results.size();
    dates.reserve(sz);
    tickers_col.reserve(sz);
    issuer_names.reserve(sz);
    listing_dates.reserve(sz);
    announced_dates.reserve(sz);
    ipo_statuses.reserve(sz);
    exchanges.reserve(sz);
    us_codes.reserve(sz);
    isins.reserve(sz);
    final_prices.reserve(sz);
    highest_prices.reserve(sz);
    lowest_prices.reserve(sz);
    total_offer_sizes.reserve(sz);
    shares_outstanding.reserve(sz);
    min_shares.reserve(sz);
    max_shares.reserve(sz);

    for (const auto &r : parsed.results) {
      const auto date_ns = parseDateToNs(r.listing_date.value_or(""));
      dates.push_back(date_ns);
      tickers_col.push_back(r.ticker.value_or(""));
      issuer_names.push_back(r.issuer_name.value_or(""));
      listing_dates.push_back(r.listing_date.value_or(""));
      announced_dates.push_back(r.announced_date.value_or(""));
      ipo_statuses.push_back(r.ipo_status.value_or(""));
      exchanges.push_back(r.primary_exchange.value_or(""));
      us_codes.push_back(r.us_code.value_or(""));
      isins.push_back(r.isin.value_or(""));
      final_prices.push_back(r.final_issue_price.value_or(0.0));
      highest_prices.push_back(r.highest_offer_price.value_or(0.0));
      lowest_prices.push_back(r.lowest_offer_price.value_or(0.0));
      total_offer_sizes.push_back(r.total_offer_size.value_or(0.0));
      shares_outstanding.push_back(r.shares_outstanding.value_or(0));
      min_shares.push_back(r.min_shares_offered.value_or(0));
      max_shares.push_back(r.max_shares_offered.value_or(0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon IPO pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = httpGetWithRetry(parsed_url->path, parsed_url->query, 3);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon IPO pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      IPOListingResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon IPO page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        const auto date_ns = parseDateToNs(r.listing_date.value_or(""));
        dates.push_back(date_ns);
        tickers_col.push_back(r.ticker.value_or(""));
        issuer_names.push_back(r.issuer_name.value_or(""));
        listing_dates.push_back(r.listing_date.value_or(""));
        announced_dates.push_back(r.announced_date.value_or(""));
        ipo_statuses.push_back(r.ipo_status.value_or(""));
        exchanges.push_back(r.primary_exchange.value_or(""));
        us_codes.push_back(r.us_code.value_or(""));
        isins.push_back(r.isin.value_or(""));
        final_prices.push_back(r.final_issue_price.value_or(0.0));
        highest_prices.push_back(r.highest_offer_price.value_or(0.0));
        lowest_prices.push_back(r.lowest_offer_price.value_or(0.0));
        total_offer_sizes.push_back(r.total_offer_size.value_or(0.0));
        shares_outstanding.push_back(r.shares_outstanding.value_or(0));
        min_shares.push_back(r.min_shares_offered.value_or(0));
        max_shares.push_back(r.max_shares_offered.value_or(0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon IPO: fetched {} pages, total_rows={}",
                  page_count, dates.size());
    }

    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {
        "ticker", "issuer_name", "listing_date", "announced_date", "ipo_status",
        "exchange", "us_code", "isin", "final_price", "highest_price",
        "lowest_price", "total_offer_size", "shares_outstanding",
        "min_shares_offered", "max_shares_offered"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(issuer_names),
        epoch_frame::factory::array::make_array(listing_dates),
        epoch_frame::factory::array::make_array(announced_dates),
        epoch_frame::factory::array::make_array(ipo_statuses),
        epoch_frame::factory::array::make_array(exchanges),
        epoch_frame::factory::array::make_array(us_codes),
        epoch_frame::factory::array::make_array(isins),
        epoch_frame::factory::array::make_array(final_prices),
        epoch_frame::factory::array::make_array(highest_prices),
        epoch_frame::factory::array::make_array(lowest_prices),
        epoch_frame::factory::array::make_array(total_offer_sizes),
        epoch_frame::factory::array::make_array(shares_outstanding),
        epoch_frame::factory::array::make_array(min_shares),
        epoch_frame::factory::array::make_array(max_shares)};

    return epoch_frame::make_dataframe(index, data, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getIPOsAsync(std::string from_date, std::string to_date,
               std::optional<std::string> ticker, std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    if (ticker.has_value() && !ticker->empty())
      q.emplace_back("ticker", *ticker);
    q.emplace_back("listing_date.gte", from_date);
    q.emplace_back("listing_date.lte", to_date);
    // Always fetch in ascending order for backtesting consistency
    q.emplace_back("order", "asc");
    q.emplace_back("sort", "listing_date");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/vX/reference/ipos";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    IPOListingResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getIPOsAsync parse failed: path={} "
                   "ec={} body_prefix={}",
                   path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse JSON response", nullptr);
    }

    // Build DataFrame
    std::vector<std::int64_t> dates;
    std::vector<std::string> tickers_col, issuer_names, listing_dates, announced_dates,
                              ipo_statuses, exchanges, us_codes, isins;
    std::vector<double> final_prices, highest_prices, lowest_prices, total_offer_sizes;
    std::vector<int> shares_outstanding, min_shares, max_shares;

    const auto sz = parsed.results.size();
    dates.reserve(sz);
    tickers_col.reserve(sz);
    issuer_names.reserve(sz);
    listing_dates.reserve(sz);
    announced_dates.reserve(sz);
    ipo_statuses.reserve(sz);
    exchanges.reserve(sz);
    us_codes.reserve(sz);
    isins.reserve(sz);
    final_prices.reserve(sz);
    highest_prices.reserve(sz);
    lowest_prices.reserve(sz);
    total_offer_sizes.reserve(sz);
    shares_outstanding.reserve(sz);
    min_shares.reserve(sz);
    max_shares.reserve(sz);

    for (const auto &r : parsed.results) {
      const auto date_ns = parseDateToNs(r.listing_date.value_or(""));
      dates.push_back(date_ns);
      tickers_col.push_back(r.ticker.value_or(""));
      issuer_names.push_back(r.issuer_name.value_or(""));
      listing_dates.push_back(r.listing_date.value_or(""));
      announced_dates.push_back(r.announced_date.value_or(""));
      ipo_statuses.push_back(r.ipo_status.value_or(""));
      exchanges.push_back(r.primary_exchange.value_or(""));
      us_codes.push_back(r.us_code.value_or(""));
      isins.push_back(r.isin.value_or(""));
      final_prices.push_back(r.final_issue_price.value_or(0.0));
      highest_prices.push_back(r.highest_offer_price.value_or(0.0));
      lowest_prices.push_back(r.lowest_offer_price.value_or(0.0));
      total_offer_sizes.push_back(r.total_offer_size.value_or(0.0));
      shares_outstanding.push_back(r.shares_outstanding.value_or(0));
      min_shares.push_back(r.min_shares_offered.value_or(0));
      max_shares.push_back(r.max_shares_offered.value_or(0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon IPO Async pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = co_await httpAsyncGet(parsed_url->path, parsed_url->query);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon IPO Async pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      IPOListingResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon IPO Async page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        const auto date_ns = parseDateToNs(r.listing_date.value_or(""));
        dates.push_back(date_ns);
        tickers_col.push_back(r.ticker.value_or(""));
        issuer_names.push_back(r.issuer_name.value_or(""));
        listing_dates.push_back(r.listing_date.value_or(""));
        announced_dates.push_back(r.announced_date.value_or(""));
        ipo_statuses.push_back(r.ipo_status.value_or(""));
        exchanges.push_back(r.primary_exchange.value_or(""));
        us_codes.push_back(r.us_code.value_or(""));
        isins.push_back(r.isin.value_or(""));
        final_prices.push_back(r.final_issue_price.value_or(0.0));
        highest_prices.push_back(r.highest_offer_price.value_or(0.0));
        lowest_prices.push_back(r.lowest_offer_price.value_or(0.0));
        total_offer_sizes.push_back(r.total_offer_size.value_or(0.0));
        shares_outstanding.push_back(r.shares_outstanding.value_or(0));
        min_shares.push_back(r.min_shares_offered.value_or(0));
        max_shares.push_back(r.max_shares_offered.value_or(0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon IPO Async: fetched {} pages, total_rows={}",
                  page_count, dates.size());
    }

    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {
        "ticker", "issuer_name", "listing_date", "announced_date", "ipo_status",
        "exchange", "us_code", "isin", "final_price", "highest_price",
        "lowest_price", "total_offer_size", "shares_outstanding",
        "min_shares_offered", "max_shares_offered"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(issuer_names),
        epoch_frame::factory::array::make_array(listing_dates),
        epoch_frame::factory::array::make_array(announced_dates),
        epoch_frame::factory::array::make_array(ipo_statuses),
        epoch_frame::factory::array::make_array(exchanges),
        epoch_frame::factory::array::make_array(us_codes),
        epoch_frame::factory::array::make_array(isins),
        epoch_frame::factory::array::make_array(final_prices),
        epoch_frame::factory::array::make_array(highest_prices),
        epoch_frame::factory::array::make_array(lowest_prices),
        epoch_frame::factory::array::make_array(total_offer_sizes),
        epoch_frame::factory::array::make_array(shares_outstanding),
        epoch_frame::factory::array::make_array(min_shares),
        epoch_frame::factory::array::make_array(max_shares)};

    co_return epoch_frame::make_dataframe(index, data, columns);
  }
};

// Public API
IPOClient::IPOClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

IPOClient::~IPOClient() = default;

Expected<epoch_frame::DataFrame>
IPOClient::getIPOs(const std::string &from_date,
                   const std::string &to_date,
                   std::optional<std::string> ticker,
                   std::optional<int> limit) const {
  return impl_->getIPOs(from_date, to_date, ticker, limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
IPOClient::getIPOsAsync(std::string from_date, std::string to_date,
                        std::optional<std::string> ticker,
                        std::optional<int> limit) const {
  return impl_->getIPOsAsync(std::move(from_date), std::move(to_date), ticker, limit);
}

} // namespace data_sdk::polygon
