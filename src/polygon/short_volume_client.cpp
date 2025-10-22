#include "epoch_data_sdk/polygon/short_volume_client.hpp"

#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "epoch_data_sdk/polygon/base_client.hpp"
#include "epoch_data_sdk/polygon/models.hpp"

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
class ShortVolumeClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getShortVolume(const std::string &ticker, const std::string &date_from,
                 const std::string &date_to, std::optional<int> limit) const {

    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("ticker", ticker);
    q.emplace_back("date.gte", date_from);
    q.emplace_back("date.lte", date_to);
    // Always fetch in ascending order for backtesting consistency
    q.emplace_back("order", "asc");
    q.emplace_back("sort", "date");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/v1/short-volume";
    auto bodyRes = httpGetWithRetry(path, q, 3);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    ShortVolumeResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getShortVolume parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse JSON response", nullptr);
    }

    // Build DataFrame
    std::vector<std::int64_t> dates;
    std::vector<std::string> tickers_col;
    std::vector<int> short_volume, total_volume, exempt_volume, non_exempt_volume;
    std::vector<double> short_volume_ratio;

    const auto sz = parsed.results.size();
    dates.reserve(sz);
    tickers_col.reserve(sz);
    short_volume.reserve(sz);
    total_volume.reserve(sz);
    exempt_volume.reserve(sz);
    non_exempt_volume.reserve(sz);
    short_volume_ratio.reserve(sz);

    for (const auto &r : parsed.results) {
      const auto date_ns = parseDateToNs(r.date.value_or(""));
      dates.push_back(date_ns);
      tickers_col.push_back(r.ticker.value_or(""));
      short_volume.push_back(r.short_volume.value_or(0));
      total_volume.push_back(r.total_volume.value_or(0));
      exempt_volume.push_back(r.exempt_volume.value_or(0));
      non_exempt_volume.push_back(r.non_exempt_volume.value_or(0));
      short_volume_ratio.push_back(r.short_volume_ratio.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon short_volume pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = httpGetWithRetry(parsed_url->path, parsed_url->query, 3);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon short_volume pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      ShortVolumeResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon short_volume page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        const auto date_ns = parseDateToNs(r.date.value_or(""));
        dates.push_back(date_ns);
        tickers_col.push_back(r.ticker.value_or(""));
        short_volume.push_back(r.short_volume.value_or(0));
        total_volume.push_back(r.total_volume.value_or(0));
        exempt_volume.push_back(r.exempt_volume.value_or(0));
        non_exempt_volume.push_back(r.non_exempt_volume.value_or(0));
        short_volume_ratio.push_back(r.short_volume_ratio.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon short_volume: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, dates.size());
    }

    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "short_volume", "total_volume",
                                         "short_volume_ratio", "exempt_volume",
                                         "non_exempt_volume"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(short_volume),
        epoch_frame::factory::array::make_array(total_volume),
        epoch_frame::factory::array::make_array(short_volume_ratio),
        epoch_frame::factory::array::make_array(exempt_volume),
        epoch_frame::factory::array::make_array(non_exempt_volume)};

    return epoch_frame::make_dataframe(index, data, columns);
  }
};

// Public API
ShortVolumeClient::ShortVolumeClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

ShortVolumeClient::~ShortVolumeClient() = default;

Expected<epoch_frame::DataFrame>
ShortVolumeClient::getShortVolume(const std::string &ticker,
                                   const std::string &date_from,
                                   const std::string &date_to,
                                   std::optional<int> limit) const {
  return impl_->getShortVolume(ticker, date_from, date_to, limit);
}

} // namespace data_sdk::polygon
