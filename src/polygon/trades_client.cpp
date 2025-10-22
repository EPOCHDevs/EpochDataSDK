#include "epoch_data_sdk/polygon/trades_client.hpp"

#include <glaze/glaze.hpp>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "epoch_data_sdk/polygon/base_client.hpp"

namespace data_sdk::polygon {

namespace {

// V3 Trades JSON structures
struct V3TradeRow {
  std::optional<std::vector<int>> conditions;
  std::optional<int> exchange;
  std::optional<std::string> id;
  std::optional<std::int64_t> participant_timestamp;
  std::optional<double> price;
  std::optional<std::int64_t> sequence_number;
  std::optional<std::int64_t> sip_timestamp;
  std::optional<double> size; // crypto fractional sizes
  std::optional<int> tape;
};

struct V3TradesResp {
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<V3TradeRow> results;
  std::string status;
};

} // namespace

// Private implementation
class TradesClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getV3Trades(const std::string &ticker, std::optional<int> limit,
              std::optional<std::string> timestamp_gte,
              std::optional<std::string> timestamp_gt,
              std::optional<std::string> timestamp_lte,
              std::optional<std::string> timestamp_lt) const {

    std::vector<std::pair<std::string, std::string>> q;
    // Always fetch in ascending chronological order for backtesting consistency
    q.emplace_back("order", "asc");
    q.emplace_back("sort", "timestamp");

    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));
    if (timestamp_gte.has_value())
      q.emplace_back("timestamp.gte", *timestamp_gte);
    if (timestamp_gt.has_value())
      q.emplace_back("timestamp.gt", *timestamp_gt);
    if (timestamp_lte.has_value())
      q.emplace_back("timestamp.lte", *timestamp_lte);
    if (timestamp_lt.has_value())
      q.emplace_back("timestamp.lt", *timestamp_lt);

    const std::string path = "/v3/trades/" + ticker;
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    V3TradesResp parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse trades JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::int64_t> t; // participant_timestamp
    std::vector<double> p, s;    // price, size
    std::vector<int> x;          // exchange
    std::vector<std::int64_t> seq, sip;
    std::vector<int> tape;

    bool has_p = false, has_s = false, has_x = false, has_seq = false,
         has_sip = false, has_tape = false;

    t.reserve(N);
    p.reserve(N);
    s.reserve(N);
    x.reserve(N);
    seq.reserve(N);
    sip.reserve(N);
    tape.reserve(N);

    for (const auto &r : parsed.results) {
      const auto tt = r.participant_timestamp.value_or(0);
      t.push_back(tt);
      const auto pv = r.price.value_or(0.0);
      p.push_back(pv);
      has_p |= r.price.has_value();
      const auto sv = r.size.value_or(0.0);
      s.push_back(sv);
      has_s |= r.size.has_value();
      const auto xv = r.exchange.value_or(0);
      x.push_back(xv);
      has_x |= r.exchange.has_value();
      const auto sq_v = r.sequence_number.value_or(0);
      seq.push_back(sq_v);
      has_seq |= r.sequence_number.has_value();
      const auto sp_v = r.sip_timestamp.value_or(0);
      sip.push_back(sp_v);
      has_sip |= r.sip_timestamp.has_value();
      const auto tp_v = r.tape.value_or(0);
      tape.push_back(tp_v);
      has_tape |= r.tape.has_value();
    }

    auto index = epoch_frame::factory::index::make_datetime_index(t, "", "UTC");
    std::vector<std::string> columns;
    std::vector<arrow::ChunkedArrayPtr> arrays;
    if (has_p) {
      columns.push_back("p");
      arrays.push_back(epoch_frame::factory::array::make_array(p));
    }
    if (has_s) {
      columns.push_back("s");
      arrays.push_back(epoch_frame::factory::array::make_array(s));
    }
    if (has_x) {
      columns.push_back("x");
      arrays.push_back(epoch_frame::factory::array::make_array(x));
    }
    if (has_seq) {
      columns.push_back("seq");
      arrays.push_back(epoch_frame::factory::array::make_array(seq));
    }
    if (has_sip) {
      columns.push_back("sip");
      arrays.push_back(epoch_frame::factory::array::make_array(sip));
    }
    if (has_tape) {
      columns.push_back("tape");
      arrays.push_back(epoch_frame::factory::array::make_array(tape));
    }

    return epoch_frame::make_dataframe(index, arrays, columns);
  }
};

// Public API
TradesClient::TradesClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

TradesClient::~TradesClient() = default;

Expected<epoch_frame::DataFrame> TradesClient::getTrades(
    const std::string &ticker, const std::string &from_date,
    const std::string &to_date, std::optional<int> limit) const {
  return impl_->getV3Trades(ticker, limit, from_date, std::nullopt,
                            to_date, std::nullopt);
}

} // namespace data_sdk::polygon
