#include "epoch_data_sdk/polygon/quotes_client.hpp"

#include <glaze/glaze.hpp>

#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "epoch_data_sdk/polygon/base_client.hpp"

namespace data_sdk::polygon {

namespace {

// V3 Quotes JSON structures
struct V3QuoteRow {
  std::optional<int> ask_exchange;
  std::optional<double> ask_price;
  std::optional<std::int64_t> ask_size;
  std::optional<int> bid_exchange;
  std::optional<double> bid_price;
  std::optional<std::int64_t> bid_size;
  std::optional<std::vector<int>> conditions;
  std::optional<std::int64_t> participant_timestamp;
  std::optional<std::int64_t> sequence_number;
  std::optional<std::int64_t> sip_timestamp;
  std::optional<int> tape;
};

struct V3QuotesResp {
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<V3QuoteRow> results;
  std::string status;
};

} // namespace

// Private implementation
class QuotesClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getV3Quotes(const std::string &ticker, std::optional<int> limit,
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

    const std::string path = "/v3/quotes/" + ticker;
    auto bodyRes = httpGet(path, q);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    V3QuotesResp parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse quotes JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::int64_t> t;        // participant_timestamp
    std::vector<double> ap, bp;         // ask_price, bid_price
    std::vector<std::int64_t> asz, bsz; // sizes
    std::vector<int> ax, bx;            // exchanges
    std::vector<std::int64_t> seq, sip;
    std::vector<int> tape;

    bool has_ap = false, has_bp = false, has_asz = false, has_bsz = false,
         has_ax = false, has_bx = false, has_seq = false, has_sip = false,
         has_tape = false;

    t.reserve(N);
    ap.reserve(N);
    bp.reserve(N);
    asz.reserve(N);
    bsz.reserve(N);
    ax.reserve(N);
    bx.reserve(N);
    seq.reserve(N);
    sip.reserve(N);
    tape.reserve(N);

    for (const auto &r : parsed.results) {
      const auto tt = r.participant_timestamp.value_or(0);
      t.push_back(tt);

      const auto ap_v = r.ask_price.value_or(0.0);
      ap.push_back(ap_v);
      has_ap |= r.ask_price.has_value();
      const auto bp_v = r.bid_price.value_or(0.0);
      bp.push_back(bp_v);
      has_bp |= r.bid_price.has_value();
      const auto as_v = r.ask_size.value_or(0);
      asz.push_back(as_v);
      has_asz |= r.ask_size.has_value();
      const auto bs_v = r.bid_size.value_or(0);
      bsz.push_back(bs_v);
      has_bsz |= r.bid_size.has_value();
      const auto ax_v = r.ask_exchange.value_or(0);
      ax.push_back(ax_v);
      has_ax |= r.ask_exchange.has_value();
      const auto bx_v = r.bid_exchange.value_or(0);
      bx.push_back(bx_v);
      has_bx |= r.bid_exchange.has_value();
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
    if (has_ap) {
      columns.push_back("ap");
      arrays.push_back(epoch_frame::factory::array::make_array(ap));
    }
    if (has_bp) {
      columns.push_back("bp");
      arrays.push_back(epoch_frame::factory::array::make_array(bp));
    }
    if (has_asz) {
      columns.push_back("asz");
      arrays.push_back(epoch_frame::factory::array::make_array(asz));
    }
    if (has_bsz) {
      columns.push_back("bsz");
      arrays.push_back(epoch_frame::factory::array::make_array(bsz));
    }
    if (has_ax) {
      columns.push_back("ax");
      arrays.push_back(epoch_frame::factory::array::make_array(ax));
    }
    if (has_bx) {
      columns.push_back("bx");
      arrays.push_back(epoch_frame::factory::array::make_array(bx));
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

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getV3QuotesAsync(std::string ticker, std::optional<int> limit,
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

    const std::string path = "/v3/quotes/" + ticker;
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    V3QuotesResp parsed{};
    if (auto ec = glz::read_json(parsed, std::string_view(*bodyRes)); ec) {
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse quotes JSON response", nullptr);
    }

    const auto N = parsed.results.size();
    std::vector<std::int64_t> t;        // participant_timestamp
    std::vector<double> ap, bp;         // ask_price, bid_price
    std::vector<std::int64_t> asz, bsz; // sizes
    std::vector<int> ax, bx;            // exchanges
    std::vector<std::int64_t> seq, sip;
    std::vector<int> tape;

    bool has_ap = false, has_bp = false, has_asz = false, has_bsz = false,
         has_ax = false, has_bx = false, has_seq = false, has_sip = false,
         has_tape = false;

    t.reserve(N);
    ap.reserve(N);
    bp.reserve(N);
    asz.reserve(N);
    bsz.reserve(N);
    ax.reserve(N);
    bx.reserve(N);
    seq.reserve(N);
    sip.reserve(N);
    tape.reserve(N);

    for (const auto &r : parsed.results) {
      const auto tt = r.participant_timestamp.value_or(0);
      t.push_back(tt);

      const auto ap_v = r.ask_price.value_or(0.0);
      ap.push_back(ap_v);
      has_ap |= r.ask_price.has_value();
      const auto bp_v = r.bid_price.value_or(0.0);
      bp.push_back(bp_v);
      has_bp |= r.bid_price.has_value();
      const auto as_v = r.ask_size.value_or(0);
      asz.push_back(as_v);
      has_asz |= r.ask_size.has_value();
      const auto bs_v = r.bid_size.value_or(0);
      bsz.push_back(bs_v);
      has_bsz |= r.bid_size.has_value();
      const auto ax_v = r.ask_exchange.value_or(0);
      ax.push_back(ax_v);
      has_ax |= r.ask_exchange.has_value();
      const auto bx_v = r.bid_exchange.value_or(0);
      bx.push_back(bx_v);
      has_bx |= r.bid_exchange.has_value();
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
    if (has_ap) {
      columns.push_back("ap");
      arrays.push_back(epoch_frame::factory::array::make_array(ap));
    }
    if (has_bp) {
      columns.push_back("bp");
      arrays.push_back(epoch_frame::factory::array::make_array(bp));
    }
    if (has_asz) {
      columns.push_back("asz");
      arrays.push_back(epoch_frame::factory::array::make_array(asz));
    }
    if (has_bsz) {
      columns.push_back("bsz");
      arrays.push_back(epoch_frame::factory::array::make_array(bsz));
    }
    if (has_ax) {
      columns.push_back("ax");
      arrays.push_back(epoch_frame::factory::array::make_array(ax));
    }
    if (has_bx) {
      columns.push_back("bx");
      arrays.push_back(epoch_frame::factory::array::make_array(bx));
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

    co_return epoch_frame::make_dataframe(index, arrays, columns);
  }
};

// Public API
QuotesClient::QuotesClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

QuotesClient::~QuotesClient() = default;

Expected<epoch_frame::DataFrame> QuotesClient::getQuotes(
    const std::string &ticker, const std::string &from_date,
    const std::string &to_date, std::optional<int> limit) const {
  return impl_->getV3Quotes(ticker, limit, from_date, std::nullopt,
                            to_date, std::nullopt);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
QuotesClient::getQuotesAsync(std::string ticker, std::string from_date,
                             std::string to_date, std::optional<int> limit) const {
  return impl_->getV3QuotesAsync(std::move(ticker), limit, std::move(from_date),
                                 std::nullopt, std::move(to_date), std::nullopt);
}

} // namespace data_sdk::polygon
