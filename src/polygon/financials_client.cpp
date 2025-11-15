#include "polygon/financials_client.hpp"

#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>

#include <arrow/compute/api.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

#include "polygon/base_client.hpp"
#include "polygon/models.hpp"

namespace data_sdk::polygon {

namespace {
// Helper to parse date strings (YYYY-MM-DD) to nanoseconds since epoch using Arrow
std::vector<std::int64_t> parseDatesToNs(const std::vector<std::string> &date_strings) {
  if (date_strings.empty()) return {};

  // Build Arrow StringArray from input strings
  arrow::StringBuilder builder;
  auto status = builder.AppendValues(date_strings);
  if (!status.ok()) {
    SPDLOG_ERROR("Failed to build StringArray for date parsing: {}", status.message());
    return std::vector<std::int64_t>(date_strings.size(), 0);
  }

  auto maybe_array = builder.Finish();
  if (!maybe_array.ok()) {
    SPDLOG_ERROR("Failed to finish StringArray: {}", maybe_array.status().message());
    return std::vector<std::int64_t>(date_strings.size(), 0);
  }

  // Parse strings to timestamps using Arrow compute strptime
  arrow::compute::StrptimeOptions options("%Y-%m-%d", arrow::TimeUnit::NANO, false);
  auto maybe_result = arrow::compute::CallFunction("strptime", {maybe_array.ValueOrDie()}, &options);
  if (!maybe_result.ok()) {
    SPDLOG_ERROR("Failed to parse dates with strptime: {}", maybe_result.status().message());
    return std::vector<std::int64_t>(date_strings.size(), 0);
  }

  // Extract nanosecond values from TimestampArray
  auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(maybe_result.ValueOrDie().make_array());
  std::vector<std::int64_t> result;
  result.reserve(timestamp_array->length());
  for (int64_t i = 0; i < timestamp_array->length(); ++i) {
    if (timestamp_array->IsNull(i)) {
      result.push_back(0);
    } else {
      result.push_back(timestamp_array->Value(i));
    }
  }

  return result;
}
} // namespace

// Private implementation
class FinancialsClient::Impl : public BaseClient {
public:
  explicit Impl(Options options) : BaseClient(std::move(options)) {}

  Expected<epoch_frame::DataFrame>
  getBalanceSheets(const std::string &ticker, const std::string &from_date,
                   const std::string &to_date, std::optional<int> limit) const {
    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("tickers", ticker);
    q.emplace_back("period_end.gte", from_date);
    q.emplace_back("period_end.lte", to_date);
    // Always fetch in ascending order for backtesting consistency
    q.emplace_back("sort", "period_end.asc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/financials/v1/balance-sheets";
    auto bodyRes = httpGetWithRetry(path, q, 3);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    BalanceSheetsResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getBalanceSheets parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse balance sheets JSON response", nullptr);
    }

    std::vector<std::string> date_strings;
    std::vector<std::string> tickers_col, filing_dates, period_ends, timeframes;
    std::vector<std::int64_t> fiscal_years, fiscal_quarters;
    std::vector<double> accounts_payable, accrued_liab, aoci, cash, debt_current,
        deferred_rev, inventories, lt_debt, ppe_net, receivables, retained_earn;

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.filing_date.value_or(""));
      filing_dates.push_back(r.filing_date.value_or(""));
      period_ends.push_back(r.period_end.value_or(""));
      tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
      fiscal_years.push_back(r.fiscal_year.value_or(0));
      fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
      timeframes.push_back(r.timeframe.value_or(""));
      accounts_payable.push_back(r.accounts_payable.value_or(0.0));
      accrued_liab.push_back(r.accrued_and_other_current_liabilities.value_or(0.0));
      aoci.push_back(r.accumulated_other_comprehensive_income.value_or(0.0));
      cash.push_back(r.cash_and_equivalents.value_or(0.0));
      debt_current.push_back(r.debt_current.value_or(0.0));
      deferred_rev.push_back(r.deferred_revenue_current.value_or(0.0));
      inventories.push_back(r.inventories.value_or(0.0));
      lt_debt.push_back(r.long_term_debt_and_capital_lease_obligations.value_or(0.0));
      ppe_net.push_back(r.property_plant_equipment_net.value_or(0.0));
      receivables.push_back(r.receivables.value_or(0.0));
      retained_earn.push_back(r.retained_earnings_deficit.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon getBalanceSheets pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = httpGetWithRetry(parsed_url->path, parsed_url->query, 3);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon getBalanceSheets pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      BalanceSheetsResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon getBalanceSheets page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.filing_date.value_or(""));
        filing_dates.push_back(r.filing_date.value_or(""));
        period_ends.push_back(r.period_end.value_or(""));
        tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
        fiscal_years.push_back(r.fiscal_year.value_or(0));
        fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
        timeframes.push_back(r.timeframe.value_or(""));
        accounts_payable.push_back(r.accounts_payable.value_or(0.0));
        accrued_liab.push_back(r.accrued_and_other_current_liabilities.value_or(0.0));
        aoci.push_back(r.accumulated_other_comprehensive_income.value_or(0.0));
        cash.push_back(r.cash_and_equivalents.value_or(0.0));
        debt_current.push_back(r.debt_current.value_or(0.0));
        deferred_rev.push_back(r.deferred_revenue_current.value_or(0.0));
        inventories.push_back(r.inventories.value_or(0.0));
        lt_debt.push_back(r.long_term_debt_and_capital_lease_obligations.value_or(0.0));
        ppe_net.push_back(r.property_plant_equipment_net.value_or(0.0));
        receivables.push_back(r.receivables.value_or(0.0));
        retained_earn.push_back(r.retained_earnings_deficit.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon getBalanceSheets: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "filing_date", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
                                        "accounts_payable", "accrued_liabilities", "aoci", "cash",
                                        "debt_current", "deferred_revenue", "inventories", "lt_debt",
                                        "ppe_net", "receivables", "retained_earnings"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(filing_dates),
        epoch_frame::factory::array::make_array(period_ends),
        epoch_frame::factory::array::make_array(fiscal_years),
        epoch_frame::factory::array::make_array(fiscal_quarters),
        epoch_frame::factory::array::make_array(timeframes),
        epoch_frame::factory::array::make_array(accounts_payable),
        epoch_frame::factory::array::make_array(accrued_liab),
        epoch_frame::factory::array::make_array(aoci),
        epoch_frame::factory::array::make_array(cash),
        epoch_frame::factory::array::make_array(debt_current),
        epoch_frame::factory::array::make_array(deferred_rev),
        epoch_frame::factory::array::make_array(inventories),
        epoch_frame::factory::array::make_array(lt_debt),
        epoch_frame::factory::array::make_array(ppe_net),
        epoch_frame::factory::array::make_array(receivables),
        epoch_frame::factory::array::make_array(retained_earn)};

    return epoch_frame::make_dataframe(index, data, columns);
  }

  Expected<epoch_frame::DataFrame>
  getCashFlowStatements(const std::string &ticker, const std::string &from_date,
                        const std::string &to_date, std::optional<int> limit) const {
    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("tickers", ticker);
    q.emplace_back("period_end.gte", from_date);
    q.emplace_back("period_end.lte", to_date);
    q.emplace_back("sort", "period_end.asc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/financials/v1/cash-flow-statements";
    auto bodyRes = httpGetWithRetry(path, q, 3);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    CashFlowStatementsResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getCashFlowStatements parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse cash flow statements JSON response", nullptr);
    }

    std::vector<std::string> date_strings;
    std::vector<std::string> filing_dates, period_ends, tickers_col, timeframes;
    std::vector<std::int64_t> fiscal_years, fiscal_quarters;
    std::vector<double> cfo, change_cash, change_assets, dda, dividends,
        lt_debt_iss, ncf_fin, ncf_inv, ncf_oper, net_income, capex;

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.filing_date.value_or(""));
      filing_dates.push_back(r.filing_date.value_or(""));
      period_ends.push_back(r.period_end.value_or(""));
      tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
      fiscal_years.push_back(r.fiscal_year.value_or(0));
      fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
      timeframes.push_back(r.timeframe.value_or(""));
      cfo.push_back(r.cash_from_operating_activities_continuing_operations.value_or(0.0));
      change_cash.push_back(r.change_in_cash_and_equivalents.value_or(0.0));
      change_assets.push_back(r.change_in_other_operating_assets_and_liabilities_net.value_or(0.0));
      dda.push_back(r.depreciation_depletion_and_amortization.value_or(0.0));
      dividends.push_back(r.dividends.value_or(0.0));
      lt_debt_iss.push_back(r.long_term_debt_issuances_repayments.value_or(0.0));
      ncf_fin.push_back(r.net_cash_from_financing_activities.value_or(0.0));
      ncf_inv.push_back(r.net_cash_from_investing_activities.value_or(0.0));
      ncf_oper.push_back(r.net_cash_from_operating_activities.value_or(0.0));
      net_income.push_back(r.net_income.value_or(0.0));
      capex.push_back(r.purchase_of_property_plant_and_equipment.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon getCashFlowStatements pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = httpGetWithRetry(parsed_url->path, parsed_url->query, 3);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon getCashFlowStatements pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      CashFlowStatementsResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon getCashFlowStatements page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.filing_date.value_or(""));
        filing_dates.push_back(r.filing_date.value_or(""));
        period_ends.push_back(r.period_end.value_or(""));
        tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
        fiscal_years.push_back(r.fiscal_year.value_or(0));
        fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
        timeframes.push_back(r.timeframe.value_or(""));
        cfo.push_back(r.cash_from_operating_activities_continuing_operations.value_or(0.0));
        change_cash.push_back(r.change_in_cash_and_equivalents.value_or(0.0));
        change_assets.push_back(r.change_in_other_operating_assets_and_liabilities_net.value_or(0.0));
        dda.push_back(r.depreciation_depletion_and_amortization.value_or(0.0));
        dividends.push_back(r.dividends.value_or(0.0));
        lt_debt_iss.push_back(r.long_term_debt_issuances_repayments.value_or(0.0));
        ncf_fin.push_back(r.net_cash_from_financing_activities.value_or(0.0));
        ncf_inv.push_back(r.net_cash_from_investing_activities.value_or(0.0));
        ncf_oper.push_back(r.net_cash_from_operating_activities.value_or(0.0));
        net_income.push_back(r.net_income.value_or(0.0));
        capex.push_back(r.purchase_of_property_plant_and_equipment.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon getCashFlowStatements: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "filing_date", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
                                        "cfo", "change_cash", "change_assets", "dda", "dividends",
                                        "lt_debt_issuances", "ncf_financing", "ncf_investing",
                                        "ncf_operating", "net_income", "capex"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(filing_dates),
        epoch_frame::factory::array::make_array(period_ends),
        epoch_frame::factory::array::make_array(fiscal_years),
        epoch_frame::factory::array::make_array(fiscal_quarters),
        epoch_frame::factory::array::make_array(timeframes),
        epoch_frame::factory::array::make_array(cfo),
        epoch_frame::factory::array::make_array(change_cash),
        epoch_frame::factory::array::make_array(change_assets),
        epoch_frame::factory::array::make_array(dda),
        epoch_frame::factory::array::make_array(dividends),
        epoch_frame::factory::array::make_array(lt_debt_iss),
        epoch_frame::factory::array::make_array(ncf_fin),
        epoch_frame::factory::array::make_array(ncf_inv),
        epoch_frame::factory::array::make_array(ncf_oper),
        epoch_frame::factory::array::make_array(net_income),
        epoch_frame::factory::array::make_array(capex)};

    return epoch_frame::make_dataframe(index, data, columns);
  }

  Expected<epoch_frame::DataFrame>
  getIncomeStatements(const std::string &ticker, const std::string &from_date,
                      const std::string &to_date, std::optional<int> limit) const {
    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("tickers", ticker);
    q.emplace_back("period_end.gte", from_date);
    q.emplace_back("period_end.lte", to_date);
    q.emplace_back("sort", "period_end.asc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/financials/v1/income-statements";
    auto bodyRes = httpGetWithRetry(path, q, 3);
    if (!bodyRes)
      return std::unexpected(bodyRes.error());

    IncomeStatementsResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getIncomeStatements parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse income statements JSON response", nullptr);
    }

    std::vector<std::string> date_strings;
    std::vector<std::string> filing_dates, period_ends, tickers_col, timeframes;
    std::vector<std::int64_t> fiscal_years, fiscal_quarters;
    std::vector<double> basic_eps, diluted_eps, revenue, cogs, gross_profit,
        operating_income, net_income, rd, sga;

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.filing_date.value_or(""));
      filing_dates.push_back(r.filing_date.value_or(""));
      period_ends.push_back(r.period_end.value_or(""));
      tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
      fiscal_years.push_back(r.fiscal_year.value_or(0));
      fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
      timeframes.push_back(r.timeframe.value_or(""));
      basic_eps.push_back(r.basic_earnings_per_share.value_or(0.0));
      diluted_eps.push_back(r.diluted_earnings_per_share.value_or(0.0));
      revenue.push_back(r.revenue.value_or(0.0));
      cogs.push_back(r.cost_of_revenue.value_or(0.0));
      gross_profit.push_back(r.gross_profit.value_or(0.0));
      operating_income.push_back(r.operating_income.value_or(0.0));
      net_income.push_back(r.consolidated_net_income_loss.value_or(0.0));
      rd.push_back(r.research_development.value_or(0.0));
      sga.push_back(r.selling_general_administrative.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon getIncomeStatements pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = httpGetWithRetry(parsed_url->path, parsed_url->query, 3);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon getIncomeStatements pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      IncomeStatementsResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon getIncomeStatements page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.filing_date.value_or(""));
        filing_dates.push_back(r.filing_date.value_or(""));
        period_ends.push_back(r.period_end.value_or(""));
        tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
        fiscal_years.push_back(r.fiscal_year.value_or(0));
        fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
        timeframes.push_back(r.timeframe.value_or(""));
        basic_eps.push_back(r.basic_earnings_per_share.value_or(0.0));
        diluted_eps.push_back(r.diluted_earnings_per_share.value_or(0.0));
        revenue.push_back(r.revenue.value_or(0.0));
        cogs.push_back(r.cost_of_revenue.value_or(0.0));
        gross_profit.push_back(r.gross_profit.value_or(0.0));
        operating_income.push_back(r.operating_income.value_or(0.0));
        net_income.push_back(r.consolidated_net_income_loss.value_or(0.0));
        rd.push_back(r.research_development.value_or(0.0));
        sga.push_back(r.selling_general_administrative.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon getIncomeStatements: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "filing_date", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
                                        "basic_eps", "diluted_eps", "revenue", "cogs", "gross_profit",
                                        "operating_income", "net_income", "rd", "sga"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(filing_dates),
        epoch_frame::factory::array::make_array(period_ends),
        epoch_frame::factory::array::make_array(fiscal_years),
        epoch_frame::factory::array::make_array(fiscal_quarters),
        epoch_frame::factory::array::make_array(timeframes),
        epoch_frame::factory::array::make_array(basic_eps),
        epoch_frame::factory::array::make_array(diluted_eps),
        epoch_frame::factory::array::make_array(revenue),
        epoch_frame::factory::array::make_array(cogs),
        epoch_frame::factory::array::make_array(gross_profit),
        epoch_frame::factory::array::make_array(operating_income),
        epoch_frame::factory::array::make_array(net_income),
        epoch_frame::factory::array::make_array(rd),
        epoch_frame::factory::array::make_array(sga)};

    return epoch_frame::make_dataframe(index, data, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getBalanceSheetsAsync(std::string ticker, std::string from_date,
                        std::string to_date, std::optional<int> limit) const {
    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("tickers", ticker);
    q.emplace_back("period_end.gte", from_date);
    q.emplace_back("period_end.lte", to_date);
    // Always fetch in ascending order for backtesting consistency
    q.emplace_back("sort", "period_end.asc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/financials/v1/balance-sheets";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    BalanceSheetsResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getBalanceSheetsAsync parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse balance sheets JSON response", nullptr);
    }

    std::vector<std::string> date_strings;
    std::vector<std::string> tickers_col, filing_dates, period_ends, timeframes;
    std::vector<std::int64_t> fiscal_years, fiscal_quarters;
    std::vector<double> accounts_payable, accrued_liab, aoci, cash, debt_current,
        deferred_rev, inventories, lt_debt, ppe_net, receivables, retained_earn;

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.filing_date.value_or(""));
      filing_dates.push_back(r.filing_date.value_or(""));
      period_ends.push_back(r.period_end.value_or(""));
      tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
      fiscal_years.push_back(r.fiscal_year.value_or(0));
      fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
      timeframes.push_back(r.timeframe.value_or(""));
      accounts_payable.push_back(r.accounts_payable.value_or(0.0));
      accrued_liab.push_back(r.accrued_and_other_current_liabilities.value_or(0.0));
      aoci.push_back(r.accumulated_other_comprehensive_income.value_or(0.0));
      cash.push_back(r.cash_and_equivalents.value_or(0.0));
      debt_current.push_back(r.debt_current.value_or(0.0));
      deferred_rev.push_back(r.deferred_revenue_current.value_or(0.0));
      inventories.push_back(r.inventories.value_or(0.0));
      lt_debt.push_back(r.long_term_debt_and_capital_lease_obligations.value_or(0.0));
      ppe_net.push_back(r.property_plant_equipment_net.value_or(0.0));
      receivables.push_back(r.receivables.value_or(0.0));
      retained_earn.push_back(r.retained_earnings_deficit.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon getBalanceSheetsAsync pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = co_await httpAsyncGet(parsed_url->path, parsed_url->query);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon getBalanceSheetsAsync pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      BalanceSheetsResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon getBalanceSheetsAsync page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.filing_date.value_or(""));
        filing_dates.push_back(r.filing_date.value_or(""));
        period_ends.push_back(r.period_end.value_or(""));
        tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
        fiscal_years.push_back(r.fiscal_year.value_or(0));
        fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
        timeframes.push_back(r.timeframe.value_or(""));
        accounts_payable.push_back(r.accounts_payable.value_or(0.0));
        accrued_liab.push_back(r.accrued_and_other_current_liabilities.value_or(0.0));
        aoci.push_back(r.accumulated_other_comprehensive_income.value_or(0.0));
        cash.push_back(r.cash_and_equivalents.value_or(0.0));
        debt_current.push_back(r.debt_current.value_or(0.0));
        deferred_rev.push_back(r.deferred_revenue_current.value_or(0.0));
        inventories.push_back(r.inventories.value_or(0.0));
        lt_debt.push_back(r.long_term_debt_and_capital_lease_obligations.value_or(0.0));
        ppe_net.push_back(r.property_plant_equipment_net.value_or(0.0));
        receivables.push_back(r.receivables.value_or(0.0));
        retained_earn.push_back(r.retained_earnings_deficit.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon getBalanceSheetsAsync: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "filing_date", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
                                        "accounts_payable", "accrued_liabilities", "aoci", "cash",
                                        "debt_current", "deferred_revenue", "inventories", "lt_debt",
                                        "ppe_net", "receivables", "retained_earnings"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(filing_dates),
        epoch_frame::factory::array::make_array(period_ends),
        epoch_frame::factory::array::make_array(fiscal_years),
        epoch_frame::factory::array::make_array(fiscal_quarters),
        epoch_frame::factory::array::make_array(timeframes),
        epoch_frame::factory::array::make_array(accounts_payable),
        epoch_frame::factory::array::make_array(accrued_liab),
        epoch_frame::factory::array::make_array(aoci),
        epoch_frame::factory::array::make_array(cash),
        epoch_frame::factory::array::make_array(debt_current),
        epoch_frame::factory::array::make_array(deferred_rev),
        epoch_frame::factory::array::make_array(inventories),
        epoch_frame::factory::array::make_array(lt_debt),
        epoch_frame::factory::array::make_array(ppe_net),
        epoch_frame::factory::array::make_array(receivables),
        epoch_frame::factory::array::make_array(retained_earn)};

    co_return epoch_frame::make_dataframe(index, data, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getCashFlowStatementsAsync(std::string ticker, std::string from_date,
                             std::string to_date, std::optional<int> limit) const {
    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("tickers", ticker);
    q.emplace_back("period_end.gte", from_date);
    q.emplace_back("period_end.lte", to_date);
    q.emplace_back("sort", "period_end.asc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/financials/v1/cash-flow-statements";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    CashFlowStatementsResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getCashFlowStatementsAsync parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse cash flow statements JSON response", nullptr);
    }

    std::vector<std::string> date_strings;
    std::vector<std::string> filing_dates, period_ends, tickers_col, timeframes;
    std::vector<std::int64_t> fiscal_years, fiscal_quarters;
    std::vector<double> cfo, change_cash, change_assets, dda, dividends,
        lt_debt_iss, ncf_fin, ncf_inv, ncf_oper, net_income, capex;

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.filing_date.value_or(""));
      filing_dates.push_back(r.filing_date.value_or(""));
      period_ends.push_back(r.period_end.value_or(""));
      tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
      fiscal_years.push_back(r.fiscal_year.value_or(0));
      fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
      timeframes.push_back(r.timeframe.value_or(""));
      cfo.push_back(r.cash_from_operating_activities_continuing_operations.value_or(0.0));
      change_cash.push_back(r.change_in_cash_and_equivalents.value_or(0.0));
      change_assets.push_back(r.change_in_other_operating_assets_and_liabilities_net.value_or(0.0));
      dda.push_back(r.depreciation_depletion_and_amortization.value_or(0.0));
      dividends.push_back(r.dividends.value_or(0.0));
      lt_debt_iss.push_back(r.long_term_debt_issuances_repayments.value_or(0.0));
      ncf_fin.push_back(r.net_cash_from_financing_activities.value_or(0.0));
      ncf_inv.push_back(r.net_cash_from_investing_activities.value_or(0.0));
      ncf_oper.push_back(r.net_cash_from_operating_activities.value_or(0.0));
      net_income.push_back(r.net_income.value_or(0.0));
      capex.push_back(r.purchase_of_property_plant_and_equipment.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon getCashFlowStatementsAsync pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = co_await httpAsyncGet(parsed_url->path, parsed_url->query);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon getCashFlowStatementsAsync pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      CashFlowStatementsResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon getCashFlowStatementsAsync page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.filing_date.value_or(""));
        filing_dates.push_back(r.filing_date.value_or(""));
        period_ends.push_back(r.period_end.value_or(""));
        tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
        fiscal_years.push_back(r.fiscal_year.value_or(0));
        fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
        timeframes.push_back(r.timeframe.value_or(""));
        cfo.push_back(r.cash_from_operating_activities_continuing_operations.value_or(0.0));
        change_cash.push_back(r.change_in_cash_and_equivalents.value_or(0.0));
        change_assets.push_back(r.change_in_other_operating_assets_and_liabilities_net.value_or(0.0));
        dda.push_back(r.depreciation_depletion_and_amortization.value_or(0.0));
        dividends.push_back(r.dividends.value_or(0.0));
        lt_debt_iss.push_back(r.long_term_debt_issuances_repayments.value_or(0.0));
        ncf_fin.push_back(r.net_cash_from_financing_activities.value_or(0.0));
        ncf_inv.push_back(r.net_cash_from_investing_activities.value_or(0.0));
        ncf_oper.push_back(r.net_cash_from_operating_activities.value_or(0.0));
        net_income.push_back(r.net_income.value_or(0.0));
        capex.push_back(r.purchase_of_property_plant_and_equipment.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon getCashFlowStatementsAsync: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "filing_date", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
                                        "cfo", "change_cash", "change_assets", "dda", "dividends",
                                        "lt_debt_issuances", "ncf_financing", "ncf_investing",
                                        "ncf_operating", "net_income", "capex"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(filing_dates),
        epoch_frame::factory::array::make_array(period_ends),
        epoch_frame::factory::array::make_array(fiscal_years),
        epoch_frame::factory::array::make_array(fiscal_quarters),
        epoch_frame::factory::array::make_array(timeframes),
        epoch_frame::factory::array::make_array(cfo),
        epoch_frame::factory::array::make_array(change_cash),
        epoch_frame::factory::array::make_array(change_assets),
        epoch_frame::factory::array::make_array(dda),
        epoch_frame::factory::array::make_array(dividends),
        epoch_frame::factory::array::make_array(lt_debt_iss),
        epoch_frame::factory::array::make_array(ncf_fin),
        epoch_frame::factory::array::make_array(ncf_inv),
        epoch_frame::factory::array::make_array(ncf_oper),
        epoch_frame::factory::array::make_array(net_income),
        epoch_frame::factory::array::make_array(capex)};

    co_return epoch_frame::make_dataframe(index, data, columns);
  }

  drogon::Task<Expected<epoch_frame::DataFrame>>
  getIncomeStatementsAsync(std::string ticker, std::string from_date,
                           std::string to_date, std::optional<int> limit) const {
    std::vector<std::pair<std::string, std::string>> q;
    q.emplace_back("tickers", ticker);
    q.emplace_back("period_end.gte", from_date);
    q.emplace_back("period_end.lte", to_date);
    q.emplace_back("sort", "period_end.asc");
    if (limit.has_value())
      q.emplace_back("limit", std::to_string(*limit));

    const std::string path = "/stocks/financials/v1/income-statements";
    auto bodyRes = co_await httpAsyncGet(path, q);
    if (!bodyRes)
      co_return std::unexpected(bodyRes.error());

    IncomeStatementsResponse parsed{};
    const std::string bodyStr = *bodyRes;
    if (auto ec = glz::read_json(parsed, std::string_view(bodyStr)); ec) {
      SPDLOG_ERROR("Polygon getIncomeStatementsAsync parse failed: ticker={} path={} "
                   "ec={} body_prefix={}",
                   ticker, path, static_cast<int>(ec.ec),
                   bodyStr.substr(0, std::min<size_t>(bodyStr.size(), 256)));
      co_return makeError<epoch_frame::DataFrame>(
          200, "Failed to parse income statements JSON response", nullptr);
    }

    std::vector<std::string> date_strings;
    std::vector<std::string> filing_dates, period_ends, tickers_col, timeframes;
    std::vector<std::int64_t> fiscal_years, fiscal_quarters;
    std::vector<double> basic_eps, diluted_eps, revenue, cogs, gross_profit,
        operating_income, net_income, rd, sga;

    for (const auto &r : parsed.results) {
      date_strings.push_back(r.filing_date.value_or(""));
      filing_dates.push_back(r.filing_date.value_or(""));
      period_ends.push_back(r.period_end.value_or(""));
      tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
      fiscal_years.push_back(r.fiscal_year.value_or(0));
      fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
      timeframes.push_back(r.timeframe.value_or(""));
      basic_eps.push_back(r.basic_earnings_per_share.value_or(0.0));
      diluted_eps.push_back(r.diluted_earnings_per_share.value_or(0.0));
      revenue.push_back(r.revenue.value_or(0.0));
      cogs.push_back(r.cost_of_revenue.value_or(0.0));
      gross_profit.push_back(r.gross_profit.value_or(0.0));
      operating_income.push_back(r.operating_income.value_or(0.0));
      net_income.push_back(r.consolidated_net_income_loss.value_or(0.0));
      rd.push_back(r.research_development.value_or(0.0));
      sga.push_back(r.selling_general_administrative.value_or(0.0));
    }

    // Follow pagination if present
    std::string next = parsed.results.empty() ? "" : parsed.next_url.value_or("");
    int page_count = 1;
    while (!next.empty()) {
      auto parsed_url = parseNextUrl(next);
      if (!parsed_url) {
        SPDLOG_WARN("Polygon getIncomeStatementsAsync pagination: failed to parse next_url={}", next);
        break;
      }

      auto bodyRes2 = co_await httpAsyncGet(parsed_url->path, parsed_url->query);
      if (!bodyRes2) {
        SPDLOG_ERROR("Polygon getIncomeStatementsAsync pagination failed at page {}: {}",
                     page_count + 1, bodyRes2.error().message);
        break;
      }

      IncomeStatementsResponse page{};
      if (auto ec = glz::read_json(page, std::string_view(*bodyRes2)); ec) {
        SPDLOG_ERROR("Polygon getIncomeStatementsAsync page parse failed: page={} ec={}",
                     page_count + 1, static_cast<int>(ec.ec));
        break;
      }

      for (const auto &r : page.results) {
        date_strings.push_back(r.filing_date.value_or(""));
        filing_dates.push_back(r.filing_date.value_or(""));
        period_ends.push_back(r.period_end.value_or(""));
        tickers_col.push_back(r.tickers.empty() ? "" : r.tickers[0]);
        fiscal_years.push_back(r.fiscal_year.value_or(0));
        fiscal_quarters.push_back(r.fiscal_quarter.value_or(0));
        timeframes.push_back(r.timeframe.value_or(""));
        basic_eps.push_back(r.basic_earnings_per_share.value_or(0.0));
        diluted_eps.push_back(r.diluted_earnings_per_share.value_or(0.0));
        revenue.push_back(r.revenue.value_or(0.0));
        cogs.push_back(r.cost_of_revenue.value_or(0.0));
        gross_profit.push_back(r.gross_profit.value_or(0.0));
        operating_income.push_back(r.operating_income.value_or(0.0));
        net_income.push_back(r.consolidated_net_income_loss.value_or(0.0));
        rd.push_back(r.research_development.value_or(0.0));
        sga.push_back(r.selling_general_administrative.value_or(0.0));
      }

      next = page.next_url.value_or("");
      page_count++;
    }

    if (page_count > 1) {
      SPDLOG_INFO("Polygon getIncomeStatementsAsync: fetched {} pages for ticker={} total_rows={}",
                  page_count, ticker, date_strings.size());
    }

    // Parse all date strings to nanoseconds using Arrow strptime
    auto dates = parseDatesToNs(date_strings);
    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "filing_date", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
                                        "basic_eps", "diluted_eps", "revenue", "cogs", "gross_profit",
                                        "operating_income", "net_income", "rd", "sga"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
        epoch_frame::factory::array::make_array(filing_dates),
        epoch_frame::factory::array::make_array(period_ends),
        epoch_frame::factory::array::make_array(fiscal_years),
        epoch_frame::factory::array::make_array(fiscal_quarters),
        epoch_frame::factory::array::make_array(timeframes),
        epoch_frame::factory::array::make_array(basic_eps),
        epoch_frame::factory::array::make_array(diluted_eps),
        epoch_frame::factory::array::make_array(revenue),
        epoch_frame::factory::array::make_array(cogs),
        epoch_frame::factory::array::make_array(gross_profit),
        epoch_frame::factory::array::make_array(operating_income),
        epoch_frame::factory::array::make_array(net_income),
        epoch_frame::factory::array::make_array(rd),
        epoch_frame::factory::array::make_array(sga)};

    co_return epoch_frame::make_dataframe(index, data, columns);
  }
};

// Public API
FinancialsClient::FinancialsClient(Options options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

FinancialsClient::~FinancialsClient() = default;

Expected<epoch_frame::DataFrame>
FinancialsClient::getBalanceSheets(const std::string &ticker,
                                   const std::string &from_date,
                                   const std::string &to_date,
                                   std::optional<int> limit) const {
  return impl_->getBalanceSheets(ticker, from_date, to_date, limit);
}

Expected<epoch_frame::DataFrame>
FinancialsClient::getCashFlowStatements(const std::string &ticker,
                                        const std::string &from_date,
                                        const std::string &to_date,
                                        std::optional<int> limit) const {
  return impl_->getCashFlowStatements(ticker, from_date, to_date, limit);
}

Expected<epoch_frame::DataFrame>
FinancialsClient::getIncomeStatements(const std::string &ticker,
                                      const std::string &from_date,
                                      const std::string &to_date,
                                      std::optional<int> limit) const {
  return impl_->getIncomeStatements(ticker, from_date, to_date, limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
FinancialsClient::getBalanceSheetsAsync(std::string ticker, std::string from_date,
                                        std::string to_date, std::optional<int> limit) const {
  return impl_->getBalanceSheetsAsync(std::move(ticker), std::move(from_date),
                                      std::move(to_date), limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
FinancialsClient::getCashFlowStatementsAsync(std::string ticker, std::string from_date,
                                             std::string to_date, std::optional<int> limit) const {
  return impl_->getCashFlowStatementsAsync(std::move(ticker), std::move(from_date),
                                           std::move(to_date), limit);
}

drogon::Task<Expected<epoch_frame::DataFrame>>
FinancialsClient::getIncomeStatementsAsync(std::string ticker, std::string from_date,
                                           std::string to_date, std::optional<int> limit) const {
  return impl_->getIncomeStatementsAsync(std::move(ticker), std::move(from_date),
                                         std::move(to_date), limit);
}

DataFrameMetadata FinancialsClient::getBalanceSheetsMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "balance_sheets",
      .description = "Retrieve comprehensive balance sheet data for public companies, providing quarterly and annual snapshots of financial positions. This endpoint replaces the legacy Financials endpoint and offers detailed asset, liability, and equity information representing point-in-time financial positions. Use cases include financial analysis and company valuation, asset assessment and evaluation, debt structure analysis, and equity research.",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,
      .category_prefix = "BS:",
      .columns = {
          {.id = "ticker",
           .name = "Ticker",
           .description = "Stock symbol identifier",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "filing_date",
           .name = "Filing Date",
           .description = "SEC filing date in YYYY-MM-DD format",
           .type = ArrowType::TIMESTAMP_NS_UTC,
           .nullable = true},
          {.id = "period_end",
           .name = "Period End",
           .description = "Last date of the reporting period in YYYY-MM-DD format",
           .type = ArrowType::TIMESTAMP_NS_UTC,
           .nullable = true},
          {.id = "fiscal_year",
           .name = "Fiscal Year",
           .description = "Fiscal year of the reporting period",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "fiscal_quarter",
           .name = "Fiscal Quarter",
           .description = "Fiscal quarter number (1-4)",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "timeframe",
           .name = "Timeframe",
           .description = "Reporting period type: quarterly, annual, or trailing_twelve_months",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "accounts_payable",
           .name = "Accounts Payable",
           .description = "Amounts owed to suppliers for goods and services purchased on credit",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "accrued_liabilities",
           .name = "Accrued Liabilities",
           .description = "Expenses incurred but not yet paid, representing short-term obligations",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "aoci",
           .name = "Accumulated Other Comprehensive Income",
           .description = "Cumulative gains and losses not included in net income, such as foreign currency translation adjustments",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "cash",
           .name = "Cash and Equivalents",
           .description = "Highly liquid assets including currency and short-term investments readily convertible to cash",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "debt_current",
           .name = "Current Debt",
           .description = "Short-term debt obligations due within one year or current operating cycle",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "deferred_revenue",
           .name = "Deferred Revenue",
           .description = "Payments received in advance for goods or services not yet delivered, representing current liabilities",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "inventories",
           .name = "Inventories",
           .description = "Value of raw materials, work-in-process, and finished goods held for sale",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "lt_debt",
           .name = "Long-term Debt",
           .description = "Debt obligations and capital lease obligations due beyond one year",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "ppe_net",
           .name = "Property, Plant & Equipment (Net)",
           .description = "Net value of tangible long-term assets after depreciation used in operations",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "receivables",
           .name = "Accounts Receivable",
           .description = "Amounts owed by customers for goods and services sold on credit",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "retained_earnings",
           .name = "Retained Earnings",
           .description = "Cumulative net income retained in the business after dividends, may be negative (deficit)",
           .type = ArrowType::FLOAT64,
           .nullable = true},
      }};
}

DataFrameMetadata FinancialsClient::getCashFlowStatementsMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "cash_flow_statements",
      .description = "Retrieve comprehensive cash flow statement data for public companies, including quarterly, annual, and trailing twelve-month periods. This endpoint replaces the legacy Financials endpoint and provides detailed cash flow information across operating, investing, and financing activities. Use cases include cash flow analysis across different reporting periods, liquidity assessment evaluations, operational efficiency evaluation, and investment activity tracking.",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,
      .category_prefix = "CF:",
      .columns = {
          {.id = "ticker",
           .name = "Ticker",
           .description = "Stock symbol identifier",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "filing_date",
           .name = "Filing Date",
           .description = "SEC filing date in YYYY-MM-DD format",
           .type = ArrowType::TIMESTAMP_NS_UTC,
           .nullable = true},
          {.id = "period_end",
           .name = "Period End",
           .description = "Last date of the reporting period in YYYY-MM-DD format",
           .type = ArrowType::TIMESTAMP_NS_UTC,
           .nullable = true},
          {.id = "fiscal_year",
           .name = "Fiscal Year",
           .description = "Fiscal year of the reporting period",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "fiscal_quarter",
           .name = "Fiscal Quarter",
           .description = "Fiscal quarter number (1-4)",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "timeframe",
           .name = "Timeframe",
           .description = "Reporting period type: quarterly, annual, or trailing_twelve_months",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "cfo",
           .name = "Cash from Operating Activities",
           .description = "Net cash generated from core business operations",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "change_cash",
           .name = "Change in Cash and Equivalents",
           .description = "Net change in cash and cash equivalents during the period",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "change_assets",
           .name = "Change in Assets",
           .description = "Net change in asset accounts affecting cash flow",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "dda",
           .name = "Depreciation, Depletion & Amortization",
           .description = "Non-cash charges reducing net income but not affecting cash flow",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "dividends",
           .name = "Dividends Paid",
           .description = "Cash payments to shareholders as dividends, typically negative values",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "lt_debt_issuances",
           .name = "Long-term Debt Issuances/Repayments",
           .description = "Net cash from issuing or repaying long-term debt obligations",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "ncf_financing",
           .name = "Net Cash from Financing Activities",
           .description = "Net cash generated or used in financing activities including debt and equity transactions",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "ncf_investing",
           .name = "Net Cash from Investing Activities",
           .description = "Net cash used in or generated from investment activities including capital expenditures",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "ncf_operating",
           .name = "Net Cash from Operating Activities",
           .description = "Total net cash generated from operating activities including discontinued operations",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "net_income",
           .name = "Net Income",
           .description = "Starting point for operating cash flow calculation using indirect method",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "capex",
           .name = "Capital Expenditures",
           .description = "Cash spent on purchasing property, plant, and equipment, typically negative",
           .type = ArrowType::FLOAT64,
           .nullable = true},
      }};
}

DataFrameMetadata FinancialsClient::getIncomeStatementsMetadata() {
  using namespace data_sdk;
  return DataFrameMetadata{
      .data_type = "income_statements",
      .description = "Retrieve comprehensive income statement data for public companies, including key metrics such as revenue, expenses, and net income for various reporting periods. This endpoint replaces the legacy Financials endpoint and provides detailed financial performance data across quarterly, annual, and trailing twelve-month periods. Use cases include profitability analysis, revenue trend analysis, expense management evaluation, and earnings assessment.",
      .asset_class = AssetClass::Stocks,
      .index_normalized = true,
      .category_prefix = "IS:",
      .columns = {
          {.id = "ticker",
           .name = "Ticker",
           .description = "Stock symbol identifier",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "filing_date",
           .name = "Filing Date",
           .description = "SEC filing date in YYYY-MM-DD format",
           .type = ArrowType::TIMESTAMP_NS_UTC,
           .nullable = true},
          {.id = "period_end",
           .name = "Period End",
           .description = "Last date of the reporting period in YYYY-MM-DD format",
           .type = ArrowType::TIMESTAMP_NS_UTC,
           .nullable = true},
          {.id = "fiscal_year",
           .name = "Fiscal Year",
           .description = "Fiscal year of the reporting period",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "fiscal_quarter",
           .name = "Fiscal Quarter",
           .description = "Fiscal quarter number (1-4)",
           .type = ArrowType::INT32,
           .nullable = true},
          {.id = "timeframe",
           .name = "Timeframe",
           .description = "Reporting period type: quarterly, annual, or trailing_twelve_months",
           .type = ArrowType::STRING,
           .nullable = true},
          {.id = "basic_eps",
           .name = "Basic Earnings Per Share",
           .description = "Net income available to common shareholders divided by basic shares outstanding",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "diluted_eps",
           .name = "Diluted Earnings Per Share",
           .description = "Net income divided by diluted shares outstanding including potentially dilutive securities",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "revenue",
           .name = "Revenue",
           .description = "Total revenues from primary business operations during the period",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "cogs",
           .name = "Cost of Goods Sold",
           .description = "Direct costs attributable to producing goods sold by the company",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "gross_profit",
           .name = "Gross Profit",
           .description = "Revenue minus cost of goods sold, representing profit before operating expenses",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "operating_income",
           .name = "Operating Income",
           .description = "Profit from core business operations before interest and taxes",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "net_income",
           .name = "Net Income",
           .description = "Consolidated net income or loss after all expenses, taxes, and adjustments",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "rd",
           .name = "Research & Development",
           .description = "Expenses incurred for developing new products, services, or processes",
           .type = ArrowType::FLOAT64,
           .nullable = true},
          {.id = "sga",
           .name = "Selling, General & Administrative",
           .description = "Operating expenses related to selling products and managing the business",
           .type = ArrowType::FLOAT64,
           .nullable = true},
      }};
}

} // namespace data_sdk::polygon
