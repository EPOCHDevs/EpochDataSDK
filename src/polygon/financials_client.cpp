#include "epoch_data_sdk/polygon/financials_client.hpp"

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

    std::vector<std::int64_t> dates;
    std::vector<std::string> tickers_col, period_ends, timeframes;
    std::vector<std::int64_t> fiscal_years, fiscal_quarters;
    std::vector<double> accounts_payable, accrued_liab, aoci, cash, debt_current,
        deferred_rev, inventories, lt_debt, ppe_net, receivables, retained_earn;

    for (const auto &r : parsed.results) {
      const auto date_ns = parseDateToNs(r.period_end.value_or(""));
      dates.push_back(date_ns);
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
        const auto date_ns = parseDateToNs(r.period_end.value_or(""));
        dates.push_back(date_ns);
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
                  page_count, ticker, dates.size());
    }

    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
                                        "accounts_payable", "accrued_liabilities", "aoci", "cash",
                                        "debt_current", "deferred_revenue", "inventories", "lt_debt",
                                        "ppe_net", "receivables", "retained_earnings"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
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

    std::vector<std::int64_t> dates;
    std::vector<std::string> period_ends, tickers_col, timeframes;
    std::vector<std::int64_t> fiscal_years, fiscal_quarters;
    std::vector<double> cfo, change_cash, change_assets, dda, dividends,
        lt_debt_iss, ncf_fin, ncf_inv, ncf_oper, net_income, capex;

    for (const auto &r : parsed.results) {
      const auto date_ns = parseDateToNs(r.period_end.value_or(""));
      dates.push_back(date_ns);
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
        const auto date_ns = parseDateToNs(r.period_end.value_or(""));
        dates.push_back(date_ns);
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
                  page_count, ticker, dates.size());
    }

    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
                                        "cfo", "change_cash", "change_assets", "dda", "dividends",
                                        "lt_debt_issuances", "ncf_financing", "ncf_investing",
                                        "ncf_operating", "net_income", "capex"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
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

    std::vector<std::int64_t> dates;
    std::vector<std::string> period_ends, tickers_col, timeframes;
    std::vector<std::int64_t> fiscal_years, fiscal_quarters;
    std::vector<double> basic_eps, diluted_eps, revenue, cogs, gross_profit,
        operating_income, net_income, rd, sga;

    for (const auto &r : parsed.results) {
      const auto date_ns = parseDateToNs(r.period_end.value_or(""));
      dates.push_back(date_ns);
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
        const auto date_ns = parseDateToNs(r.period_end.value_or(""));
        dates.push_back(date_ns);
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
                  page_count, ticker, dates.size());
    }

    auto index = epoch_frame::factory::index::make_datetime_index(dates, "", "UTC");
    std::vector<std::string> columns = {"ticker", "period_end", "fiscal_year", "fiscal_quarter", "timeframe",
                                        "basic_eps", "diluted_eps", "revenue", "cogs", "gross_profit",
                                        "operating_income", "net_income", "rd", "sga"};
    std::vector<arrow::ChunkedArrayPtr> data{
        epoch_frame::factory::array::make_array(tickers_col),
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

} // namespace data_sdk::polygon
