#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <glaze/glaze.hpp>

namespace data_sdk::polygon {

struct AggBar {
  double v{};                      // volume
  std::optional<double> vw{};      // volume weighted
  double o{};                      // open
  double c{};                      // close
  double h{};                      // high
  double l{};                      // low
  std::int64_t t{};                // timestamp (ms)
  std::optional<std::int64_t> n{}; // number of trades
};

struct AggregatesResponse {
  std::string ticker;
  std::optional<int> queryCount;
  std::optional<int> resultsCount;
  std::optional<bool> adjusted{};
  std::optional<std::string> status;
  std::optional<std::string> request_id;
  std::vector<AggBar> results;
  std::optional<size_t> count;
  std::optional<std::string> next_url;
};

// v3 latest trade shape (compatible with v2 via optional fields)
struct LastTrade {
  std::string T;                 // ticker
  double p{};                    // price
  std::optional<std::int64_t> s; // size
  std::optional<int> x;          // exchange id
  std::int64_t t{};              // timestamp (ms)
};

struct LastTradeResponse {
  std::string status;
  std::string request_id;
  std::optional<std::string> symbol; // v2 compatibility
  LastTrade results;
};

// v3 latest quote (NBBO) shape
struct LastQuote {
  std::string T;                  // ticker
  double ap{};                    // ask price
  double bp{};                    // bid price
  std::optional<std::int64_t> as; // ask size
  std::optional<std::int64_t> bs; // bid size
  std::int64_t t{};               // timestamp (ms)
};

struct LastQuoteResponse {
  std::string status;
  std::string request_id;
  std::optional<std::string> symbol; // v2 compatibility
  LastQuote results;
};

// /v1/open-close/{ticker}/{date}
struct OpenCloseResponse {
  std::string status;
  std::string request_id;
  std::optional<std::string>
      symbol; // sometimes present as "symbol" or "ticker"
  std::optional<std::string> ticker;
  std::optional<std::string> from; // date string
  std::optional<double> open;
  std::optional<double> close;
  std::optional<double> afterHours;
  std::optional<double> preMarket;
  std::optional<std::int64_t> openTime;
  std::optional<std::int64_t> closeTime;
};

// Financial Statements (v1)
struct BalanceSheetData {
  std::optional<double> accounts_payable;
  std::optional<double> accrued_and_other_current_liabilities;
  std::optional<double> accumulated_other_comprehensive_income;
  std::optional<double> cash_and_equivalents;
  std::optional<std::string> cik;
  std::optional<std::string> common_stock;
  std::optional<double> debt_current;
  std::optional<double> deferred_revenue_current;
  std::optional<std::int64_t> fiscal_quarter;
  std::optional<std::int64_t> fiscal_year;
  std::optional<double> inventories;
  std::optional<double> long_term_debt_and_capital_lease_obligations;
  std::optional<std::string> other_assets;
  std::optional<double> other_current_assets;
  std::optional<std::string> other_equity;
  std::optional<double> other_noncurrent_liabilities;
  std::optional<std::string> period_end;
  std::optional<double> property_plant_equipment_net;
  std::optional<double> receivables;
  std::optional<double> retained_earnings_deficit;
  std::vector<std::string> tickers;
  std::optional<std::string> timeframe;
};

struct BalanceSheetsResponse {
  std::optional<int> count;
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<BalanceSheetData> results;
  std::string status;
};

struct CashFlowData {
  std::optional<double> cash_from_operating_activities_continuing_operations;
  std::optional<double> change_in_cash_and_equivalents;
  std::optional<double> change_in_other_operating_assets_and_liabilities_net;
  std::optional<std::string> cik;
  std::optional<double> depreciation_depletion_and_amortization;
  std::optional<double> dividends;
  std::optional<std::int64_t> fiscal_quarter;
  std::optional<std::int64_t> fiscal_year;
  std::optional<double> long_term_debt_issuances_repayments;
  std::optional<double> net_cash_from_financing_activities;
  std::optional<double> net_cash_from_financing_activities_continuing_operations;
  std::optional<double> net_cash_from_investing_activities;
  std::optional<double> net_cash_from_investing_activities_continuing_operations;
  std::optional<double> net_cash_from_operating_activities;
  std::optional<double> net_income;
  std::optional<double> other_financing_activities;
  std::optional<double> other_investing_activities;
  std::optional<double> other_operating_activities;
  std::optional<std::string> period_end;
  std::optional<double> purchase_of_property_plant_and_equipment;
  std::optional<double> short_term_debt_issuances_repayments;
  std::vector<std::string> tickers;
  std::optional<std::string> timeframe;
};

struct CashFlowStatementsResponse {
  std::optional<int> count;
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<CashFlowData> results;
  std::string status;
};

struct IncomeStatementData {
  std::optional<double> basic_earnings_per_share;
  std::optional<double> basic_shares_outstanding;
  std::optional<std::string> cik;
  std::optional<double> consolidated_net_income_loss;
  std::optional<double> cost_of_revenue;
  std::optional<double> diluted_earnings_per_share;
  std::optional<double> diluted_shares_outstanding;
  std::optional<std::string> dividends;
  std::optional<std::int64_t> fiscal_quarter;
  std::optional<std::int64_t> fiscal_year;
  std::optional<double> gross_profit;
  std::optional<double> income_before_income_taxes;
  std::optional<double> income_taxes;
  std::optional<double> net_income_loss_attributable_to_common_shareholders;
  std::optional<double> operating_income;
  std::optional<double> other_income_expenses;
  std::optional<double> other_operating_expenses;
  std::optional<std::string> period_end;
  std::optional<double> research_development;
  std::optional<double> revenue;
  std::optional<double> selling_general_administrative;
  std::vector<std::string> tickers;
  std::optional<std::string> timeframe;
};

struct IncomeStatementsResponse {
  std::optional<int> count;
  std::optional<std::string> next_url;
  std::string request_id;
  std::vector<IncomeStatementData> results;
  std::string status;
};

struct FinancialRatiosData {
  std::optional<double> average_volume;
  std::optional<double> cash;
  std::optional<std::string> cik;
  std::optional<double> current;
  std::optional<std::string> date;
  std::optional<double> debt_to_equity;
  std::optional<double> dividend_yield;
  std::optional<double> earnings_per_share;
  std::optional<double> enterprise_value;
  std::optional<double> ev_to_ebitda;
  std::optional<double> ev_to_sales;
  std::optional<double> free_cash_flow;
  std::optional<double> market_cap;
  std::optional<double> price;
  std::optional<double> price_to_book;
  std::optional<double> price_to_cash_flow;
  std::optional<double> price_to_earnings;
  std::optional<double> price_to_free_cash_flow;
  std::optional<double> price_to_sales;
  std::optional<double> quick;
  std::optional<double> return_on_assets;
  std::optional<double> return_on_equity;
  std::optional<std::string> ticker;
};

struct FinancialRatiosResponse {
  std::optional<int> count;
  std::string request_id;
  std::vector<FinancialRatiosData> results;
  std::string status;
};

} // namespace data_sdk::polygon
