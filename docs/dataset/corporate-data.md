---
page_type: reference
layout: default
order: 2
category: Datasets
description: Financial statements, dividends, splits, and IPO data
parent: ./index.md
---

# Corporate Data

Fundamental company data - financial statements, dividends, splits, and IPO information. Indexed by filing dates for backtesting accuracy.

---

## Datasets

:::grid
[
  {
    "title": "Financial Statements",
    "description": "Income statement, balance sheet, cash flow. Quarterly and annual.",
    "category": "Fundamentals",
    "coverage": "10+ years",
    "frequencies": "Quarterly (10-Q), Annual (10-K)"
  },
  {
    "title": "Dividends",
    "description": "Cash and stock distributions. Critical for total return calculations.",
    "category": "Corporate Actions",
    "coverage": "10+ years",
    "frequencies": "Event-driven"
  },
  {
    "title": "Stock Splits",
    "description": "Forward and reverse splits. Required for price adjustment.",
    "category": "Corporate Actions",
    "coverage": "Complete history",
    "frequencies": "Event-driven"
  },
  {
    "title": "IPO Data",
    "description": "New listings, pricing, offering details. Calendar tracking.",
    "category": "Events",
    "coverage": "10+ years",
    "frequencies": "Event-driven"
  }
]
:::

---

## Overview

| Dataset | Update Frequency | Coverage | Primary Use |
|---------|------------------|----------|-------------|
| **Financial Statements** | Quarterly | U.S. public companies | Fundamental analysis, screening |
| **Dividends** | Event-driven | U.S. stocks | Income strategies, total returns |
| **Stock Splits** | Event-driven | U.S. stocks | Price adjustment, corporate actions |
| **IPO Data** | Event-driven | New listings | IPO strategies, calendar tracking |

**Coverage**: All U.S. publicly traded companies

**History**: 10+ years for most datasets

---

## Designed for Realistic Backtesting

:::warning Lookahead Bias Prevention
Financial data indexed by `filing_date`, **not** `period_end`.

When Q1 ends (March 31), earnings aren't public until filed (up to 45 days later). Using `period_end` = trading on data you couldn't have known = lookahead bias.

**Always use `filing_date`** for backtesting. `period_end` included for reference only.
:::

---

## Financial Statements

### What They Are

Quarterly and annual financial reports filed with the SEC by public companies, providing detailed information about a company's financial condition and performance.

**Three Statement Types**:
1. **Income Statement** - Revenue, expenses, profitability
2. **Balance Sheet** - Assets, liabilities, equity
3. **Cash Flow Statement** - Operating, investing, and financing cash flows

**Update Frequency**: Quarterly (10-Q) and Annual (10-K)

**Reporting Deadlines**:
- Large companies: 40 days after quarter end
- Smaller companies: 45 days after quarter end

**History**: 10+ years of quarterly and annual data

---

## Income Statement

**What It Is**: Shows a company's revenue, expenses, and profit over a specific period (quarter or year).

### Key Fields

| Field | Description |
|-------|-------------|
| **fiscal_year** | Fiscal year of report |
| **fiscal_quarter** | Quarter (1-4) or blank for annual |
| **filing_date** | When the report was filed with SEC |
| **period_end** | Last date of reporting period |
| **revenue** | Total sales (top line) |
| **cost_of_revenue** | Direct costs of producing goods/services |
| **gross_profit** | Revenue minus cost of revenue |
| **operating_expenses** | R&D, sales/marketing, general/administrative |
| **operating_income** | Profit from core operations (EBIT) |
| **net_income** | Bottom line profit after all expenses and taxes |
| **basic_eps** | Earnings per share (basic) |
| **diluted_eps** | Earnings per share (fully diluted) |
| **shares_outstanding** | Average shares outstanding during period |

### Additional Metrics Available

- **Research & Development (R&D)** - Innovation spending
- **Selling, General & Administrative (SG&A)** - Operating overhead
- **EBITDA** - Earnings before interest, taxes, depreciation, amortization
- **Interest Expense** - Debt servicing costs
- **Tax Expense** - Income taxes paid

### Use Cases

- **Profitability Analysis**: Compare gross/operating margins across competitors, track trends over time
- **Growth Screening**: Find accelerating revenue/earnings growth, compare growth rates to valuations
- **Quality Assessment**: Analyze R&D intensity, operating leverage, earnings consistency
- **Valuation**: Calculate P/E ratios, build DCF models, compare earnings yields

---

## Balance Sheet

**What It Is**: Snapshot of a company's financial position at a specific point in time, showing what it owns (assets), owes (liabilities), and net worth (equity).

### Key Fields - Assets

| Field | Description |
|-------|-------------|
| **cash_and_equivalents** | Most liquid assets |
| **short_term_investments** | Marketable securities |
| **accounts_receivable** | Money owed by customers |
| **inventory** | Unsold products and materials |
| **current_assets** | Assets convertible to cash within 1 year |
| **property_plant_equipment** | Physical assets (net of depreciation) |
| **goodwill** | Value from acquisitions |
| **intangible_assets** | Patents, trademarks, intellectual property |
| **total_assets** | Sum of all assets |

### Key Fields - Liabilities & Equity

| Field | Description |
|-------|-------------|
| **accounts_payable** | Money owed to suppliers |
| **current_debt** | Debt due within 1 year |
| **current_liabilities** | Obligations due within 1 year |
| **long_term_debt** | Debt due after 1 year |
| **total_liabilities** | Sum of all obligations |
| **common_stock** | Par value of issued shares |
| **retained_earnings** | Cumulative profits reinvested |
| **total_equity** | Assets minus liabilities (book value) |

### Use Cases

- **Financial Health**: Calculate current ratio, debt-to-equity, analyze asset quality and composition
- **Value Investing**: Calculate P/B ratio, identify net-net opportunities (current assets > market cap)
- **Credit Analysis**: Evaluate leverage, liquidity, debt maturity schedule
- **Working Capital**: Calculate working capital, days sales outstanding, inventory turnover

---

## Cash Flow Statement

**What It Is**: Shows actual cash generated and used by the company, categorized by operating, investing, and financing activities.

### Key Fields

| Field | Description |
|-------|-------------|
| **net_income** | Starting point (from income statement) |
| **depreciation_amortization** | Non-cash expenses added back |
| **operating_cash_flow** | Cash from core business operations |
| **capital_expenditures** | Spending on property, plant, equipment |
| **investing_cash_flow** | Cash from buying/selling assets and investments |
| **dividends_paid** | Cash returned to shareholders |
| **debt_issued** | New debt raised |
| **debt_repaid** | Debt paid down |
| **stock_issued** | Cash from issuing new shares |
| **stock_repurchased** | Cash used for buybacks |
| **financing_cash_flow** | Cash from debt, equity, and dividends |
| **change_in_cash** | Net change in cash position |

### Use Cases

- **Cash Quality**: Compare operating cash flow to net income (>1 = good), track free cash flow (OCF - capex)
- **Capital Allocation**: Analyze spending (capex, acquisitions), shareholder returns (dividends + buybacks), debt management
- **Financial Sustainability**: Verify operations funded by cash flow, identify external financing reliance, track burn rate
- **Dividend Coverage**: Ensure OCF > dividends (sustainable), identify dividend cut risk from deteriorating cash flow

---

## Dividends

**What It Is**: Cash or stock distributions paid by companies to shareholders.

### Key Fields

| Field | Description |
|-------|-------------|
| **ex_dividend_date** | Date after which new buyers don't get dividend |
| **declaration_date** | Date dividend was announced |
| **record_date** | Date to be on record to receive dividend |
| **pay_date** | Date dividend is paid |
| **cash_amount** | Dividend per share (in dollars) |
| **frequency** | Payment frequency (quarterly, annual, etc.) |
| **dividend_type** | CD (cash), SC (stock), LT (long-term gain), etc. |

### Data Characteristics

**Update Frequency**: Event-driven (as companies declare dividends)

**History**: 10+ years of dividend history

**Coverage**: All dividend-paying U.S. stocks

**Important Date**: **Ex-dividend date** is critical - buy before this date to receive the dividend.

:::tip Total Returns
Dividends are essential for accurate backtesting. Stock returns without dividends severely understate actual performance. Always include dividend adjustments in price data or add dividends separately.
:::

### Use Cases

- **Total Return Calculation**: Critical for backtesting - add dividends to price returns for true performance
- **Income Strategies**: Screen for high yields, build dividend growth portfolios, track consistency
- **Dividend Analysis**: Calculate yield, track growth rates, identify cuts or increases
- **Event Trading**: Dividend capture strategies, study price behavior around ex-dates
- **Risk Assessment**: Cuts signal distress, consistent growth signals quality, compare to FCF (coverage)

---

## Stock Splits

**What It Is**: Corporate action where a company divides existing shares into multiple shares, changing the number of shares but not the total market value.

### Key Fields

| Field | Description |
|-------|-------------|
| **execution_date** | Date split becomes effective |
| **split_from** | Old share count |
| **split_to** | New share count |
| **split_ratio** | Ratio of new to old (e.g., 2-for-1 = 2.0) |

### Split Types

**Forward Split**: Increases number of shares, reduces price
- Example: 2-for-1 split → $100 stock becomes $50, 1 share becomes 2

**Reverse Split**: Reduces number of shares, increases price
- Example: 1-for-10 reverse → $1 stock becomes $10, 10 shares become 1

### Data Characteristics

**Update Frequency**: Event-driven (as companies announce and execute splits)

**History**: Complete split history for all U.S. stocks

**Coverage**: Both forward and reverse splits

### Use Cases

- **Price Adjustment**: Must adjust historical prices for splits to maintain continuity and prevent false technical signals
- **Corporate Action Tracking**: Forward splits signal confidence; reverse splits signal distress (maintaining listing)
- **Data Cleaning**: Verify split adjustments applied, ensure volume adjusted proportionally
- **Strategy Impact**: Splits affect price filters, option contracts, and position sizing calculations

---

## IPO Data

**What It Is**: Information about companies going public through Initial Public Offerings.

### Key Fields

| Field | Description |
|-------|-------------|
| **ticker** | Stock ticker symbol |
| **company_name** | Company name |
| **listing_date** | Date stock begins trading publicly |
| **announced_date** | Date IPO was announced |
| **offering_price** | Final IPO price per share |
| **price_range_low** | Initial filing price range (low) |
| **price_range_high** | Initial filing price range (high) |
| **shares_offered** | Number of shares sold in IPO |
| **offering_value** | Total amount raised (shares × price) |
| **ipo_status** | Status (anticipated, priced, withdrawn) |

### Data Characteristics

**Update Frequency**: Event-driven (as IPOs are announced and completed)

**Coverage**: U.S. exchange listings (NYSE, NASDAQ)

**History**: IPO data going back 10+ years

### Use Cases

- **IPO Trading**: Track calendar, study first-day movements, analyze pricing vs. range, identify over/under-subscribed deals
- **New Issue Research**: Compare offering sizes, track market cycles (hot vs. cold), analyze industry clusters
- **Portfolio Screening**: Exclude recent IPOs (volatile, limited history), track lockup expirations
- **Market Sentiment**: IPO volume signals market appetite, first-day pops signal enthusiasm, withdrawals signal weakness

---

## Data Quality & Timing

### Filing Delays

**Financial Statements**:
- Filed 40-45 days after quarter end
- Use **filing_date** (not period_end) for backtesting to avoid look-ahead bias

**Dividends**:
- Declared in advance (usually weeks before ex-date)
- Use **ex_dividend_date** for total return calculations

**Splits**:
- Announced weeks before execution
- Use **execution_date** for price adjustments

**IPOs**:
- Announced weeks/months before listing
- Use **listing_date** for trading eligibility

### Data Completeness

- **Large companies**: Most complete data (better disclosure)
- **Small companies**: Some optional fields may be blank
- **Historical**: Older data may have fewer fields due to changing standards

---

## Integration with Other Datasets

### + Market Data
- Calculate valuation ratios (P/E, P/B, EV/EBITDA)
- Study price reaction to earnings releases
- Align earnings dates with price movements
- Adjust prices for dividends and splits

### + Economic Data
- Adjust revenues and earnings for inflation
- Model earnings sensitivity to GDP growth
- Correlate margins with input costs (commodities, labor)
- Sector rotation based on economic cycle

### + Alternative Data
- Cross-reference insider buying with financial metrics
- Compare fundamentals of heavily-shorted companies
- Study institutional preference for quality metrics
- Validate fundamental story with smart money positioning

---

## Best Practices

**Backtesting**: Use `filing_date` (not period end), include dividends for total returns, adjust for splits, use point-in-time data

**Screening**: Multiple criteria to filter false positives, relative metrics vs. sector/industry, trend analysis over snapshots, include delisted companies

**Analysis**: Compare within same industry, understand accounting changes (read footnotes), prioritize cash flow over earnings, watch metric direction

### Common Ratios

**Profitability**:
- Gross Margin = Gross Profit / Revenue
- Operating Margin = Operating Income / Revenue
- Net Margin = Net Income / Revenue
- ROE = Net Income / Total Equity
- ROA = Net Income / Total Assets

**Liquidity**:
- Current Ratio = Current Assets / Current Liabilities
- Quick Ratio = (Current Assets - Inventory) / Current Liabilities

**Leverage**:
- Debt/Equity = Total Liabilities / Total Equity
- Debt/Assets = Total Liabilities / Total Assets
- Interest Coverage = EBIT / Interest Expense

**Efficiency**:
- Asset Turnover = Revenue / Total Assets
- Inventory Turnover = Cost of Revenue / Inventory
- Receivables Turnover = Revenue / Accounts Receivable

---

## Next Steps

- **[Market Data](market-data.md)** - Combine fundamentals with price action
- **[Economic Data](economic-data.md)** - Add macro context to fundamental analysis
- **[Alternative Data](alternative-data.md)** - Validate fundamental story with smart money moves
