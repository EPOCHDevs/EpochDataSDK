# Corporate Data

Corporate data provides fundamental information about publicly traded companies, including financial statements, dividend payments, stock splits, and initial public offerings.

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

**Financial data is indexed by SEC filing dates, not quarter-end dates, preventing look-ahead bias in your strategies.**

When a company's quarter ends (say, March 31), the financial numbers aren't public yet. Companies have up to 45 days to file their 10-Q with the SEC. This means Q1 numbers that "belong to" March 31 might not be available until mid-May.

**The problem with using quarter-end dates:**
If your backtest uses Q1 data on March 31, you're making trading decisions with information you couldn't have had for another 45 days. Over a 10-year backtest, this creates years of "seeing the future."

**How we solve this:**
- All financial data is **indexed by the filing date** (when reports became public)
- The quarter-end date is included in the data so you know which period the financials cover
- You can only "trade" on financial information after it was actually filed with the SEC
- This ensures your fundamental strategies reflect real-world timing

For value investors and fundamental traders, this means your backtest won't show artificially perfect timing from using data before it was publicly available.

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

**Profitability Analysis**
- Compare gross margins across competitors
- Track operating margin trends over time
- Identify improving/deteriorating profitability

**Growth Screening**
- Find companies with accelerating revenue growth
- Identify earnings growth consistency
- Compare growth rates to stock valuations

**Quality Assessment**
- Analyze R&D intensity (R&D / Revenue)
- Evaluate operating leverage (operating income growth vs. revenue growth)
- Track earnings consistency and predictability

**Valuation**
- Calculate P/E ratios using latest EPS
- Build earnings-based DCF models
- Compare earnings yields across securities

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

**Financial Health Assessment**
- Calculate current ratio (current assets / current liabilities)
- Measure debt-to-equity ratio
- Analyze asset quality and composition

**Value Investing**
- Calculate price-to-book (P/B) ratio
- Identify net-net opportunities (current assets > market cap)
- Find asset-rich companies trading below book value

**Credit Analysis**
- Evaluate leverage (debt / equity)
- Assess liquidity (cash relative to obligations)
- Analyze debt maturity schedule

**Working Capital Analysis**
- Calculate working capital (current assets - current liabilities)
- Analyze days sales outstanding (receivables / daily revenue)
- Evaluate inventory turnover

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

**Cash Generation Quality**
- Compare operating cash flow to net income (>1 is good)
- Identify earnings quality (high income but low cash = red flag)
- Track free cash flow (operating cash - capex)

**Capital Allocation Analysis**
- See how company invests cash (capex, acquisitions)
- Track shareholder returns (dividends + buybacks)
- Analyze debt management (issuance vs. repayment)

**Financial Sustainability**
- Verify company can fund operations from cash flow
- Identify reliance on external financing
- Track cash burn rate for unprofitable companies

**Dividend Coverage**
- Ensure operating cash flow > dividends (sustainable)
- Identify dividend cuts risk (cash flow deteriorating)
- Calculate payout ratio from cash flow perspective

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

### Use Cases

**Total Return Calculation**
- **Critical for accurate backtesting**: Price returns alone understate total returns
- Add dividends back to calculate true investment performance
- Compound reinvested dividends for long-term returns

**Income Strategy Development**
- Screen for high dividend yields
- Build dividend growth portfolios
- Track dividend consistency and reliability

**Dividend Analysis**
- Calculate dividend yield (annual dividend / price)
- Track dividend growth rates
- Identify dividend cuts or increases

**Event Trading**
- Dividend capture strategies
- Study price behavior around ex-dividend dates
- Analyze dividend aristocrats vs. non-payers

**Risk Assessment**
- Dividend cuts signal financial stress
- Consistent dividend growth = quality signal
- Compare dividend to free cash flow (coverage ratio)

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

**Price Adjustment (Critical for Backtesting)**
- **Must adjust historical prices** for splits to maintain continuity
- Without adjustment: artificial price discontinuities
- Prevents false signals in technical analysis

**Corporate Action Tracking**
- Forward splits often signal management confidence
- Reverse splits often signal distress (maintaining listing requirements)
- Study price behavior post-split

**Data Cleaning**
- Verify data quality by checking for split adjustments
- Ensure volume is adjusted proportionally
- Reconcile historical price series

**Strategy Impact**
- Some strategies exclude low-priced stocks (reverse splits push up)
- Splits affect option strike prices and contracts
- Share count changes impact position sizing

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

**IPO Trading Strategies**
- Track IPO calendar for upcoming opportunities
- Study first-day price movements
- Analyze pricing (final price vs. range)
- Identify over/under-subscribed deals

**New Issue Research**
- Compare offering size across similar companies
- Track IPO market cycles (hot vs. cold)
- Analyze industry IPO clusters
- Study sponsor/underwriter patterns

**Portfolio Screening**
- Exclude recent IPOs (often volatile, limited history)
- Focus on seasoned companies with >2 years public
- Track IPO lockup expiration dates (insiders can sell)

**Market Sentiment**
- IPO volume as market sentiment indicator
- First-day pops signal investor enthusiasm
- Withdrawn IPOs signal weak market conditions

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

## Fundamental Analysis Examples

### Value Screening

Combine multiple data points:
- **P/E Ratio**: Price / Diluted EPS < 15
- **P/B Ratio**: Market cap / Total Equity < 1.5
- **Debt/Equity**: Total Liabilities / Total Equity < 0.5
- **Dividend Yield**: Annual Dividend / Price > 3%

### Growth Screening

Look for acceleration:
- Revenue growth > 20% year-over-year
- Earnings growth > revenue growth (margin expansion)
- Positive operating cash flow
- Low debt-to-equity < 0.3

### Quality Metrics

Financial health indicators:
- **ROE**: Net Income / Total Equity > 15%
- **Current Ratio**: Current Assets / Current Liabilities > 2.0
- **Free Cash Flow**: Operating Cash Flow - Capex > 0
- **Dividend Coverage**: Operating Cash Flow / Dividends > 2.0

### Relative Valuation

Compare across peers:
- P/E relative to sector average
- Revenue growth vs. industry median
- Margin analysis vs. competitors
- Balance sheet strength ranking

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

### For Backtesting

1. **Use Filing Dates**: Always use when data became public (filing_date), not period end
2. **Include Dividends**: Calculate total returns, not just price returns
3. **Adjust for Splits**: Apply split adjustments to all historical prices
4. **Point-in-Time**: Use data as it was originally filed (avoid restated data)

### For Screening

1. **Multiple Criteria**: Use multiple metrics to filter false positives
2. **Relative Metrics**: Compare to sector/industry, not absolute values
3. **Trend Analysis**: Look at trends (improving margins) not just snapshots
4. **Survivorship**: Include delisted/bankrupt companies to avoid bias

### For Analysis

1. **Context Matters**: Compare companies in same industry
2. **Read Footnotes**: Understand accounting changes and one-time items
3. **Cash is King**: Prioritize cash flow over earnings quality
4. **Trend > Absolute**: Watch direction of metrics, not just levels

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
