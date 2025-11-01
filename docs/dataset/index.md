# Dataset Reference

Welcome to the EpochDataSDK Dataset Reference. This guide provides comprehensive information about all the financial datasets available for querying in your trading strategies and analysis.

## Overview

EpochDataSDK provides access to institutional-grade financial data across multiple categories:

- **[Market Data](market-data.md)** - Price, volume, and trade data for stocks, cryptocurrencies, and forex
- **[Economic Data](economic-data.md)** - U.S. economic indicators and Federal Reserve data
- **[Corporate Data](corporate-data.md)** - Company financials, dividends, stock splits, and IPO information
- **[Alternative Data](alternative-data.md)** - Institutional holdings, insider trading, short interest, and financial news

All data is returned in structured table format (DataFrames), optimized for quantitative analysis and backtesting.

---

## Quick Start

### What Data Is Available?

| Category | Data Types | Update Frequency | History |
|----------|-----------|------------------|---------|
| **Market Data** | OHLCV bars, tick trades, bid-ask quotes | Real-time to daily | Multi-year |
| **Economic Data** | GDP, inflation, unemployment, interest rates | Monthly/Quarterly | Up to 100+ years |
| **Corporate Data** | Financial statements, dividends, splits, IPOs | Quarterly/As-filed | 10+ years |
| **Alternative Data** | Institutional holdings, insider trades, short interest | Daily to quarterly | 10+ years |

---

## Data Categories

### [Market Data](market-data.md)

Price and volume data for trading analysis:

- **OHLCV Aggregates** - Historical price bars (daily, minute-level)
- **Tick Trades** - Individual trade executions
- **Quotes** - Best bid and ask prices

**Coverage**: U.S. stocks, cryptocurrencies, foreign exchange

**Use Cases**: Backtesting, technical analysis, price discovery, execution analysis

---

### [Economic Data](economic-data.md)

U.S. economic indicators from the Federal Reserve Economic Data (FRED):

- **Inflation Metrics** - CPI, Core CPI, PCE
- **Interest Rates** - Fed Funds, Treasury yields (3-month to 30-year)
- **Economic Activity** - GDP, industrial production, retail sales
- **Labor Market** - Unemployment rate, jobless claims, payrolls
- **Market Indicators** - S&P 500 index, VIX volatility

**Update Frequency**: Monthly to quarterly

**Use Cases**: Macro factor models, regime detection, risk-free rate calculations

---

### [Corporate Data](corporate-data.md)

Company fundamentals and corporate actions:

- **Financial Statements** - Income statements, balance sheets, cash flow statements
- **Dividends** - Cash and stock dividend distributions
- **Stock Splits** - Forward and reverse split history
- **IPO Data** - Initial public offering details and schedules

**Update Frequency**: Quarterly (financials), event-driven (dividends, splits)

**Use Cases**: Fundamental analysis, value investing, total return calculations

---

### [Alternative Data](alternative-data.md)

Non-traditional data sources for edge discovery:

- **Institutional Holdings** - Quarterly 13F-HR filings from hedge funds and institutions
- **Insider Trading** - Officer, director, and major shareholder transactions
- **Short Interest** - Daily short volume and bi-weekly settlement data
- **Financial News** - Corporate news and announcements

**Update Frequency**: Daily to quarterly (varies by data type)

**Use Cases**: Smart money tracking, sentiment analysis, crowding detection

---

## Data Characteristics

### Time Ranges
All datasets support historical queries with flexible date ranges. History depth varies by dataset:

- **Market Data**: Multi-year tick and daily data
- **Economic Data**: Up to 100+ years for key indicators
- **Corporate/Alternative**: Generally 10-20+ years

### Data Frequency

| Frequency | Dataset Examples |
|-----------|-----------------|
| **Intraday** | Tick trades, minute bars, quotes |
| **Daily** | OHLCV bars, short volume, news |
| **Weekly** | Initial jobless claims |
| **Monthly** | Inflation, unemployment, retail sales |
| **Quarterly** | Financial statements, institutional holdings, GDP |
| **Event-Driven** | Dividends, splits, insider trades, IPOs |

### Coverage

- **Equities**: All U.S. listed stocks (NYSE, NASDAQ, etc.)
- **Cryptocurrencies**: Major cryptocurrencies with X: prefix (e.g., X:BTCUSD)
- **Forex**: Currency pairs with C: prefix (e.g., C:EURUSD)
- **Economic**: U.S. Federal Reserve and government data
- **Corporate**: U.S. publicly traded companies

---

## Common Use Cases

### Backtesting a Trading Strategy
- Query historical price data (OHLCV aggregates)
- Include dividend and split adjustments for total returns
- Add economic indicators for macro factors
- Use institutional holdings to detect smart money flows

### Fundamental Analysis
- Pull quarterly financial statements (income, balance sheet, cash flow)
- Track dividend history and yield trends
- Monitor insider buying/selling activity
- Compare metrics across peer companies

### Risk Management
- Access Treasury yields for risk-free rate calculations
- Monitor VIX for volatility regime detection
- Track economic indicators for regime shifts
- Use short interest for crowding analysis

### Event-Driven Strategies
- Track IPO schedules and pricing
- Monitor insider transactions for sentiment signals
- Analyze institutional ownership changes quarterly
- React to corporate actions (splits, dividends)

---

## Next Steps

Explore each dataset category in detail:

1. **[Market Data](market-data.md)** - Start here for price and volume data
2. **[Economic Data](economic-data.md)** - Add macro factors to your analysis
3. **[Corporate Data](corporate-data.md)** - Incorporate fundamentals
4. **[Alternative Data](alternative-data.md)** - Discover edge through alternative datasets

Each section provides detailed field descriptions, coverage information, and practical use cases for traders.
