# Economic Data

Economic data provides critical macro context for trading decisions, including inflation rates, interest rates, GDP, employment metrics, and market indicators from the Federal Reserve Economic Data (FRED) system.

---

## Overview

| Category | Example Indicators | Frequency | History |
|----------|-------------------|-----------|---------|
| **Inflation** | CPI, Core CPI, PCE | Monthly | 60+ years |
| **Interest Rates** | Fed Funds, Treasury Yields | Daily | 30+ years |
| **Economic Activity** | GDP, Industrial Production | Quarterly/Monthly | 70+ years |
| **Labor Market** | Unemployment, Jobless Claims | Weekly/Monthly | 70+ years |
| **Market Indicators** | S&P 500, VIX | Daily | 30+ years |

**Coverage**: United States economic and financial indicators

**Update Schedule**: Varies by indicator (daily, weekly, monthly, quarterly)

---

## Designed for Realistic Backtesting

**Our economic data is carefully engineered to prevent look-ahead bias in your backtests.**

Economic indicators are constantly revised after their initial release. For example, GDP might be first reported as +2.0% growth, then revised to +1.8% a month later, and revised again to +2.1% in an annual revision. Using the final revised numbers in your backtest means you're making decisions with information you didn't have at the time.

**How we solve this:**
- Data uses **publication dates as the index**, not economic period dates
- You see what was actually known when you would have made trading decisions
- Revisions are tracked so you can see how data evolved over time
- This ensures your strategy performance reflects reality, not hindsight

For systematic traders, this means your backtests won't show artificially inflated returns from "perfect" economic data that didn't exist during your backtest period.

---

## Inflation Metrics

### Consumer Price Index (CPI)

**What It Is**: Measures the average change in prices paid by urban consumers for a basket of goods and services.

**Update Frequency**: Monthly (released mid-month for previous month)

**History**: 60+ years of data

**Key Variants**:
- **CPI (All Items)** - Headline inflation including all categories
- **Core CPI** - Excludes volatile food and energy prices (preferred by Fed)

**Use Cases**:
- Calculate real (inflation-adjusted) returns
- Identify inflation regime changes
- Build inflation factor models
- Adjust historical prices to current dollars

### Personal Consumption Expenditures (PCE)

**What It Is**: Measures price changes in goods and services consumed by households. The Federal Reserve's preferred inflation gauge.

**Update Frequency**: Monthly

**History**: 60+ years

**Key Variants**:
- **PCE (All Items)** - Headline PCE inflation
- **Core PCE** - Excludes food and energy (Fed's 2% target references this)

**Why It Matters**: The Fed uses Core PCE for monetary policy decisions, making it more market-relevant than CPI for anticipating rate changes.

**Use Cases**:
- Predict Federal Reserve policy decisions
- Assess whether economy is above/below Fed's 2% target
- Build interest rate forecasting models

---

## Interest Rates

### Federal Funds Rate

**What It Is**: The interest rate at which banks lend reserves to each other overnight. The Fed's primary monetary policy tool.

**Update Frequency**: Daily (effective rate)

**History**: 60+ years

**Typical Range**: 0% to 6% (varies with economic conditions)

**Use Cases**:
- Identify monetary policy regimes (easing vs. tightening)
- Model equity risk premiums
- Understand funding costs for leveraged strategies
- Predict market regime changes

### Treasury Yields

**What They Are**: Interest rates on U.S. government debt across different maturities.

**Update Frequency**: Daily (market rates)

**Available Maturities**:
- **3-Month Treasury** - Short-term risk-free rate
- **2-Year Treasury** - Sensitive to near-term Fed policy
- **5-Year Treasury** - Medium-term rate
- **10-Year Treasury** - Benchmark long-term rate
- **30-Year Treasury** - Long-duration rate

**History**: 30+ years for most maturities

### Key Fields

| Field | Description |
|-------|-------------|
| **date** | Observation date |
| **value** | Yield in percent (e.g., 4.25 = 4.25%) |

### Use Cases

**Risk-Free Rate for Sharpe Ratio**
- Use 10-year Treasury as risk-free benchmark
- Calculate excess returns over Treasury rates
- Adjust for changing rate environments

**Yield Curve Analysis**
- Compare 2-year vs. 10-year for curve shape
- Identify inversions (recession signal)
- Track term premium changes

**Interest Rate Factor Models**
- Model portfolio sensitivity to rate changes
- Hedge duration risk
- Build rate-sensitive trading strategies

**Discount Rate for Valuation**
- Use appropriate maturity for cash flow discounting
- Adjust valuation models for rate environment
- Calculate duration and convexity

---

## Economic Activity

### Gross Domestic Product (GDP)

**What It Is**: Total value of all goods and services produced in the U.S. economy (in real, inflation-adjusted terms).

**Update Frequency**: Quarterly (released ~1 month after quarter end)

**History**: 70+ years

**Units**: Billions of chained 2017 dollars

**Growth Calculation**: Quarter-over-quarter change (annualized) is widely reported

**Use Cases**:
- Identify recession vs. expansion periods
- Build business cycle indicators
- Predict earnings growth at market level
- Time cyclical vs. defensive positioning

### Industrial Production Index

**What It Is**: Measures output of factories, mines, and utilities in the U.S.

**Update Frequency**: Monthly

**History**: 100+ years

**Units**: Index (2017 = 100)

**Use Cases**:
- Leading indicator for manufacturing sector
- Track production cycles in real-time
- Identify sector-specific trends
- Predict commodity demand

### Retail Sales

**What It Is**: Total receipts of retail stores, measuring consumer spending.

**Update Frequency**: Monthly

**History**: 30+ years

**Units**: Millions of dollars

**Use Cases**:
- Gauge consumer spending strength
- Predict consumer discretionary stock performance
- Identify seasonal patterns
- Early indicator of GDP (consumption is 70% of GDP)

---

## Labor Market

### Unemployment Rate

**What It Is**: Percentage of labor force that is unemployed and actively seeking work.

**Update Frequency**: Monthly (released first Friday of month)

**History**: 70+ years

**Typical Range**: 3% to 10% (varies with business cycle)

**Use Cases**:
- Identify economic expansions vs. recessions
- Predict Fed policy (unemployment is half of Fed's dual mandate)
- Time cyclical sector exposure
- Build sentiment indicators

### Initial Jobless Claims

**What It Is**: Number of people filing for unemployment benefits for the first time (weekly).

**Update Frequency**: Weekly (every Thursday for previous week)

**History**: 50+ years

**Units**: Thousands of people

**Why It Matters**: Most timely indicator of labor market health (updated weekly vs. monthly jobs report).

**Use Cases**:
- Early warning of labor market deterioration
- Predict monthly unemployment rate
- Recession indicator (sharp spikes signal weakness)
- High-frequency economic tracking

### Nonfarm Payrolls

**What It Is**: Total number of paid U.S. workers excluding farm, government, and non-profit employees.

**Update Frequency**: Monthly (released first Friday of month, same time as unemployment)

**History**: 80+ years

**Units**: Thousands of employees

**Use Cases**:
- Track job growth trends
- Major market-moving indicator (released monthly)
- Predict consumer spending capacity
- Gauge tightness of labor market

---

## Market Indicators

### S&P 500 Index

**What It Is**: Market-cap-weighted index of 500 large-cap U.S. stocks.

**Update Frequency**: Daily (closing value)

**History**: 90+ years (dating back to 1920s)

**Use Cases**:
- Benchmark for U.S. equity performance
- Calculate beta relative to market
- Track broad market trends
- Build market timing models

### VIX (Volatility Index)

**What It Is**: Implied volatility of S&P 500 options, measuring expected market volatility over next 30 days.

**Update Frequency**: Daily

**History**: 30+ years

**Typical Range**: 10 to 80 (higher = more fear/uncertainty)

**Interpretation**:
- **Low VIX (10-15)**: Complacency, low expected volatility
- **Normal VIX (15-20)**: Average market conditions
- **High VIX (20-30)**: Elevated uncertainty
- **Extreme VIX (30+)**: Market stress, panic

**Use Cases**:
- Risk-on vs. risk-off regime identification
- Position sizing (reduce size when VIX spikes)
- Contrarian indicator (extreme VIX = opportunity)
- Volatility trading strategies

---

## Consumer & Housing

### Consumer Sentiment

**What It Is**: University of Michigan Consumer Sentiment Index, measuring consumer confidence.

**Update Frequency**: Monthly (preliminary and final releases)

**History**: 70+ years

**Units**: Index (1966 = 100)

**Use Cases**:
- Predict consumer spending behavior
- Early indicator of economic turning points
- Sentiment-based trading signals
- Risk appetite gauge

### Housing Starts

**What It Is**: Number of new residential construction projects begun during the month.

**Update Frequency**: Monthly

**History**: 60+ years

**Units**: Thousands of housing units (annualized)

**Use Cases**:
- Leading indicator for economy (housing is cyclical)
- Predict construction sector performance
- Gauge residential real estate market health
- Leading indicator for furniture, appliance demand

---

## Monetary Aggregates

### M2 Money Supply

**What It Is**: Total amount of money in circulation including cash, checking deposits, savings deposits, and money market funds.

**Update Frequency**: Weekly

**History**: 60+ years

**Units**: Billions of dollars

**Use Cases**:
- Track money supply growth (inflation signal)
- Understand liquidity conditions
- Quantitative easing / tightening assessment
- Build monetary models

---

## Data Characteristics

### Update Schedules

Economic data releases follow regular schedules:

| Indicator | Frequency | Release Timing | Market Impact |
|-----------|-----------|----------------|---------------|
| **Jobless Claims** | Weekly | Thursday 8:30 AM ET | Moderate |
| **Nonfarm Payrolls** | Monthly | First Friday 8:30 AM ET | High |
| **CPI / PCE** | Monthly | Mid-month 8:30 AM ET | High |
| **GDP** | Quarterly | ~1 month after quarter | Moderate |
| **Fed Funds** | Daily | Continuous | Low (already priced) |
| **Treasury Yields** | Daily | Continuous | Moderate |

### Revisions

Many economic indicators are revised after initial release:
- **GDP**: Three releases per quarter (advance, preliminary, final)
- **Employment**: Revised for prior 2 months each release
- **CPI/PCE**: Minor revisions occasionally

**Important**: For backtesting accuracy, use historical values as they were originally published, not revised data. This avoids look-ahead bias.

### Seasonal Adjustments

Most indicators are seasonally adjusted (SA) to remove predictable seasonal patterns:
- **Use SA data** for trend analysis and strategy signals
- **NSA (not seasonally adjusted)** data also available for specialized analysis

---

## Building Macro Factor Models

### Common Factor Approaches

**Inflation Factor**:
- Use Core CPI or Core PCE year-over-year change
- Rising inflation = pressure on margins, Fed tightening risk

**Interest Rate Factor**:
- Use 10-Year Treasury yield or change in Fed Funds rate
- Rising rates = headwind for growth stocks, positive for financials

**Economic Growth Factor**:
- Use GDP growth rate or Industrial Production change
- Strong growth = cyclicals outperform

**Unemployment Factor**:
- Use unemployment rate level or jobless claims trend
- Rising unemployment = recession risk, defensive positioning

### Regime Identification

Use economic data to identify market regimes:

**Expansion**: Low unemployment + positive GDP growth + moderate inflation
- Favor: Cyclicals, small caps, value stocks

**Late Cycle**: Low unemployment + high inflation + Fed tightening
- Favor: Commodities, defensive sectors

**Recession**: Rising unemployment + negative GDP + Fed easing
- Favor: Treasuries, consumer staples, utilities

**Recovery**: Falling unemployment + GDP acceleration + low rates
- Favor: Cyclicals, financials, technology

---

## Practical Applications

### Risk-Free Rate Calculation

**For Sharpe Ratio**:
- Daily strategies: Use 3-Month Treasury yield
- Monthly+ strategies: Use 10-Year Treasury yield
- Convert annual yield to strategy period: daily yield = annual / 252

### Discount Rate Selection

**For DCF Models**:
- Start with 10-Year Treasury as base risk-free rate
- Add equity risk premium (typically 4-6%)
- Adjust for company-specific risk

### Inflation Adjustment

**Real Returns Calculation**:
```
Real Return = Nominal Return - Inflation Rate
```
Use appropriate inflation measure (CPI for consumer purchasing power, PCE for Fed policy context).

### Market Timing Signals

**Example Combinations**:
- **Risk-On**: VIX < 20 + unemployment falling + GDP positive
- **Risk-Off**: VIX > 30 + unemployment rising + yield curve inverted
- **Fed Tightening**: Core PCE > 2.5% + unemployment < 4%

---

## Data Quality

FRED data represents official U.S. government statistics:
- **Authoritative**: Data directly from Bureau of Labor Statistics, Bureau of Economic Analysis, Federal Reserve
- **Reliable**: Subject to rigorous collection and calculation standards
- **Consistent**: Long time series with documented methodologies
- **Revised**: Initial releases may be revised as more information becomes available

---

## Best Practices

### For Strategy Development

1. **Match Frequency**: Use monthly data for monthly strategies, daily rates for daily strategies
2. **Align Timing**: Account for release delays (data for "January" released mid-February)
3. **Avoid Look-Ahead**: Use data as it was available historically, not revised values
4. **Combine Indicators**: Single indicators can give false signals; use multiple confirmations

### For Risk Management

1. **Monitor Fed Policy**: Track Fed Funds and Core PCE (Fed's targets)
2. **Watch Curve**: 2y-10y yield curve inversion predicts recessions
3. **Track Volatility**: VIX spikes signal risk-off periods
4. **Multiple Horizons**: Use both leading (claims) and coincident (unemployment) indicators

### For Research

1. **Understand Units**: Verify whether data is in levels, rates, or index form
2. **Check Seasonality**: Most series are seasonally adjusted by default
3. **Long History**: FRED offers very long time series for robust analysis
4. **Document Sources**: Note the FRED series ID for reproducibility

---

## Integration with Other Datasets

Economic data enhances other analyses:

**+ Market Data**:
- Study stock returns during different inflation regimes
- Analyze sector rotation with economic cycle
- Backtest using risk-free rate from Treasuries

**+ Corporate Data**:
- Adjust earnings for inflation
- Model revenue growth vs. GDP growth
- Predict margin pressure from input cost inflation

**+ Alternative Data**:
- Compare insider buying during high vs. low VIX
- Analyze hedge fund positioning during different regimes
- Correlate short interest with economic uncertainty

---

## Next Steps

- **[Market Data](market-data.md)** - Combine economic factors with price data
- **[Corporate Data](corporate-data.md)** - Link macro conditions to fundamentals
- **[Alternative Data](alternative-data.md)** - Understand smart money behavior in different economic environments
