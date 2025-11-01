# Market Data

Market data provides the foundation for quantitative trading strategies, including historical prices, real-time trades, and bid-ask quotes across multiple asset classes.

---

## Overview

| Dataset | Frequency | Assets | Primary Use |
|---------|-----------|--------|-------------|
| **OHLCV Aggregates** | Daily, Minute | Stocks, Crypto, Forex | Backtesting, charting |
| **Tick Trades** | Real-time | Stocks, Crypto | Execution analysis, microstructure |
| **Quotes** | Real-time | Stocks, Forex | Spread analysis, liquidity |

**Coverage**: All U.S. listed equities, major cryptocurrencies, and forex pairs

**History**: Multi-year historical data available

---

## OHLCV Aggregates

**What It Is**: Historical price bars showing Open, High, Low, Close prices and Volume for a given time period.

**Available Frequencies**:
- **Daily bars** - End-of-day price summaries
- **Minute bars** - Intraday price action at minute resolution

### Key Fields

| Field | Description |
|-------|-------------|
| **timestamp** | Date/time of the bar (index) |
| **open** | Opening price for the period |
| **high** | Highest price during the period |
| **low** | Lowest price during the period |
| **close** | Closing price for the period |
| **volume** | Number of shares/units traded |
| **vwap** | Volume-weighted average price |
| **transactions** | Number of individual trades in the bar |

### Data Characteristics

**Update Schedule**:
- Daily bars available after market close (~4:00 PM ET)
- Minute bars available with minimal delay during trading hours

**Price Adjustments**:
- Can be adjusted for stock splits
- Can be adjusted for dividends
- Raw (unadjusted) data also available

**History Depth**:
- Multi-year history for daily data
- Extended intraday history available (subscription dependent)

### Asset Formats

**Stocks**: Use standard ticker symbols
- Example: `AAPL`, `MSFT`, `TSLA`

**Cryptocurrencies**: Prefix with `X:`
- Example: `X:BTCUSD`, `X:ETHUSD`

**Forex**: Prefix with `C:`
- Example: `C:EURUSD`, `C:GBPJPY`

### Use Cases

**Backtesting Trading Strategies**
- Query multi-year daily bars for systematic strategy development
- Include split and dividend adjustments for accurate total returns
- Use daily aggregates to ensure clean, duplicate-free data

**Technical Analysis**
- Calculate moving averages, RSI, MACD from OHLCV data
- Identify support/resistance levels using high/low prices
- Analyze volume patterns and trends

**Price Discovery**
- Compare daily close prices across assets
- Identify gaps between close and next open
- Track volatility using high-low ranges

**Intraday Analysis**
- Use minute bars for intraday pattern recognition
- Analyze opening and closing auction behavior
- Study time-of-day volume patterns

---

## Tick Trades

**What It Is**: Individual trade executions showing every transaction that occurred on an exchange.

### Key Fields

| Field | Description |
|-------|-------------|
| **timestamp** | Exact time of trade execution |
| **price** | Price at which trade executed |
| **size** | Number of shares/units traded |
| **exchange** | Exchange where trade occurred |

### Data Characteristics

**Update Schedule**: Real-time during market hours with minimal latency

**Granularity**: Most granular level of market data available

**History Depth**: Extensive tick history available for analysis

**Data Volume**: Very large datasets - use date filtering for specific analysis periods

### Use Cases

**Execution Analysis**
- Analyze your fills vs. market trades
- Study execution quality and slippage
- Benchmark against VWAP

**Market Microstructure**
- Research price formation mechanisms
- Study trade size distributions
- Analyze inter-trade time patterns

**High-Frequency Strategy Development**
- Simulate tick-by-tick strategy execution
- Backtest ultra-short-term strategies
- Analyze order flow dynamics

**Volume Profile Analysis**
- Build price-volume histograms
- Identify high-volume price nodes
- Study volume-weighted price levels

---

## Quotes

**What It Is**: National Best Bid and Offer (NBBO) showing the best available buy and sell prices across all exchanges.

### Key Fields

| Field | Description |
|-------|-------------|
| **timestamp** | Time of quote update |
| **bid_price** | Best available buy price |
| **ask_price** | Best available sell price |
| **bid_size** | Quantity available at bid |
| **ask_size** | Quantity available at ask |
| **bid_exchange** | Exchange with best bid |
| **ask_exchange** | Exchange with best ask |

### Data Characteristics

**Update Schedule**: Real-time quote updates as market conditions change

**Quote Type**: National Best Bid Offer (NBBO) for stocks

**History Depth**: Historical quote data available for analysis

### Calculated Metrics

**Spread**: `ask_price - bid_price`
- Measures transaction cost
- Indicator of liquidity

**Mid Price**: `(bid_price + ask_price) / 2`
- Fair value estimate
- Used in theoretical pricing

### Use Cases

**Spread Analysis**
- Measure liquidity across different stocks
- Identify times of day with best/worst spreads
- Estimate transaction costs before trading

**Liquidity Research**
- Study depth at best bid/ask
- Compare liquidity across similar securities
- Identify liquid vs. illiquid periods

**Market Making Strategy Development**
- Analyze bid-ask dynamics
- Study quote update frequency
- Model inventory risk

**Fair Value Estimation**
- Use mid-price for mark-to-market
- Calculate theoretical prices between trades
- Estimate entry/exit prices

**Order Type Selection**
- Determine when to use limit vs. market orders
- Estimate probability of limit order execution
- Optimize limit order placement

---

## Data Quality

All market data undergoes quality checks:

- **Completeness**: Continuous time series without gaps
- **Accuracy**: Exchange-grade data from official sources
- **Adjustments**: Transparent handling of corporate actions
- **Timestamps**: Precise timing for event sequencing

---

## Practical Considerations

### Choosing the Right Dataset

**Use OHLCV Aggregates When**:
- Developing daily or swing trading strategies
- Need clean, manageable dataset sizes
- Performing long-term backtests (multi-year)
- Calculating standard technical indicators

**Use Tick Trades When**:
- Analyzing execution quality
- Researching market microstructure
- Need most granular view of trading activity
- Studying order flow

**Use Quotes When**:
- Modeling transaction costs
- Researching liquidity patterns
- Developing market-making strategies
- Need bid-ask spread analysis

### Data Volume Management

**Daily Aggregates**: Small, efficient datasets suitable for multi-year queries

**Minute Aggregates**: Moderate size, good for intraday analysis over days/weeks

**Tick Data**: Very large datasets, query smaller date ranges (hours/days)

### Asset Class Differences

**Stocks**:
- Trade on major U.S. exchanges during market hours (9:30 AM - 4:00 PM ET)
- Subject to corporate actions (splits, dividends)
- Regulated markets with NBBO requirements

**Cryptocurrencies**:
- Trade 24/7 across multiple exchanges
- High volatility, especially for smaller coins
- No corporate actions (splits/dividends)

**Forex**:
- Trade 24/5 (Sunday evening - Friday evening)
- Continuous market with no central exchange
- Influenced by global macro events

---

## Integration with Other Datasets

Market data becomes more powerful when combined with other datasets:

**+ Corporate Data**:
- Adjust prices for dividends and splits
- Calculate total returns accurately
- Align prices with earnings dates

**+ Alternative Data**:
- Overlay insider buying with price moves
- Compare institutional holdings to price trends
- Study price reaction to 13F filing dates

**+ Economic Data**:
- Analyze market behavior during economic releases
- Study correlation with interest rate changes
- Build macro factor models

---

## Best Practices

### For Backtesting

1. **Use Daily Aggregates**: More stable and easier to work with for most strategies
2. **Include Adjustments**: Always account for splits and dividends in total return calculations
3. **Avoid Duplicates**: Use daily aggregation to ensure one bar per day
4. **Survive Bias**: Include delisted stocks if possible (avoid survivorship bias)

### For Live Trading

1. **Start with Aggregates**: Use daily/minute bars for most use cases
2. **Monitor Spreads**: Check quote data before executing large orders
3. **Validate Prices**: Cross-reference trade prices with quotes
4. **Respect Latency**: Real-time data has minimal but non-zero delay

### For Research

1. **Sample First**: Query small date ranges to understand data structure
2. **Filter by Volume**: Focus on liquid securities for reliable results
3. **Check for Gaps**: Verify continuous data during expected trading hours
4. **Document Assumptions**: Note adjustment settings and data sources

---

## Next Steps

- **[Economic Data](economic-data.md)** - Add macro factors to your market analysis
- **[Corporate Data](corporate-data.md)** - Incorporate fundamental data
- **[Alternative Data](alternative-data.md)** - Enhance with sentiment and positioning data
