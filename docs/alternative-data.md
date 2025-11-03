---
page_type: reference
layout: default
order: 4
category: Datasets
description: Institutional holdings, insider trading, short interest, news
parent: ./index.md
---

# Alternative Data

Smart money signals - institutional holdings (13F), insider transactions, short interest, and financial news. Track positioning and sentiment.

---

## Datasets

:::grid
[
  {
    "title": "Institutional Holdings (13F)",
    "description": "Quarterly filings from $100M+ managers. Track elite fund positions.",
    "category": "Smart Money",
    "coverage": "10+ years",
    "frequencies": "Quarterly (45-day filing lag)"
  },
  {
    "title": "Insider Trading",
    "description": "Officer, director, and major shareholder transactions. Sentiment signals.",
    "category": "Corporate Insiders",
    "coverage": "20+ years",
    "frequencies": "Event-driven (2-day lag)"
  },
  {
    "title": "Short Interest",
    "description": "Daily short volume and bi-weekly settlement data. Crowding detection.",
    "category": "Market Positioning",
    "coverage": "Multi-year",
    "frequencies": "Daily and bi-weekly"
  },
  {
    "title": "Financial News",
    "description": "Corporate news and press releases. Event detection and sentiment.",
    "category": "Information Flow",
    "coverage": "Multi-year",
    "frequencies": "Real-time"
  }
]
:::

---

## Overview

| Dataset | Update Frequency | Coverage | Primary Use |
|---------|------------------|----------|-------------|
| **Institutional Holdings (13F)** | Quarterly | $100M+ AUM managers | Smart money tracking, ownership analysis |
| **Insider Trading** | Event-driven (2-day lag) | All public companies | Sentiment signals, insider confidence |
| **Short Interest** | Daily / Bi-weekly | U.S. stocks | Crowding detection, squeeze potential |
| **Financial News** | Real-time | Major companies | Sentiment analysis, event detection |

**Coverage**: U.S. publicly traded securities

**History**: 10-20+ years depending on dataset

---

## Designed for Realistic Backtesting

:::warning Disclosure Timing
Alternative data indexed by **public disclosure** dates, not event dates.

- **13F**: Use `filed_at` (not `period_of_report`) - 45-day lag is material
- **Insider trades**: Use `filed_at` (not `transaction_date`) - 2-day disclosure
- **Short interest**: Bi-weekly settlement snapshots, not real-time

Prevents front-running disclosures with information you couldn't have known.
:::

---

## Institutional Holdings (Form 13F)

**What It Is**: Quarterly disclosure of equity holdings by institutional investment managers with over $100 million in assets under management. This includes hedge funds, mutual funds, pension funds, and other large investors.

### Regulatory Background

- **Required by**: Securities and Exchange Commission (SEC)
- **Filing deadline**: 45 days after quarter end
- **Who must file**: Institutions managing $100M+ in "13F securities" (stocks, options, convertible bonds)
- **What's disclosed**: Long positions only (shorts not reported)

### Key Fields

| Field | Description |
|-------|-------------|
| **filed_at** | Date the 13F was filed (**use this for backtesting**) |
| **period_of_report** | Quarter end date (3/31, 6/30, 9/30, 12/31) |
| **cik** | Central Index Key - unique identifier for institution |
| **institution_name** | Name of the filing institution |
| **ticker** | Stock ticker symbol |
| **cusip** | Security identifier (9-character) |
| **company_name** | Name of the company held |
| **shares** | Number of shares held |
| **value** | Market value of position in dollars |
| **investment_discretion** | SOLE, SHARED, or OTHER |

### Data Characteristics

**Update Frequency**: Quarterly

**Filing Lag**: 45 days after quarter end (e.g., Q1 data filed by mid-May)

**History**: 10+ years of filings

**Coverage**:
- Thousands of institutional managers
- All U.S. listed equities held by these institutions

### Notable Institutions (by CIK)

| Institution | CIK | Type |
|------------|-----|------|
| **Berkshire Hathaway** | 1067983 | Conglomerate / Value investor |
| **Citadel Advisors** | 1324404 | Multi-strategy hedge fund |
| **Tiger Global Management** | 1649339 | Growth-focused hedge fund |
| **Renaissance Technologies** | 1037389 | Quantitative hedge fund |
| **Bridgewater Associates** | 1350694 | Macro hedge fund |

### Use Cases

- **Smart Money Tracking**: Monitor elite fund holdings, identify new positions before widespread knowledge
- **Crowding Analysis**: Detect concentrated ownership, identify "hedge fund hotels" at risk of unwinding
- **Ownership Changes**: Track quarter-over-quarter changes (new positions, exits, additions vs. distributions)
- **Conviction Analysis**: Identify largest positions (>10% of portfolio) as high-conviction bets
- **Screening**: Find stocks bought by 10+ top funds, detect smart money divergence from retail

### Query Patterns

- **By Ticker**: View all institutions holding a stock, calculate total ownership, track changes over time
- **By Institution (CIK)**: View complete portfolio, compare quarter-over-quarter, identify new positions and exits
- **Large Positions**: Query positions >10% of portfolio or >$1B in size (conviction bets)

### Important Considerations

:::tip Critical Date Field
**Always use `filed_at`** for backtesting, not `period_of_report`.

Q1 position (3/31) not public until filing (5/15). Using `period_of_report` = 45 days of future knowledge.
:::

**Longs Only**:
- 13F only reports long positions
- Short positions are not disclosed
- May overstate net positioning for hedge funds

**Delayed Information**:
- Up to 45-day lag between quarter end and filing
- Institutions may have already changed positions
- Best used for medium-term trends, not day-trading

**Aggregated Holdings**:
- Multiple accounts of same manager aggregated
- Can't distinguish between different strategies
- Represents firm-wide positioning

---

## Insider Trading

**What It Is**: Transactions in company stock by corporate insiders - officers, directors, and beneficial owners (10%+ shareholders). Filed with SEC via Forms 3, 4, 5, and 144.

### Regulatory Background

- **Required by**: Securities and Exchange Commission (SEC)
- **Form 4 deadline**: 2 business days after transaction
- **Who must file**: Officers, directors, 10%+ shareholders
- **Transactions covered**: Purchases, sales, option exercises, grants, awards

### Key Fields

| Field | Description |
|-------|-------------|
| **filed_at** | When filing was submitted (index) |
| **transaction_date** | When the trade actually occurred |
| **ticker** | Stock ticker symbol |
| **company_name** | Company name |
| **insider_name** | Name of insider (officer/director) |
| **insider_title** | Position (CEO, CFO, Director, etc.) |
| **transaction_code** | Type of transaction (P, S, A, M, etc.) |
| **shares** | Number of shares transacted |
| **price** | Price per share |
| **shares_owned_after** | Total shares owned after transaction |
| **ownership_type** | Direct or indirect ownership |

### Transaction Codes

| Code | Meaning | Interpretation |
|------|---------|----------------|
| **P** | Open market purchase | **Bullish signal** - insider buying with own money |
| **S** | Open market sale | Neutral/bearish - could be diversification |
| **A** | Award/grant | Compensation - not meaningful signal |
| **M** | Exercise of options | Neutral - converting options to stock |
| **G** | Gift | Neutral - charitable/estate planning |
| **J** | Other acquisition | Various - check details |
| **F** | Tax withholding | Neutral - automatic tax payment |

### Data Characteristics

**Update Frequency**: Event-driven

**Filing Lag**: 2 business days (Form 4)

**History**: 20+ years of insider transactions

**Coverage**: All U.S. public companies

### Use Cases

- **Bullish Signals**: Multiple insiders buying (especially CEO/CFO), cluster buying (3+ in 30 days), purchases near 52-week lows
- **Red Flags**: Heavy executive selling, multiple simultaneous sellers, sales near all-time highs
- **Signal Development**: Weight by purchase size, insider role, and timing; combine with price action for confirmation
- **Research**: Analyze insider return predictability, sector differences, compare to institutional flows

### Interpretation Guidelines

:::tip Insider Buying Signal
Insiders buy for **one reason** (expect gains). They sell for **many reasons** (diversification, taxes, liquidity).

Focus on purchases, especially clusters of 3+ insiders buying within 30 days.
:::

| Factor | Interpretation |
|--------|----------------|
| **Buying > Selling** | Insiders buy for one reason (expect gains); sell for many (diversification, taxes, etc.) |
| **Size Matters** | Compare to compensation and holdings ($10k by $5M CEO = noise; $1M = meaningful) |
| **Timing Context** | Buying after bad news or at multi-year lows = strong confidence; selling at highs = less concerning |
| **Position Matters** | CEO/CFO = best informed; Directors = outside perspective; 10%+ owners = different motivations |

---

## Short Interest

**What It Is**: Data on short selling activity, showing how many shares are sold short and the volume of short selling.

### Two Types of Short Data

1. **Short Volume** (Daily)
2. **Short Interest** (Bi-weekly settlements)

---

### Short Volume (Daily)

**What It Is**: Daily volume of shares sold short across different exchanges.

#### Key Fields

| Field | Description |
|-------|-------------|
| **date** | Trading date |
| **ticker** | Stock ticker symbol |
| **total_volume** | Total shares traded |
| **short_volume** | Shares sold short |
| **short_volume_ratio** | Short volume / total volume (%) |
| **market_volume** | Volume by exchange (NYSE, NASDAQ, etc.) |

#### Data Characteristics

**Update Frequency**: Daily (published next day)

**Coverage**: All U.S. listed stocks

**History**: Multi-year daily data

#### Use Cases

- **Short-Term Sentiment**: Track daily shorting intensity (>50% = heavy pressure), compare to historical averages
- **Relative Analysis**: Compare to stock's typical ratio, identify most-shorted stocks by sector
- **Trading Strategy**: Fade extreme short volume (contrarian), avoid persistently high levels, combine with price action

---

### Short Interest (Bi-weekly)

**What It Is**: Settlement-based count of total shares sold short, reported twice monthly.

#### Key Fields

| Field | Description |
|-------|-------------|
| **settlement_date** | Reporting date (twice monthly) |
| **ticker** | Stock ticker symbol |
| **short_interest** | Total shares sold short |
| **shares_outstanding** | Total shares outstanding |
| **average_daily_volume** | Recent average volume |
| **days_to_cover** | Short interest / avg volume |

#### Data Characteristics

**Update Frequency**: Twice monthly (mid-month and month-end)

**Coverage**: All U.S. listed stocks

**History**: Multi-year settlement data

**Publication Lag**: ~1 week after settlement date

#### Use Cases

- **Squeeze Potential**: Days to cover >10 + short interest >20% + catalyst = high squeeze risk
- **Crowding Indicator**: Extreme short interest signals crowded trade at risk of violent reversal
- **Sentiment Gauge**: Rising = growing bearishness; falling = covering (potential bottom)
- **Position Management**: Avoid heavily shorted for longs unless contrarian; track changes (adding vs. covering)

#### Key Metrics

**Short Interest Ratio** (Days to Cover):
```
Days to Cover = Short Interest / Average Daily Volume
```
- < 3 days: Low short interest
- 3-7 days: Moderate
- 7-10 days: High
- \> 10 days: Extreme (squeeze risk)

**Short % of Float**:
```
Short % = Short Interest / Shares Outstanding × 100%
```
- < 5%: Low
- 5-10%: Moderate
- 10-20%: High
- \> 20%: Extreme

---

## Financial News

**What It Is**: Corporate news articles and press releases with metadata for systematic analysis.

### Key Fields

| Field | Description |
|-------|-------------|
| **published_at** | Publication timestamp |
| **title** | Headline |
| **description** | Summary/snippet |
| **content** | Full article text |
| **author** | Article author |
| **publisher** | News source |
| **tickers** | Related stock tickers |
| **url** | Article URL |

### Data Characteristics

**Update Frequency**: Real-time

**Coverage**: Major U.S. companies and markets

**History**: Multi-year news archive

**Sources**: Major financial news publishers

### Use Cases

- **Event Detection**: Identify earnings, M&A, product launches, regulatory actions, management changes
- **Sentiment Analysis**: Extract sentiment via NLP, track changes over time, compare to price action
- **Thematic Investing**: Search keywords (AI, renewable energy), track narrative trends, identify emerging themes
- **Trading Strategies**: Trade news surprises, fade overreactions, detect information leakage (price moves before news)

### Processing News Data

- **Structured Metadata**: Timestamp for event studies, ticker linking, publisher quality filtering
- **Text Analysis**: Sentiment extraction, entity recognition, topic classification (earnings, M&A, legal)
- **Signal Generation**: News surprise, volume spikes, sentiment shifts, cross-asset impact

---

## Data Quality & Timing

### 13F Holdings

**Timing**: Use `filed_at` date, not `period_of_report`
- Avoids look-ahead bias
- Reflects when information became public
- 45-day lag is material (positions may have changed)

**Completeness**: Only long positions, no shorts

**Accuracy**: Self-reported by institutions, audited by SEC

### Insider Trading

**Timing**: 2-day filing deadline mostly adhered to
- Occasionally late filings
- Use `filed_at` for data availability date
- `transaction_date` for actual trade

**Interpretation**: Context crucial
- Not all selling is bearish
- Cluster analysis more reliable than single trades

### Short Interest

**Settlement-Based**: Snapshot in time, not continuous
- Twice monthly = significant lag
- Positions change between reports
- Use for trends, not precise timing

**Daily Volume**: More timely but noisy
- Daily fluctuations normal
- Look at moving averages
- Combine with settlement data

### News

**Timeliness**: Real-time, but processing takes time
- Sentiment analysis requires NLP
- Human reading for nuance
- Automated extraction for scale

**Quality**: Varies by source
- Major publishers more reliable
- Press releases vs. journalism
- Verify with multiple sources

---

## Integration with Other Datasets

### + Market Data
- Study price reaction to insider buys
- Analyze short squeeze dynamics (short interest + price spike)
- Overlay 13F filing dates with price trends
- News-driven price moves

### + Corporate Data
- Compare insider buying to fundamentals (buying undervalued stocks?)
- 13F institutions prefer high-quality fundamentals
- Short interest highest for deteriorating fundamentals
- News about earnings, dividends, financial health

### + Economic Data
- Institutional flows during different macro regimes
- Insider buying increases near economic bottoms
- Short interest patterns in recessions vs. expansions
- News sentiment correlation with VIX, market stress

---

## Best Practices

**13F Analysis**: Use `filed_at` (avoid look-ahead bias), focus on changes vs. continuations, track proven investors, account for 45-day lag

**Insider Trading**: Prioritize purchases over sales, require meaningful size relative to compensation, look for clusters (3+ insiders), CEO/CFO most informed

**Short Interest**: High interest + catalyst = squeeze opportunity, track trends not just levels, combine daily volume with settlement data

**News**: Focus on reputable publishers, validate NLP sentiment (spot check), prioritize unique information over rehashed content

---

## Next Steps

- **[Market Data](market-data.md)** - Combine alternative data signals with price confirmation
- **[Corporate Data](corporate-data.md)** - Validate alternative data with fundamental quality
- **[Economic Data](economic-data.md)** - Understand macro context for positioning data
