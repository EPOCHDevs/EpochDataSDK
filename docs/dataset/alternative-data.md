# Alternative Data

Alternative data provides non-traditional signals for trading strategies, including institutional holdings, insider transactions, short interest metrics, and financial news. These datasets offer insights into smart money positioning, corporate insider sentiment, and market crowding.

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

**Alternative data is carefully indexed to reflect when information actually became public, not when events occurred.**

"Following the smart money" sounds great, but if your backtest uses quarter-end dates for 13F holdings, you're effectively front-running Buffett with 45 days of insider knowledge. Similarly, insider purchases are only reported within 2 business days - your backtest needs to respect that timing.

**How we solve this:**
- **13F data** is indexed by filing date, not quarter-end (institutions have 45 days to report)
- **Insider transactions** use the filing date (when you actually learned about the trade)
- **Dividends** use the ex-dividend date (the trading date that matters for returns)
- **Short interest** reflects the settlement schedule (bi-weekly snapshots, not continuous)

**What this means for your strategies:**
- "Copy the whales" strategies see holdings when they were disclosed, not when positions were established
- Insider buying signals appear when filings hit, not when insiders actually bought
- Dividend capture strategies use the correct dates for entry/exit timing

For alternative data strategies, this ensures you're not accidentally trading on information you couldn't have known, keeping your backtest results honest.

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

**"Follow the Smart Money"**
- Track what elite hedge funds are buying/selling
- Compare your holdings to successful investors
- Identify emerging positions before they become widely known

**Crowding Analysis**
- Find stocks with concentrated hedge fund ownership
- Identify "crowded trades" at risk of unwinding
- Spot hedge fund hotels (many funds in same stock)

**Ownership Tracking**
- Monitor institutional ownership percentage
- Track changes quarter-over-quarter (new positions, exits, additions)
- Identify accumulation vs. distribution patterns

**Position Analysis**
- Find an institution's largest positions (conviction bets)
- Calculate position size as % of institution's portfolio
- Track position changes (increasing vs. decreasing)

**Screening**
- Find stocks recently bought by 10+ top hedge funds
- Identify "smart money divergence" (insiders buying, institutions selling)
- Screen for institutional favorites in specific sectors

### Query Patterns

**By Ticker**: Who owns this stock?
- See all institutions holding AAPL
- Calculate total institutional ownership
- Track ownership changes over time

**By Institution (CIK)**: What does this fund own?
- View complete portfolio of Berkshire Hathaway
- Compare current vs. previous quarter
- Identify new positions and exits

**Large Positions**: Find conviction bets
- Query positions >10% of institution's portfolio
- Find positions >$1B in size
- Identify outsized bets

### Important Considerations

**Use `filed_at` for Backtesting**:
- The `period_of_report` (e.g., 3/31) is the snapshot date
- But data wasn't public until `filed_at` (e.g., 5/15)
- Using `period_of_report` creates look-ahead bias

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

**Insider Buying as Signal**
- **Strong signal**: Multiple insiders buying (especially executives)
- **Strongest**: CEO/CFO buying large amounts
- Focus on open market purchases (code = P)
- Cluster buying = high conviction

**Identifying Confidence**
- Large purchases relative to insider's salary
- Purchases near 52-week lows (buying the dip)
- Insider buying during negative news
- Board members buying (outsider perspective)

**Red Flags**
- Heavy insider selling (especially executives)
- Selling by multiple insiders simultaneously
- Sales near highs (perfect timing = suspicious)
- 10%+ owner distributions

**Strategy Development**
- Build "insider buying" signal
- Weight by size, role, timing
- Combine with price action (insider buy + price strength)
- Screen for clusters (3+ insiders in 30 days)

**Research & Studies**
- Analyze insider returns (buying before runups)
- Study regulatory effectiveness (2-day reporting)
- Sector differences (tech vs. value)
- Compare insider vs. institutional flows

### Interpretation Guidelines

**Buying > Selling**:
- Insiders buy for one reason: they think stock will go up
- Insiders sell for many reasons: diversification, taxes, house, divorce, etc.
- Buying is a stronger signal than selling

**Size Matters**:
- $10k purchase by CEO earning $5M = not meaningful
- $1M purchase by CEO = very meaningful
- Compare to insider's compensation and existing holdings

**Timing Context**:
- Buying after bad news = confidence
- Buying at multi-year lows = value signal
- Selling at all-time highs = less concerning
- Selling before earnings = potential red flag

**Position Matters**:
- CEO/CFO = best information
- Directors = good outside perspective
- 10%+ owners = different motivations (activist, financial investor)
- Lower-level officers = less informative

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

**Short-Term Sentiment**
- High short volume ratio (>50%) = heavy shorting pressure
- Track daily shorting intensity
- Compare to historical averages

**Relative Analysis**
- Compare short volume to stock's typical ratio
- Sector comparison (which stocks getting shorted most)
- Time-series patterns (increasing shorting)

**Trading Strategy**
- Fade extreme short volume (contrarian)
- Avoid stocks with persistently high short volume
- Combine with price action (short volume + down day = pressure)

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

**Short Squeeze Potential**
- **Days to Cover > 10**: High squeeze risk
- **Short Interest > 20% of float**: Heavily shorted
- Combine with catalysts (earnings beat, insider buying, positive news)

**Crowding Indicator**
- Very high short interest = crowded short
- Risk of violent squeeze if sentiment changes
- Compare to peers (relatively over-shorted?)

**Sentiment Gauge**
- Rising short interest = growing bearishness
- Falling short interest = shorts covering (could be bottom)
- Extreme short interest = potential contrarian signal

**Position Management**
- Avoid heavily shorted stocks for long positions (unless contrarian)
- High short interest = potential borrowing costs
- Track changes (shorts adding vs. covering)

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

**Event Detection**
- Identify earnings announcements
- Track M&A announcements
- Find product launches, regulatory actions
- Detect management changes

**Sentiment Analysis**
- Use NLP to extract sentiment (positive/negative/neutral)
- Track sentiment changes over time
- Compare news sentiment to price action
- Build news-based trading signals

**Thematic Investing**
- Search for keywords (AI, renewable energy, biotech)
- Track narrative trends
- Identify emerging themes
- Find companies mentioned in context

**Fundamental Research**
- Read company-specific news
- Track industry developments
- Monitor competitors
- Understand context behind price moves

**News-Based Strategies**
- Trade on news surprises
- Fade overreactions
- Follow momentum from positive news
- Detect information leakage (price moves before news)

### Processing News Data

**Structured Metadata**:
- Timestamp for event studies
- Ticker linking for portfolio context
- Publisher for source quality filtering

**Text Analysis**:
- Headline sentiment (positive/negative words)
- Entity recognition (companies, people, products)
- Topic classification (earnings, M&A, legal, etc.)
- Comparison to historical news patterns

**Signal Generation**:
- News surprise (unexpected news)
- News volume (increased coverage)
- Sentiment shift (negative to positive)
- Cross-asset impact (news on supplier affects company)

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

### For 13F Analysis

1. **Use Filing Date**: Always use `filed_at` to avoid look-ahead bias
2. **Focus on Changes**: New positions and big increases more meaningful than continuations
3. **Quality over Quantity**: Track proven investors, not all filers
4. **Lag Awareness**: 45-day delay limits actionability
5. **Aggregate View**: Look at multiple institutions for conviction

### For Insider Trading

1. **Buys > Sells**: Prioritize purchase analysis
2. **Size Matters**: Large relative purchases most meaningful
3. **Clusters**: Multiple insiders buying = stronger signal
4. **Exclude Routine**: Filter out automatic 10b5-1 plans if possible
5. **Title Matters**: CEO/CFO most informed

### For Short Interest

1. **Context Required**: High short interest + catalyst = squeeze opportunity
2. **Trend > Level**: Rising short interest more important than absolute level
3. **Compare to History**: Stock's own history, sector peers
4. **Combine Metrics**: Use both daily volume and settlement data
5. **Beware Crowding**: Extreme shorts = risky (either direction)

### For News

1. **Filter Quality**: Focus on reputable publishers
2. **Timestamp Precision**: Critical for event studies
3. **Validate NLP**: Sentiment analysis not perfect - spot check
4. **Unique Info**: Focus on new information, not rehashed
5. **Scale + Depth**: Automate extraction, read manually for nuance

---

## Common Strategies

### Smart Money Following
1. Identify proven investors (Berkshire, quality funds)
2. Track new 13F positions within days of filing
3. Enter positions with small size
4. Hold for medium term (quarters)

### Insider Cluster Signal
1. Screen for 3+ insider purchases in 30 days
2. Filter for P (purchase) transactions only
3. Require total purchases >$500k
4. Combine with technical setup (oversold, support)

### Short Squeeze Candidate
1. Short interest >20% of float
2. Days to cover >7
3. Positive catalyst (earnings beat, insider buy, upgrade)
4. Recent bottom in price

### News Momentum
1. Detect unusually high news volume for stock
2. Extract sentiment (positive/negative)
3. Enter on positive surprise with follow-through
4. Exit if sentiment reverses or volume normalizes

---

## Next Steps

- **[Market Data](market-data.md)** - Combine alternative data signals with price confirmation
- **[Corporate Data](corporate-data.md)** - Validate alternative data with fundamental quality
- **[Economic Data](economic-data.md)** - Understand macro context for positioning data
