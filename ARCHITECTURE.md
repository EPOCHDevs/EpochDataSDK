# EpochDataSDK Architecture

## Overview

The EpochDataSDK has been refactored to mirror the [polygon-sdk-python](https://github.com/polygon-io/client-python) architecture, providing a clean separation of concerns and flexible usage patterns.

## Architecture Pattern

### Design Goals

1. **Separation of Concerns**: Each client handles one specific data domain
2. **Factory Pattern**: Clean client creation without exposing base classes
3. **Composition over Inheritance**: PolygonClient composes specialized clients
4. **No Singleton**: Factory creates new instances (user controls lifecycle)
5. **Historical Data Focus**: Only backtest-relevant endpoints (no snapshots/reference data)

### Component Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                    User Application                          │
└──────────────────┬──────────────────────────────────────────┘
                   │
         ┌─────────┴──────────┐
         │                    │
    ┌────▼────────┐    ┌─────▼──────────┐
    │ PolygonClient│    │ ClientFactory  │
    │   (Facade)   │    │  (Factory)     │
    └────┬─────────┘    └─────┬──────────┘
         │                    │
         │  creates           │  creates
         ▼                    ▼
    ┌────────────────────────────────────┐
    │      Specialized Clients           │
    ├────────────────────────────────────┤
    │ • AggsClient                       │
    │ • QuotesClient                     │
    │ • TradesClient                     │
    │ • FinancialsClient                 │
    │ • SplitsClient                     │
    │ • DividendsClient                  │
    │ • NewsClient                       │
    └────────┬───────────────────────────┘
             │  inherits (private)
             ▼
    ┌────────────────┐
    │  BaseClient    │  (private, in src/)
    │  (HTTP core)   │
    └────────────────┘
```

## File Structure

```
include/epoch_data_sdk/polygon/
├── client_factory.hpp          # Factory methods
├── polygon_client.hpp          # Main facade (composition)
├── aggs_client.hpp             # OHLCV aggregates
├── quotes_client.hpp           # Historical quotes (NBBO)
├── trades_client.hpp           # Historical trades
├── financials_client.hpp       # Financial statements & ratios
├── splits_client.hpp           # Stock splits (CRITICAL)
├── dividends_client.hpp        # Dividends (CRITICAL)
├── news_client.hpp             # News articles (OPTIONAL)
├── options.hpp                 # Configuration
├── error.hpp                   # Error types
└── models.hpp                  # Data models

src/polygon/
├── base_client.hpp/cpp         # PRIVATE base class (not exposed)
├── client_factory.cpp          # Factory implementation
├── polygon_client.cpp          # Facade implementation
├── aggs_client.cpp             # Aggregates logic
├── quotes_client.cpp           # Quotes logic
├── trades_client.cpp           # Trades logic
├── financials_client.cpp       # Financials logic
├── splits_client.cpp           # Splits logic
├── dividends_client.cpp        # Dividends logic
└── news_client.cpp             # News logic
```

## Usage Patterns

### Pattern 1: Main Facade (Recommended for most users)

```cpp
#include <epoch_data_sdk/polygon/polygon_client.hpp>

using namespace data_sdk::polygon;

Options options;
options.api_key = "YOUR_API_KEY";

PolygonClient client(options);

// All methods available
auto bars = client.getAggregates("AAPL", "2024-01-01", "2024-01-05", true);
auto quotes = client.getQuotes("AAPL", "2024-01-01", "2024-01-05");
auto trades = client.getTrades("AAPL", "2024-01-01", "2024-01-05");
auto financials = client.getBalanceSheets(std::nullopt, std::nullopt, "AAPL");
auto splits = client.getSplits("AAPL");
auto dividends = client.getDividends("AAPL");
auto news = client.getNews("AAPL", std::nullopt, "2024-01-01", "2024-01-05");
```

### Pattern 2: Factory with Specialized Clients

```cpp
#include <epoch_data_sdk/polygon/client_factory.hpp>

using namespace data_sdk::polygon;

Options options;
options.api_key = "YOUR_API_KEY";

// Create only what you need
auto aggs = ClientFactory::createAggsClient(options);
auto quotes = ClientFactory::createQuotesClient(options);
auto splits = ClientFactory::createSplitsClient(options);
auto dividends = ClientFactory::createDividendsClient(options);

// Use specialized interfaces
auto bars = aggs->getAggregates("AAPL", "2024-01-01", "2024-01-05", true);
auto nbbo = quotes->getQuotes("AAPL", "2024-01-01", "2024-01-05");
auto stock_splits = splits->getSplits("AAPL");
auto divs = dividends->getDividends("AAPL");
```

## Client Responsibilities

### AggsClient
**Purpose**: OHLCV aggregate bars

**Endpoints**:
- `getAggregates()` - `/v2/aggs/ticker/{ticker}/range/...`
- `getGroupedDaily()` - `/v2/aggs/grouped/locale/.../market/...` (TODO)
- `getDailyOpenClose()` - `/v1/open-close/{ticker}/{date}` (TODO)
- `getPreviousClose()` - `/v2/aggs/ticker/{ticker}/prev` (TODO)

**Supports**: Stocks, Forex (C:), Crypto (X:), Indices (^)

**Features**:
- RTH filtering for stocks (09:31-16:00 ET)
- Automatic pagination
- Timezone conversion (ET → UTC for stocks, UTC for crypto)

### QuotesClient
**Purpose**: Historical NBBO quotes

**Endpoints**:
- `getQuotes()` - `/v3/quotes/{ticker}`

**Supports**: Stocks, Forex

**Features**:
- Bid/ask prices and sizes
- Exchange identifiers
- Nanosecond timestamps

### TradesClient
**Purpose**: Historical trade ticks

**Endpoints**:
- `getTrades()` - `/v3/trades/{ticker}`

**Supports**: Stocks, Crypto

**Features**:
- Price and size
- Exchange identifiers
- Tape and conditions

### FinancialsClient
**Purpose**: Financial statements and ratios

**Endpoints**:
- `getBalanceSheets()` - `/stocks/financials/v1/balance-sheets` (TODO)
- `getCashFlowStatements()` - `/stocks/financials/v1/cash-flow-statements` (TODO)
- `getIncomeStatements()` - `/stocks/financials/v1/income-statements` (TODO)
- `getFinancialRatios()` - `/stocks/financials/v1/ratios` (TODO)

**Supports**: Stocks only

**Note**: Stub implementations - need to be filled in from `polygon_client_old.cpp`

### SplitsClient
**Purpose**: Stock split data

**Endpoints**:
- `getSplits()` - `/v3/reference/splits`

**Supports**: Stocks only

**Features**:
- Split ratios (from/to)
- Execution dates
- Reverse split filtering
- **CRITICAL** for backtesting: adjust historical prices for splits

### DividendsClient
**Purpose**: Dividend payment data

**Endpoints**:
- `getDividends()` - `/v3/reference/dividends`

**Supports**: Stocks only

**Features**:
- Cash amounts
- Ex-dividend dates
- Declaration, record, and pay dates
- Dividend frequency (annual, quarterly, monthly)
- Dividend type filtering
- **CRITICAL** for backtesting: calculate total returns (price + dividends)

### NewsClient
**Purpose**: News article data

**Endpoints**:
- `getNews()` - `/v2/reference/news`

**Supports**: All asset types

**Features**:
- Article metadata (title, author, publisher)
- Published timestamps
- Ticker associations
- Article URLs
- **OPTIONAL** for backtesting: sentiment analysis, event-driven strategies

## Implementation Status

### ✅ Completed
- [x] BaseClient (private HTTP core)
- [x] ClientFactory (factory methods)
- [x] AggsClient interface + getAggregates() implementation
- [x] QuotesClient interface + getQuotes() implementation
- [x] TradesClient interface + getTrades() implementation
- [x] FinancialsClient interface (stub implementations)
- [x] SplitsClient interface + getSplits() implementation (CRITICAL)
- [x] DividendsClient interface + getDividends() implementation (CRITICAL)
- [x] NewsClient interface + getNews() implementation (OPTIONAL)
- [x] PolygonClient facade (composition pattern)
- [x] CMakeLists.txt updated
- [x] Example code

### 🔄 TODO
- [ ] Implement AggsClient: getGroupedDaily(), getDailyOpenClose(), getPreviousClose()
- [ ] Implement all FinancialsClient methods (extract from polygon_client_old.cpp)
- [ ] Update tests for new architecture
- [ ] Add pagination support to QuotesClient and TradesClient
- [ ] Optional: Create OptionsClient for options data

## Migration Guide

### For Existing Code

No changes needed! The `PolygonClient` interface remains the same:

```cpp
// Old code still works
PolygonClient client(options);
client.getAggregates("AAPL", "2024-01-01", "2024-01-05", true);
```

### For New Code

You can now use specialized clients:

```cpp
// New pattern
auto aggs = ClientFactory::createAggsClient(options);
aggs->getAggregates("AAPL", "2024-01-01", "2024-01-05", true);
```

## Design Principles

### 1. Encapsulation
- BaseClient is private (in `src/`, not `include/`)
- Specialized clients use pImpl pattern
- Users never see implementation details

### 2. Single Responsibility
- Each client handles one domain
- Clear separation: Aggs, Quotes, Trades, Financials
- Easy to test and maintain

### 3. Open/Closed Principle
- Easy to add new clients (e.g., OptionsClient)
- No modification of existing clients needed
- Factory pattern supports extension

### 4. Dependency Inversion
- Clients depend on abstract Options, not concrete types
- Error handling through Expected<T> pattern
- No circular dependencies

## Error Handling

All methods return `Expected<DataFrame>`:

```cpp
auto result = client.getAggregates("AAPL", "2024-01-01", "2024-01-05", true);

if (result) {
  // Success
  auto df = *result;
  // use df...
} else {
  // Error
  auto error = result.error();
  std::cerr << "Error: " << error.message << "\n";
  std::cerr << "HTTP Status: " << error.http_status << "\n";
}
```

## Testing Strategy

### Unit Tests
- Test each specialized client independently
- Mock BaseClient HTTP layer
- Verify DataFrame transformations

### Integration Tests
- Test ClientFactory creation
- Test PolygonClient delegation
- Verify backwards compatibility

### End-to-End Tests
- Test against real Polygon API (with rate limiting)
- Verify all asset types (stocks, forex, crypto)
- Test error scenarios

## Performance Considerations

1. **No Singleton**: Users control instance lifecycle
2. **Lazy Evaluation**: Clients created on demand
3. **Connection Pooling**: Drogon HTTP client reused
4. **Pagination**: Automatic handling with retry logic
5. **Event Loop Sharing**: Optional Drogon main loop integration

## Future Enhancements

1. **OptionsClient**: Add support for options data
2. **Async API**: Expose coroutine-based async methods
3. **Streaming**: WebSocket integration with specialized clients
4. **Caching**: Optional response caching layer
5. **Metrics**: Built-in performance tracking

## References

- [polygon-sdk-python](https://github.com/polygon-io/client-python) - Reference implementation
- [Polygon.io API Docs](https://polygon.io/docs) - API documentation
- [EpochFrame](https://github.com/your-org/epoch_frame) - DataFrame library
