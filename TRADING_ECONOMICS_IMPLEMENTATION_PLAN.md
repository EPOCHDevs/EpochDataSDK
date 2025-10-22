# Trading Economics SDK Implementation Plan

## Summary

Created comprehensive skeleton code for **9 Trading Economics clients** with **57 total methods**.

## Phase 1 COMPLETED: Skeleton Headers ✅

All skeleton headers created in `/include/epoch_data_sdk/trading_economics/`:

### Core Infrastructure (2 files)
- ✅ `options.hpp` - Configuration options (API key, base URL, timeout, etc.)
- ✅ `error.hpp` - Error handling types

### Tier 1: Core Economic Data Clients (6 clients, 39 methods)
1. ✅ **HistoricalIndicatorsClient** (5 methods) - `historical_indicators_client.hpp`
   - getHistoricalData() - GDP, inflation, unemployment time series
   - getHistoricalRatings() - Credit ratings history
   - getHistoricalByTicker() - Data by TE ticker
   - getHistoricalLatest() - Latest updates
   - getHistoricalUpdates() - Recent changes

2. ✅ **CalendarClient** (5 methods) - `calendar_client.hpp`
   - getCalendarData() - Economic calendar events
   - getCalendarById() - Events by ID
   - getCalendarUpdates() - Latest calendar updates
   - getCalendarEventsByGroup() - Events by group (bonds, inflation, etc.)
   - getCalendarEvents() - All calendar events

3. ✅ **ForecastsClient** (3 methods) - `forecasts_client.hpp`
   - getForecastData() - Economic forecasts
   - getForecastByTicker() - Forecasts by ticker
   - getForecastUpdates() - Forecast revisions

4. ✅ **IndicatorsClient** (9 methods) - `indicators_client.hpp`
   - getIndicatorData() - Current indicator values
   - getRatings() - Credit ratings
   - getDiscontinuedIndicator() - Discontinued series
   - getIndicatorByCategoryGroup() - Indicators by category
   - getIndicatorByTicker() - Indicators by ticker
   - getLatestUpdates() - Latest updates
   - getPeers() - Peer indicators
   - getAllCountries() - Available countries
   - getIndicatorChanges() - Indicator revisions

5. ✅ **MarketsClient** (13 methods) - `markets_client.hpp`
   - getMarketsData() - Latest market data by asset class
   - getCurrencyCross() - Currency cross rates
   - getMarketsBySymbol() - Markets by symbol
   - getMarketsIntraday() - Intraday data
   - getMarketsPeers() - Peer markets
   - getMarketsComponents() - Index components
   - getMarketsSearch() - Search markets
   - getMarketsForecasts() - Market forecasts
   - getMarketsIntradayByInterval() - Aggregated intraday
   - getMarketsStockDescriptions() - Stock descriptions
   - getMarketsSymbology() - Symbol mappings
   - getStocksByCountry() - Stocks list
   - getMarketsHistorical() - Historical OHLC

6. ✅ **FederalReserveClient** (4 methods) - `federal_reserve_client.hpp`
   - getFedRStates() - US states/counties list
   - getFedRSnaps() - FED snapshots
   - getFedRCounty() - County-level data
   - getFedRHistorical() - Historical FED data

### Tier 2: Extended Research Clients (3 clients, 18 methods)
7. ✅ **EurostatClient** (4 methods) - `eurostat_client.hpp`
   - getEurostatData() - European statistics
   - getEurostatCountries() - Available countries
   - getEurostatCategoryGroups() - Categories
   - getHistoricalEurostat() - Historical Eurostat

8. ✅ **WorldBankClient** (4 methods) - `world_bank_client.hpp`
   - getWBCategories() - WB categories
   - getWBIndicator() - WB indicators
   - getWBCountry() - Indicators by country
   - getWBHistorical() - Historical WB data

9. ✅ **ComtradeClient** (10 methods) - `comtrade_client.hpp`
   - getCmtCategories() - Trade categories
   - getCmtCountry() - Country trade data
   - getCmtHistorical() - Historical trade
   - getCmtTwoCountries() - Bilateral trade
   - getCmtUpdates() - Latest updates
   - getCmtCountryByCategory() - Trade by category
   - getCmtTotalByType() - Total imports/exports
   - getCmtCountryFilterByType() - Filtered trade
   - getCmtSnapshotByType() - Trade snapshot
   - getCmtLastUpdates() - Last updates

### Factory
- ✅ `client_factory.hpp` - Factory methods for all 9 clients

---

## Phase 2: Implementation Plan

### Step 1: Create BaseClient (Shared HTTP Logic) 🔄
**Location**: `/src/trading_economics/base_client.hpp` and `.cpp`

**Responsibilities**:
- HTTP GET requests with Drogon
- Query string building
- Error handling and retries
- Pagination support
- Rate limiting (if needed)
- JSON to DataFrame conversion

**Key Methods**:
```cpp
class BaseClient {
protected:
  Expected<std::string> httpGet(const std::string& path,
                                const std::map<std::string, std::string>& params);
  Expected<epoch_frame::DataFrame> httpGetDataFrame(const std::string& path,
                                                     const std::map<std::string, std::string>& params);
  std::string buildQueryString(const std::map<std::string, std::string>& params);
  // ... pagination, retry logic, etc.
private:
  Options options_;
  drogon::HttpClientPtr httpClient_;
};
```

---

### Step 2: Implement Clients in Priority Order

Each client implementation follows this process:
1. Create stub `.cpp` file with pImpl pattern
2. Implement constructor/destructor with BaseClient
3. Implement each method one by one
4. Write unit test after each method
5. Write integration tests for the complete client
6. Move to next client

---

#### **Client 1: HistoricalIndicatorsClient** (Priority: HIGHEST)
**Why First**: Most critical for macro backtesting, simplest API structure

**Implementation Order**:
1. ✅ Create `/src/trading_economics/historical_indicators_client.cpp` with pImpl
2. ⬜ Implement `getHistoricalData()` + unit test
3. ⬜ Implement `getHistoricalByTicker()` + unit test
4. ⬜ Implement `getHistoricalRatings()` + unit test
5. ⬜ Implement `getHistoricalLatest()` + unit test
6. ⬜ Implement `getHistoricalUpdates()` + unit test
7. ⬜ Integration tests (live API)
8. ⬜ Example code in `/examples/trading_economics/`

**Estimated Time**: 2-3 days

---

#### **Client 2: CalendarClient** (Priority: HIGH)
**Why Second**: Critical for event-driven strategies

**Implementation Order**:
1. ⬜ Create `/src/trading_economics/calendar_client.cpp`
2. ⬜ Implement `getCalendarData()` + unit test (most complex, do first)
3. ⬜ Implement `getCalendarById()` + unit test
4. ⬜ Implement `getCalendarEvents()` + unit test
5. ⬜ Implement `getCalendarEventsByGroup()` + unit test
6. ⬜ Implement `getCalendarUpdates()` + unit test
7. ⬜ Integration tests
8. ⬜ Example code

**Estimated Time**: 2-3 days

---

#### **Client 3: IndicatorsClient** (Priority: HIGH)
**Why Third**: Metadata discovery, largest client (9 methods)

**Implementation Order**:
1. ⬜ Create `/src/trading_economics/indicators_client.cpp`
2. ⬜ Implement `getIndicatorData()` + unit test
3. ⬜ Implement `getIndicatorByTicker()` + unit test
4. ⬜ Implement `getAllCountries()` + unit test
5. ⬜ Implement `getRatings()` + unit test
6. ⬜ Implement `getLatestUpdates()` + unit test
7. ⬜ Implement `getIndicatorByCategoryGroup()` + unit test
8. ⬜ Implement `getPeers()` + unit test
9. ⬜ Implement `getDiscontinuedIndicator()` + unit test
10. ⬜ Implement `getIndicatorChanges()` + unit test
11. ⬜ Integration tests
12. ⬜ Example code

**Estimated Time**: 3-4 days

---

#### **Client 4: ForecastsClient** (Priority: MEDIUM)
**Why Fourth**: Smaller client, important for expectation analysis

**Implementation Order**:
1. ⬜ Create `/src/trading_economics/forecasts_client.cpp`
2. ⬜ Implement `getForecastData()` + unit test
3. ⬜ Implement `getForecastByTicker()` + unit test
4. ⬜ Implement `getForecastUpdates()` + unit test
5. ⬜ Integration tests
6. ⬜ Example code

**Estimated Time**: 1-2 days

---

#### **Client 5: MarketsClient** (Priority: MEDIUM-HIGH)
**Why Fifth**: Largest client (13 methods), critical for multi-asset

**Implementation Order**:
1. ⬜ Create `/src/trading_economics/markets_client.cpp`
2. ⬜ Implement `getMarketsHistorical()` + unit test (most important for backtesting)
3. ⬜ Implement `getMarketsIntraday()` + unit test
4. ⬜ Implement `getMarketsIntradayByInterval()` + unit test
5. ⬜ Implement `getMarketsBySymbol()` + unit test
6. ⬜ Implement `getMarketsData()` + unit test
7. ⬜ Implement `getCurrencyCross()` + unit test
8. ⬜ Implement `getMarketsForecasts()` + unit test
9. ⬜ Implement `getMarketsComponents()` + unit test
10. ⬜ Implement `getMarketsPeers()` + unit test
11. ⬜ Implement `getMarketsSearch()` + unit test
12. ⬜ Implement `getMarketsStockDescriptions()` + unit test
13. ⬜ Implement `getMarketsSymbology()` + unit test
14. ⬜ Implement `getStocksByCountry()` + unit test
15. ⬜ Integration tests
16. ⬜ Example code

**Estimated Time**: 4-5 days

---

#### **Client 6: FederalReserveClient** (Priority: MEDIUM)
**Why Sixth**: Unique US regional data

**Implementation Order**:
1. ⬜ Create `/src/trading_economics/federal_reserve_client.cpp`
2. ⬜ Implement `getFedRHistorical()` + unit test
3. ⬜ Implement `getFedRSnaps()` + unit test
4. ⬜ Implement `getFedRStates()` + unit test
5. ⬜ Implement `getFedRCounty()` + unit test
6. ⬜ Integration tests
7. ⬜ Example code

**Estimated Time**: 2 days

---

#### **Client 7: EurostatClient** (Priority: LOW-MEDIUM)
**Why Seventh**: European focus, Tier 2

**Implementation Order**:
1. ⬜ Create `/src/trading_economics/eurostat_client.cpp`
2. ⬜ Implement `getHistoricalEurostat()` + unit test
3. ⬜ Implement `getEurostatData()` + unit test
4. ⬜ Implement `getEurostatCountries()` + unit test
5. ⬜ Implement `getEurostatCategoryGroups()` + unit test
6. ⬜ Integration tests
7. ⬜ Example code

**Estimated Time**: 2 days

---

#### **Client 8: WorldBankClient** (Priority: LOW)
**Why Eighth**: Long-term data, Tier 2

**Implementation Order**:
1. ⬜ Create `/src/trading_economics/world_bank_client.cpp`
2. ⬜ Implement `getWBHistorical()` + unit test
3. ⬜ Implement `getWBIndicator()` + unit test
4. ⬜ Implement `getWBCountry()` + unit test
5. ⬜ Implement `getWBCategories()` + unit test
6. ⬜ Integration tests
7. ⬜ Example code

**Estimated Time**: 2 days

---

#### **Client 9: ComtradeClient** (Priority: LOW)
**Why Last**: Specialized trade data, Tier 2, 10 methods

**Implementation Order**:
1. ⬜ Create `/src/trading_economics/comtrade_client.cpp`
2. ⬜ Implement `getCmtHistorical()` + unit test
3. ⬜ Implement `getCmtCountry()` + unit test
4. ⬜ Implement `getCmtTwoCountries()` + unit test
5. ⬜ Implement `getCmtCategories()` + unit test
6. ⬜ Implement `getCmtCountryByCategory()` + unit test
7. ⬜ Implement `getCmtTotalByType()` + unit test
8. ⬜ Implement `getCmtCountryFilterByType()` + unit test
9. ⬜ Implement `getCmtSnapshotByType()` + unit test
10. ⬜ Implement `getCmtUpdates()` + unit test
11. ⬜ Implement `getCmtLastUpdates()` + unit test
12. ⬜ Integration tests
13. ⬜ Example code

**Estimated Time**: 3 days

---

### Step 3: ClientFactory Implementation
**Location**: `/src/trading_economics/client_factory.cpp`

Implement all 9 factory methods:
```cpp
std::unique_ptr<HistoricalIndicatorsClient>
ClientFactory::createHistoricalIndicatorsClient(const Options& options) {
  return std::make_unique<HistoricalIndicatorsClient>(options);
}
// ... repeat for all 9 clients
```

---

### Step 4: CMakeLists.txt Updates

Update `/src/trading_economics/CMakeLists.txt`:
```cmake
add_library(trading_economics_sdk
  base_client.cpp
  historical_indicators_client.cpp
  calendar_client.cpp
  forecasts_client.cpp
  indicators_client.cpp
  markets_client.cpp
  federal_reserve_client.cpp
  eurostat_client.cpp
  world_bank_client.cpp
  comtrade_client.cpp
  client_factory.cpp
)

target_link_libraries(trading_economics_sdk
  PUBLIC epoch_frame
  PRIVATE drogon
)
```

---

## Testing Strategy

### Unit Tests
For each method:
- Test successful API response parsing
- Test error handling (404, 500, etc.)
- Test parameter validation
- Test DataFrame structure
- Mock HTTP responses using test fixtures

**Example**:
```cpp
TEST_CASE("HistoricalIndicatorsClient::getHistoricalData", "[trading_economics]") {
  SECTION("Valid request returns DataFrame") {
    // Mock HTTP response
    // Create client
    // Call method
    // Verify DataFrame structure
  }

  SECTION("Invalid parameters throw error") {
    // Test error handling
  }
}
```

### Integration Tests
For each client:
- Test against live API (rate-limited)
- Test pagination
- Test date range queries
- Test multiple parameters

**Location**: `/test/trading_economics/{client}_tests.cpp`

---

## Timeline Estimate

| Phase | Duration | Notes |
|-------|----------|-------|
| Phase 1: Skeleton Headers | ✅ Complete | All headers created |
| Phase 2.1: BaseClient | 2-3 days | Shared HTTP logic |
| Phase 2.2: Client 1 (Historical) | 2-3 days | 5 methods |
| Phase 2.3: Client 2 (Calendar) | 2-3 days | 5 methods |
| Phase 2.4: Client 3 (Indicators) | 3-4 days | 9 methods |
| Phase 2.5: Client 4 (Forecasts) | 1-2 days | 3 methods |
| Phase 2.6: Client 5 (Markets) | 4-5 days | 13 methods |
| Phase 2.7: Client 6 (FedReserve) | 2 days | 4 methods |
| Phase 2.8: Client 7 (Eurostat) | 2 days | 4 methods |
| Phase 2.9: Client 8 (WorldBank) | 2 days | 4 methods |
| Phase 2.10: Client 9 (Comtrade) | 3 days | 10 methods |
| Phase 3: ClientFactory | 1 day | Factory methods |
| **Total** | **~25-30 days** | **57 methods** |

---

## Next Steps

1. ✅ Phase 1 Complete: All skeleton headers created
2. 🔄 Phase 2 Start: Create BaseClient with HTTP logic
3. ⬜ Implement Client 1: HistoricalIndicatorsClient (highest priority)
4. ⬜ Continue with remaining clients in priority order

---

## File Structure Summary

```
include/epoch_data_sdk/trading_economics/
├── options.hpp                        ✅
├── error.hpp                          ✅
├── historical_indicators_client.hpp   ✅
├── calendar_client.hpp                ✅
├── forecasts_client.hpp               ✅
├── indicators_client.hpp              ✅
├── markets_client.hpp                 ✅
├── federal_reserve_client.hpp         ✅
├── eurostat_client.hpp                ✅
├── world_bank_client.hpp              ✅
├── comtrade_client.hpp                ✅
└── client_factory.hpp                 ✅

src/trading_economics/
├── base_client.hpp         (TODO)
├── base_client.cpp         (TODO)
└── {9 client .cpp files}   (TODO)

test/trading_economics/
└── {9 client test files}   (TODO)
```

---

## Success Criteria

- [ ] All 57 methods implemented
- [ ] All unit tests passing
- [ ] All integration tests passing
- [ ] Example code for each client
- [ ] Documentation complete
- [ ] CMakeLists.txt updated
- [ ] Code review completed

---

**Status**: Phase 1 Complete ✅ | Ready for Phase 2 Implementation 🚀
