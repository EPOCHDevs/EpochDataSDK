# EpochDataSDK Migration Guide: DataLoaderOptions API v2

This guide helps EpochScript team migrate from the legacy `DataLoaderOptions` API to the new unified request-based API.

---

## Recent Changes: Indices → ReferenceAgg (v2.1)

The `Indices` category has been replaced with a more generic `ReferenceAgg` that supports loading aggregates for multiple asset classes from Polygon.

### What Changed

| Before | After |
|--------|-------|
| `DataCategory::Indices` | `DataCategory::ReferenceAgg` |
| `IndicesKwargs{ticker, is_eod}` | `ReferenceAggKwargs{ticker, asset_class, is_eod}` |
| Column: `IDX:SPX:daily:c` | Column: `IDX:SPX:c` (no timeframe) |

### Supported Asset Classes

| AssetClass | Column Prefix | Polygon Prefix | Example |
|------------|---------------|----------------|---------|
| `Indices` | `IDX:` | `I:` | `IDX:SPX:c` |
| `Stocks` | `STK:` | (none) | `STK:AAPL:c` |
| `FX` | `FX:` | `C:` | `FX:EURUSD:c` |
| `Crypto` | `CRYPTO:` | `X:` | `CRYPTO:BTCUSD:c` |

**Note:** Futures are NOT supported - will throw `std::invalid_argument`.

### Migration Examples

```cpp
// Before: Adding indices only
opt.AddIndex("SPX");
opt.AddIndex("VIX", /*is_eod=*/false);  // minute bars

// After: Generic reference aggregates with asset class
opt.AddReferenceAgg("SPX", AssetClass::Indices);    // Index
opt.AddReferenceAgg("AAPL", AssetClass::Stocks);    // Stock
opt.AddReferenceAgg("EURUSD", AssetClass::FX);      // Forex
opt.AddReferenceAgg("BTCUSD", AssetClass::Crypto);  // Crypto

// Backward compatible: AddIndex() still works (defaults to Indices)
opt.AddIndex("SPX");  // Same as AddReferenceAgg("SPX", AssetClass::Indices)
```

### Column Name Changes

The timeframe (daily/minute) is no longer embedded in column names since ReferenceAgg uses the same timeframe as the primary category:

```cpp
// Before: Column names included timeframe
// DailyBars primary → "IDX:SPX:daily:c"
// MinuteBars primary → "IDX:SPX:minute:c"

// After: Column names use asset class prefix only
// "IDX:SPX:c", "FX:EURUSD:c", "CRYPTO:BTCUSD:c", "STK:AAPL:c"
```

### New Query Methods

```cpp
// Get all reference agg requests with their kwargs
std::vector<ReferenceAggKwargs> GetReferenceAggRequests() const;

// Get just indices tickers (backward compat)
std::set<std::string> GetIndicesTickers() const;
```

### ReferenceAggKwargs Structure

```cpp
struct ReferenceAggKwargs {
  std::string ticker;           // Required: "SPX", "AAPL", "EURUSD", "BTCUSD"
  AssetClass asset_class;       // Required: Stocks, FX, Crypto, or Indices
  bool is_eod = true;           // Set by dataloader from primary category

  // Helper methods
  void validate() const;              // Throws if asset_class not supported
  std::string getPolygonPrefix() const;  // "I:", "C:", "X:", or ""
  std::string getColumnPrefix() const;   // "IDX:", "STK:", "FX:", "CRYPTO:"
  std::string getPolygonTicker() const;  // Full ticker with prefix
};
```

---

## Summary of Changes

The `DataLoaderOptions` struct has been refactored to use a unified `requests` vector instead of separate fields for different data types.

### Removed Fields

The following fields have been **removed** from `DataLoaderOptions`:
- `categories` - Use `AddRequest(DataCategory)` instead
- `crossSectionalCategories` - Use `AddEconomicIndicator(CrossSectionalDataCategory)` instead
- `indicesTickers` - Use `AddIndex(ticker)` instead

### Removed Methods

The following methods have been **removed**:
- `SetCategories()` - Use `AddRequest()` instead
- `AddCategory()` - Use `AddRequest()` instead
- `RemoveCategory()` - Not supported in new API
- `GetDataCategory()` - Use `GetPrimaryCategory()` instead
- `GetAllCategories()` - Use `GetCategories()` instead

## Migration Examples

### Before (Old API)

```cpp
DataloaderOption opt;
opt.startDate = DateTime::from_date_str("2024-01-01").date();
opt.endDate = DateTime::from_date_str("2024-12-31").date();
opt.dataloaderAssets = {SPY, AAPL, MSFT};

// Old: Setting categories directly
opt.categories = {DataCategory::DailyBars, DataCategory::Dividends, DataCategory::Splits};

// Old: Adding cross-sectional categories
opt.AddCrossSectionalCategory(CrossSectionalDataCategory::CPI);
opt.AddCrossSectionalCategory(CrossSectionalDataCategory::FedFunds);

// Old: Adding index tickers
opt.AddIndexTicker("SPX");
opt.AddIndexTicker("VIX");
```

### After (New API)

```cpp
DataloaderOption opt;
opt.startDate = DateTime::from_date_str("2024-01-01").date();
opt.endDate = DateTime::from_date_str("2024-12-31").date();
opt.dataloaderAssets = {SPY, AAPL, MSFT};

// New: Add each category as a request
opt.AddRequest(DataCategory::DailyBars);
opt.AddRequest(DataCategory::Dividends);
opt.AddRequest(DataCategory::Splits);

// New: Add economic indicators using enum (recommended)
opt.AddEconomicIndicator(CrossSectionalDataCategory::CPI);
opt.AddEconomicIndicator(CrossSectionalDataCategory::FedFunds);

// New: Add indices
opt.AddIndex("SPX");
opt.AddIndex("VIX");
```

## New API Methods

### Adding Requests

```cpp
// Simple category request (no kwargs needed)
void AddRequest(DataCategory category);

// Financial statements with timeframe
void AddFinancialsRequest(DataCategory category,
                          FinancialsTimeframe timeframe = FinancialsTimeframe::Quarterly);

// Economic indicators (FRED data via enum)
void AddEconomicIndicator(CrossSectionalDataCategory indicator,
                          bool use_alfred = true);

// Market indices (convenience wrapper for AddReferenceAgg with Indices)
void AddIndex(const std::string& ticker);

// Reference aggregates (Stocks, FX, Crypto, Indices)
void AddReferenceAgg(const std::string& ticker, AssetClass asset_class);
```

### Query Methods

```cpp
// Get all requests
const std::vector<DataRequest>& GetRequests() const;

// Get per-asset requests (excludes EconomicIndicator and ReferenceAgg)
std::vector<DataRequest> GetAssetRequests() const;

// Get cross-sectional requests (EconomicIndicator and ReferenceAgg only)
std::vector<DataRequest> GetCrossSectionalRequests() const;

// Get reference agg requests with full kwargs
std::vector<ReferenceAggKwargs> GetReferenceAggRequests() const;

// Get unique categories from all requests
std::set<DataCategory> GetCategories() const;

// Check if any request has a specific category
bool HasCategory(DataCategory cat) const;

// Check if using multi-category mode
bool IsMultiCategory() const;

// Get primary data category (first non-cross-sectional)
DataCategory GetPrimaryCategory() const;
```

## Backward Compatibility

For a smoother transition, the following deprecated methods are still available:

```cpp
// DEPRECATED: Use AddEconomicIndicator(indicator) instead
void AddCrossSectionalCategory(CrossSectionalDataCategory category);

// DEPRECATED: Use AddIndex(ticker) instead
void AddIndexTicker(const std::string& ticker);

// DEPRECATED: Returns empty set - use GetCrossSectionalRequests().size()
std::set<CrossSectionalDataCategory> GetCrossSectionalCategories() const;

// DEPRECATED: Use GetIndicesTickers() returns tickers from Indices requests
std::set<std::string> GetIndicesTickers() const;
```

## IDataLoader Interface Changes

### Economic Indicator Methods

The `LoadEconomicIndicator` methods now accept `CrossSectionalDataCategory` enum instead of string series IDs:

```cpp
// Before (removed)
LoadEconomicIndicator("CPIAUCSL", fromDate, toDate, use_alfred);

// After (new)
LoadEconomicIndicator(CrossSectionalDataCategory::CPI, fromDate, toDate, use_alfred);
```

### Available Economic Indicators

Use `CrossSectionalDataCategory` enum values:

| Enum Value | FRED Series ID | Description |
|------------|----------------|-------------|
| `CPI` | CPIAUCSL | Consumer Price Index |
| `CoreCPI` | CPILFESL | Core CPI (ex food & energy) |
| `PCE` | PCEPI | Personal Consumption Expenditures |
| `CorePCE` | PCEPILFE | Core PCE |
| `FedFunds` | DFF | Federal Funds Rate |
| `Treasury3M` | DTB3 | 3-Month Treasury |
| `Treasury2Y` | DGS2 | 2-Year Treasury |
| `Treasury5Y` | DGS5 | 5-Year Treasury |
| `Treasury10Y` | DGS10 | 10-Year Treasury |
| `Treasury30Y` | DGS30 | 30-Year Treasury |
| `Unemployment` | UNRATE | Unemployment Rate |
| `NonfarmPayrolls` | PAYEMS | Nonfarm Payrolls |
| `InitialClaims` | ICSA | Initial Jobless Claims |
| `GDP` | GDPC1 | Real GDP |
| `IndustrialProduction` | INDPRO | Industrial Production |
| `RetailSales` | RSXFS | Retail Sales |
| `HousingStarts` | HOUST | Housing Starts |
| `ConsumerSentiment` | UMCSENT | Consumer Sentiment |
| `M2` | M2SL | M2 Money Supply |
| `SP500` | SP500 | S&P 500 Index |
| `VIX` | VIXCLS | VIX Volatility Index |

### Index Methods (Backward Compatible)

```cpp
// These aliases are available for backward compatibility:
LoadIndicesData(...)    // Alias for LoadIndexData()
LoadIndicesDataAsync()  // Alias for LoadIndexDataAsync()
```

## Financial Statements with Timeframe

Financial statement categories now support a timeframe parameter:

```cpp
// Before: No timeframe control
opt.AddCategory(DataCategory::BalanceSheets);

// After: Explicit timeframe control
opt.AddFinancialsRequest(DataCategory::BalanceSheets, FinancialsTimeframe::Quarterly);
opt.AddFinancialsRequest(DataCategory::IncomeStatements, FinancialsTimeframe::Annual);
opt.AddFinancialsRequest(DataCategory::CashFlowStatements, FinancialsTimeframe::TTM);
```

Available timeframes:
- `FinancialsTimeframe::Quarterly` - Quarterly results (default)
- `FinancialsTimeframe::Annual` - Annual results (10-K)
- `FinancialsTimeframe::TTM` - Trailing Twelve Months

## Data Request Structure

Each request is stored as a `DataRequest` struct:

```cpp
struct DataRequest {
  DataCategory category;
  FetchKwargs kwargs;  // std::variant of NoKwargs, FinancialsKwargs,
                       // EconomicIndicatorKwargs, ReferenceAggKwargs
};
```

## Validation

The `IsValid()` method now checks:
1. At least one request must be present
2. Cannot mix `MinuteBars` and `DailyBars` (they affect the same OHLCV columns)

```cpp
if (!opt.IsValid()) {
  // Handle invalid options
}
```

## Need Help?

If you encounter issues during migration, please contact the EpochDataSDK team.
