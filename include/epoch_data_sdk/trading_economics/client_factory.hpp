#pragma once

#include <memory>

#include "options.hpp"
#include "historical_indicators_client.hpp"
#include "calendar_client.hpp"
#include "forecasts_client.hpp"
#include "indicators_client.hpp"
#include "markets_client.hpp"
#include "federal_reserve_client.hpp"
#include "eurostat_client.hpp"
#include "world_bank_client.hpp"
#include "comtrade_client.hpp"

namespace data_sdk::trading_economics {

// ClientFactory - Factory methods to create specialized Trading Economics clients
// Each call creates a new instance (no singleton pattern)
// Mirrors the Polygon client factory pattern for consistency
class ClientFactory {
public:
  // ============================================================================
  // TIER 1: Core Economic Data Clients (Critical for backtesting)
  // ============================================================================

  // Create a new HistoricalIndicatorsClient
  // Use for: GDP, inflation, unemployment, interest rates historical data
  // Critical for: Macro strategy backtesting
  static std::unique_ptr<HistoricalIndicatorsClient>
  createHistoricalIndicatorsClient(const Options& options);

  // Create a new CalendarClient
  // Use for: Economic calendar events with actual vs forecast values
  // Critical for: Event-driven strategies (NFP, FOMC, CPI releases, etc.)
  static std::unique_ptr<CalendarClient>
  createCalendarClient(const Options& options);

  // Create a new ForecastsClient
  // Use for: Economic forecasts and consensus expectations
  // Important for: Forecast vs actual analysis, sentiment analysis
  static std::unique_ptr<ForecastsClient>
  createForecastsClient(const Options& options);

  // Create a new IndicatorsClient
  // Use for: Current indicator values and metadata discovery
  // Important for: Getting latest values, discovering available indicators
  static std::unique_ptr<IndicatorsClient>
  createIndicatorsClient(const Options& options);

  // Create a new MarketsClient
  // Use for: Bonds, commodities, currencies, indexes historical data
  // Critical for: Multi-asset backtesting, yield curve analysis
  static std::unique_ptr<MarketsClient>
  createMarketsClient(const Options& options);

  // Create a new FederalReserveClient
  // Use for: US state and county-level economic data
  // Important for: Regional economic analysis, demographic studies
  static std::unique_ptr<FederalReserveClient>
  createFederalReserveClient(const Options& options);

  // ============================================================================
  // TIER 2: Extended Research Data Clients
  // ============================================================================

  // Create a new EurostatClient
  // Use for: European Union economic and social statistics
  // Important for: European market research
  static std::unique_ptr<EurostatClient>
  createEurostatClient(const Options& options);

  // Create a new WorldBankClient
  // Use for: Long-term global development indicators
  // Important for: Long-term macro research, emerging markets
  static std::unique_ptr<WorldBankClient>
  createWorldBankClient(const Options& options);

  // Create a new ComtradeClient
  // Use for: International trade flow data (imports/exports)
  // Important for: Trade relationship analysis, supply chain research
  static std::unique_ptr<ComtradeClient>
  createComtradeClient(const Options& options);
};

} // namespace data_sdk::trading_economics
