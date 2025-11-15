#pragma once

#include <memory>

#include "options.hpp"
#include "aggs_client.hpp"
#include "quotes_client.hpp"
#include "trades_client.hpp"
#include "financials_client.hpp"
#include "short_volume_client.hpp"
#include "short_interest_client.hpp"
#include "splits_client.hpp"
#include "dividends_client.hpp"
#include "ticker_events_client.hpp"
#include "news_client.hpp"
#include "ratios_client.hpp"

namespace data_sdk::polygon {

// ClientFactory - Factory methods to create specialized Polygon clients
// Each call creates a new instance (no singleton pattern)
class ClientFactory {
public:
  // Create a new AggsClient for OHLCV/aggregate data
  // Supports: stocks, forex (C:), crypto (X:)
  static std::unique_ptr<AggsClient> createAggsClient(const Options& options);

  // Create a new QuotesClient for historical quote (NBBO) data
  // Supports: stocks, forex
  static std::unique_ptr<QuotesClient> createQuotesClient(const Options& options);

  // Create a new TradesClient for historical trade data
  // Supports: stocks, crypto
  static std::unique_ptr<TradesClient> createTradesClient(const Options& options);

  // Create a new FinancialsClient for financial statements and ratios
  // Supports: stocks only
  static std::unique_ptr<FinancialsClient> createFinancialsClient(const Options& options);

  // Create a new ShortVolumeClient for short volume data
  // Supports: stocks only
  static std::unique_ptr<ShortVolumeClient> createShortVolumeClient(const Options& options);

  // Create a new ShortInterestClient for short interest data
  // Supports: stocks only
  static std::unique_ptr<ShortInterestClient> createShortInterestClient(const Options& options);

  // Create a new SplitsClient for stock split data
  // CRITICAL for backtesting: adjusts historical prices for splits
  // Supports: stocks only
  static std::unique_ptr<SplitsClient> createSplitsClient(const Options& options);

  // Create a new DividendsClient for dividend data
  // CRITICAL for backtesting: calculate total returns (price + dividends)
  // Supports: stocks only
  static std::unique_ptr<DividendsClient> createDividendsClient(const Options& options);

  // Create a new TickerEventsClient for ticker events (ticker changes, etc.)
  // Useful for: tracking ticker symbol changes over time
  // Supports: stocks only
  static std::unique_ptr<TickerEventsClient> createTickerEventsClient(const Options& options);

  // Create a new NewsClient for news article data
  // OPTIONAL for backtesting: sentiment analysis, event-driven strategies
  // Supports: all asset types
  static std::unique_ptr<NewsClient> createNewsClient(const Options& options);

  // Create a new RatiosClient for financial ratios data
  // Supports: stocks only
  static std::unique_ptr<RatiosClient> createRatiosClient(const Options& options);
};

} // namespace data_sdk::polygon
