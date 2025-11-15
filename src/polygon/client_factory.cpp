#include "polygon/client_factory.hpp"

namespace data_sdk::polygon {

std::unique_ptr<AggsClient> ClientFactory::createAggsClient(const Options& options) {
  return std::make_unique<AggsClient>(options);
}

std::unique_ptr<QuotesClient> ClientFactory::createQuotesClient(const Options& options) {
  return std::make_unique<QuotesClient>(options);
}

std::unique_ptr<TradesClient> ClientFactory::createTradesClient(const Options& options) {
  return std::make_unique<TradesClient>(options);
}

std::unique_ptr<FinancialsClient> ClientFactory::createFinancialsClient(const Options& options) {
  return std::make_unique<FinancialsClient>(options);
}

std::unique_ptr<ShortVolumeClient> ClientFactory::createShortVolumeClient(const Options& options) {
  return std::make_unique<ShortVolumeClient>(options);
}

std::unique_ptr<ShortInterestClient> ClientFactory::createShortInterestClient(const Options& options) {
  return std::make_unique<ShortInterestClient>(options);
}

std::unique_ptr<SplitsClient> ClientFactory::createSplitsClient(const Options& options) {
  return std::make_unique<SplitsClient>(options);
}

std::unique_ptr<DividendsClient> ClientFactory::createDividendsClient(const Options& options) {
  return std::make_unique<DividendsClient>(options);
}

std::unique_ptr<TickerEventsClient> ClientFactory::createTickerEventsClient(const Options& options) {
  return std::make_unique<TickerEventsClient>(options);
}

std::unique_ptr<NewsClient> ClientFactory::createNewsClient(const Options& options) {
  return std::make_unique<NewsClient>(options);
}

std::unique_ptr<RatiosClient> ClientFactory::createRatiosClient(const Options& options) {
  return std::make_unique<RatiosClient>(options);
}

} // namespace data_sdk::polygon
