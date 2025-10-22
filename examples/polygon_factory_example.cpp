// Example demonstrating the use of ClientFactory to create specialized Polygon clients
// This shows the new architecture mirroring polygon-sdk-python

#include <iostream>
#include <epoch_data_sdk/polygon/client_factory.hpp>

using namespace data_sdk::polygon;

int main() {
  // Configure options
  Options options;
  options.api_key = std::getenv("POLYGON_API_KEY");
  if (options.api_key.empty()) {
    std::cerr << "Error: POLYGON_API_KEY environment variable not set\n";
    return 1;
  }

  std::cout << "=== Polygon SDK Architecture Demo ===\n\n";

  // ============================================================================
  // Use ClientFactory for specialized clients
  // ============================================================================
  std::cout << "Using ClientFactory for specialized clients\n";
  std::cout << "--------------------------------------------\n";
  {
    // Create only the clients you need
    auto aggs_client = ClientFactory::createAggsClient(options);
    auto quotes_client = ClientFactory::createQuotesClient(options);
    auto trades_client = ClientFactory::createTradesClient(options);
    auto financials_client = ClientFactory::createFinancialsClient(options);

    std::cout << "✓ Created AggsClient\n";
    std::cout << "✓ Created QuotesClient\n";
    std::cout << "✓ Created TradesClient\n";
    std::cout << "✓ Created FinancialsClient\n";

    // Use specialized clients
    auto aggs_result = aggs_client->getAggregates("AAPL", "2024-01-01", "2024-01-05", true);
    if (aggs_result) {
      std::cout << "✓ Fetched aggregates via AggsClient\n";
    }

    auto quotes_result = quotes_client->getQuotes("AAPL", "2024-01-01", "2024-01-05");
    if (quotes_result) {
      std::cout << "✓ Fetched quotes via QuotesClient\n";
    }
  }
  std::cout << "\n";

  // ============================================================================
  // ARCHITECTURE BENEFITS
  // ============================================================================
  std::cout << "Architecture Benefits:\n";
  std::cout << "----------------------\n";
  std::cout << "1. Separation of Concerns: Each client handles one data type\n";
  std::cout << "2. Factory Pattern: Clean client creation without exposing base class\n";
  std::cout << "3. Single Responsibility: Focused, maintainable clients\n";
  std::cout << "4. Mirrored Design: Matches polygon-sdk-python architecture\n";
  std::cout << "5. Extensible: Easy to add new clients (e.g., OptionsClient)\n";
  std::cout << "\n";

  // ============================================================================
  // SUPPORTED DATA TYPES
  // ============================================================================
  std::cout << "Supported Historical Data:\n";
  std::cout << "--------------------------\n";
  std::cout << "• AggsClient: OHLCV bars (stocks, forex C:, crypto X:)\n";
  std::cout << "• QuotesClient: NBBO quotes (stocks, forex)\n";
  std::cout << "• TradesClient: Trade ticks (stocks, crypto)\n";
  std::cout << "• FinancialsClient: Balance sheets, cash flow, income, ratios\n";
  std::cout << "\n";

  std::cout << "Demo complete!\n";
  return 0;
}
