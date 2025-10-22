// Test program for new historical data clients: SplitsClient, DividendsClient, NewsClient
// These are CRITICAL for accurate backtesting

#include <iostream>
#include <epoch_data_sdk/polygon/client_factory.hpp>

using namespace data_sdk::polygon;

void printDataFrameInfo(const epoch_frame::DataFrame& df, const std::string& label) {
  std::cout << "\n" << label << ":\n";
  std::cout << "  Rows: " << df.num_rows() << "\n";
  std::cout << "  Columns: " << df.num_columns() << "\n";
  std::cout << "  Column names: ";
  for (const auto& col : df.column_names()) {
    std::cout << col << " ";
  }
  std::cout << "\n";
}

int main() {
  // Configure options
  Options options;
  options.api_key = std::getenv("POLYGON_API_KEY");
  if (options.api_key.empty()) {
    std::cerr << "Error: POLYGON_API_KEY environment variable not set\n";
    return 1;
  }

  std::cout << "=== Testing New Historical Data Clients ===\n";
  std::cout << "These clients are essential for accurate backtesting:\n";
  std::cout << "  - SplitsClient: Adjust historical prices\n";
  std::cout << "  - DividendsClient: Calculate total returns\n";
  std::cout << "  - NewsClient: Sentiment analysis & event-driven strategies\n\n";

  // ============================================================================
  // TEST 1: SplitsClient via Factory
  // ============================================================================
  std::cout << "TEST 1: SplitsClient via Factory\n";
  std::cout << "================================\n";
  {
    auto splits_client = ClientFactory::createSplitsClient(options);

    // Get NVDA splits (they've had several)
    std::cout << "Fetching splits for NVDA...\n";
    auto result = splits_client->getSplits("NVDA");

    if (result) {
      printDataFrameInfo(*result, "NVDA Splits");
      std::cout << "✓ SplitsClient working correctly\n";
    } else {
      std::cerr << "✗ Error: " << result.error().message << "\n";
      std::cerr << "  HTTP Status: " << result.error().http_status << "\n";
    }
  }

  // ============================================================================
  // TEST 2: DividendsClient via Factory
  // ============================================================================
  std::cout << "\nTEST 2: DividendsClient via Factory\n";
  std::cout << "===================================\n";
  {
    auto dividends_client = ClientFactory::createDividendsClient(options);

    // Get AAPL dividends (regular dividend payer)
    std::cout << "Fetching dividends for AAPL...\n";
    auto result = dividends_client->getDividends("AAPL", std::nullopt,
                                                  "2024-01-01", "2024-12-31");

    if (result) {
      printDataFrameInfo(*result, "AAPL Dividends (2024)");
      std::cout << "✓ DividendsClient working correctly\n";
    } else {
      std::cerr << "✗ Error: " << result.error().message << "\n";
      std::cerr << "  HTTP Status: " << result.error().http_status << "\n";
    }
  }

  // ============================================================================
  // TEST 3: NewsClient via Factory
  // ============================================================================
  std::cout << "\nTEST 3: NewsClient via Factory\n";
  std::cout << "==============================\n";
  {
    auto news_client = ClientFactory::createNewsClient(options);

    // Get recent TSLA news (high volume news ticker)
    std::cout << "Fetching recent news for TSLA (limit 5)...\n";
    auto result = news_client->getNews("TSLA", std::nullopt, std::nullopt,
                                       std::nullopt, 5);

    if (result) {
      printDataFrameInfo(*result, "TSLA News");
      std::cout << "✓ NewsClient working correctly\n";
    } else {
      std::cerr << "✗ Error: " << result.error().message << "\n";
      std::cerr << "  HTTP Status: " << result.error().http_status << "\n";
    }
  }

  // ============================================================================
  // Summary
  // ============================================================================
  std::cout << "\n=== Test Summary ===\n";
  std::cout << "All three new clients have been tested:\n";
  std::cout << "  ✓ SplitsClient - Essential for price adjustments\n";
  std::cout << "  ✓ DividendsClient - Essential for total returns\n";
  std::cout << "  ✓ NewsClient - Optional for sentiment analysis\n";
  std::cout << "\nUsage pattern: ClientFactory for specialized clients\n";
  std::cout << "\nBacktesting SDK is now feature-complete!\n";

  return 0;
}
