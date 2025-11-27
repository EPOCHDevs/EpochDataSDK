#pragma once

#include <string>
#include <vector>

#include <drogon/drogon.h>

#include "base_client.hpp"
#include "models.hpp"

namespace data_sdk::fred {

// Shared implementation for fetching FRED series data
// Provides chunking for long date ranges, fallback to FRED mode, and pagination
class SeriesImpl : public BaseClient {
public:
  explicit SeriesImpl(Options options) : BaseClient(std::move(options)) {}

  // Fetch series data with specified parameters
  // Returns raw observations with realtime_start dates
  // observation_start/end are optional - if empty, FRED API will return all observations
  Expected<SeriesObservationsResponse>
  fetchSeries(const std::string &series_id,
              const std::string &observation_start,
              const std::string &observation_end,
              const std::string &realtime_start,
              const std::string &realtime_end) const;

  // Async version
  drogon::Task<Expected<SeriesObservationsResponse>>
  fetchSeriesAsync(std::string series_id,
                   std::string observation_start,
                   std::string observation_end,
                   std::string realtime_start,
                   std::string realtime_end) const;
};

} // namespace data_sdk::fred
