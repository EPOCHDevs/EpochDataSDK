#pragma once

#include <optional>
#include <string>
#include <vector>

#include <glaze/glaze.hpp>

namespace data_sdk::fred {

// FRED series observation
struct Observation {
  std::optional<std::string> realtime_start;
  std::optional<std::string> realtime_end;
  std::optional<std::string> date;
  std::optional<std::string> value;
};

// Response from fred/series/observations endpoint
struct SeriesObservationsResponse {
  std::optional<std::string> realtime_start;
  std::optional<std::string> realtime_end;
  std::optional<std::string> observation_start;
  std::optional<std::string> observation_end;
  std::optional<std::string> units;
  std::optional<int> output_type;
  std::optional<std::string> file_type;
  std::optional<std::string> order_by;
  std::optional<std::string> sort_order;
  std::optional<int> count;
  std::optional<int> offset;
  std::optional<int> limit;
  std::vector<Observation> observations;
};

} // namespace data_sdk::fred

// Glaze metadata for JSON parsing
template <>
struct glz::meta<data_sdk::fred::Observation> {
  using T = data_sdk::fred::Observation;
  static constexpr auto value = object(
      "realtime_start", &T::realtime_start,
      "realtime_end", &T::realtime_end,
      "date", &T::date,
      "value", &T::value
  );
};

template <>
struct glz::meta<data_sdk::fred::SeriesObservationsResponse> {
  using T = data_sdk::fred::SeriesObservationsResponse;
  static constexpr auto value = object(
      "realtime_start", &T::realtime_start,
      "realtime_end", &T::realtime_end,
      "observation_start", &T::observation_start,
      "observation_end", &T::observation_end,
      "units", &T::units,
      "output_type", &T::output_type,
      "file_type", &T::file_type,
      "order_by", &T::order_by,
      "sort_order", &T::sort_order,
      "count", &T::count,
      "offset", &T::offset,
      "limit", &T::limit,
      "observations", &T::observations
  );
};
