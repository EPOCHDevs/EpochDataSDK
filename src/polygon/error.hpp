#pragma once

#include <optional>
#include <string>

namespace data_sdk::polygon {

struct HttpError {
  int http_status{0};
  std::string message;
  std::string request_id;
  std::optional<int> rate_limit_remaining;
  std::optional<int> rate_limit_limit;
};

} // namespace data_sdk::polygon


