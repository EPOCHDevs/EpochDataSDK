#pragma once

#include <string>

namespace data_sdk::trading_economics {

// HTTP error information
struct HttpError {
  int http_status;       // HTTP status code (e.g., 404, 500)
  std::string message;   // Error message
  std::string details;   // Additional error details (optional)

  HttpError(int status, std::string msg, std::string det = "")
      : http_status(status), message(std::move(msg)), details(std::move(det)) {}
};

} // namespace data_sdk::trading_economics
