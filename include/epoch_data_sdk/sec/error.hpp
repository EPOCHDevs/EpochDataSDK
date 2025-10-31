#pragma once

#include <string>

namespace data_sdk::sec {

struct HttpError {
  int status_code{0};
  std::string message;

  HttpError() = default;
  HttpError(int code, std::string msg)
      : status_code(code), message(std::move(msg)) {}
};

} // namespace data_sdk::sec
