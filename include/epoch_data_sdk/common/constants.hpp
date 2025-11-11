#pragma once
#include <string_view>
#include <string>

namespace data_sdk {

// S3 archive path configuration
constexpr std::string_view EPOCH_DB_S3 = "s3://epoch-db";

// Column name constants for OHLCV data
struct ColumnConstants {
  static const ColumnConstants &instance() {
    static ColumnConstants instance;
    return instance;
  }

  std::string OPEN() const { return "o"; }
  std::string CLOSE() const { return "c"; }
  std::string HIGH() const { return "h"; }
  std::string LOW() const { return "l"; }
  std::string VOLUME() const { return "v"; }
  std::string ASK() const { return "ap"; }
  std::string BID() const { return "bp"; }
  std::string ASK_VOLUME() const { return "av"; }
  std::string BID_VOLUME() const { return "bv"; }
  std::string PRICE() const { return "p"; }
  std::string CONTRACT() const { return "s"; }
  std::string OPEN_INTEREST() const { return "oi"; }
  std::string IV() const { return "iv"; }
  std::string DELTA() const { return "delta"; }
  std::string GAMMA() const { return "gamma"; }
  std::string VEGA() const { return "vega"; }
  std::string THETA() const { return "theta"; }
  std::string RHO() const { return "rho"; }
  std::string TIMESTAMP() const { return "t"; }
};

} // namespace data_sdk
