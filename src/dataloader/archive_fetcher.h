#pragma once
#include "epoch_frame/common.h"
#include <epoch_data_sdk/dataloader/fetcher.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>
#include <epoch_data_sdk/model/asset/constants.hpp>
#include <epoch_frame/serialization.h>

namespace data_sdk::dataloader {

class ArchiveDataFetcher : public IDataFetcher {
public:
  ArchiveDataFetcher(std::string const &archivePath)
      : m_archivePath(archivePath) {}

  std::expected<epoch_frame::DataFrame, std::string>
  Fetch(const asset::Asset &asset, DataCategory category,
        const epoch_frame::Date &fromDate,
        const epoch_frame::Date &toDate,
        const FetchKwargs &kwargs = NoKwargs{}) const override {
    using namespace epoch_frame;
    // Note: kwargs are currently unused for archive fetching
    // Future: Could use FinancialsKwargs to select different archive files by timeframe
    (void)kwargs;

    auto source = std::filesystem::path(m_archivePath);
    auto base = (source / epoch_core::DataCategoryWrapper::ToString(category) /
                 epoch_core::AssetClassWrapper::ToLongFormString(
                     asset.GetAssetClass()) /
                 asset.GetSymbolStr());
    const bool isFuturesMinute =
        (asset.GetAssetClass() == epoch_core::AssetClass::Futures &&
         category == DataCategory::MinuteBars);
    auto filter = [&](DataFrame df) {
      df = df.set_index(df.index()->tz_localize("UTC"));
      const Scalar startTs{epoch_frame::DateTime{fromDate, {.tz = "UTC"}}};
      const Scalar endTs{epoch_frame::DateTime{toDate, {.tz = "UTC"}}};
      return df.sort_index().loc({startTs, endTs});
    };
    if (!isFuturesMinute) {
      auto res = read_parquet(
          base.string() + ".parquet.gzip",
          {.index_column = std::string(data_sdk::asset::DEFAULT_TIMESTAMP_COLUMN)});
      if (!res.ok())
        return std::unexpected(res.status().message());
      return filter(res.MoveValueUnsafe());
    }
    std::vector<epoch_frame::DataFrame> frames;
    for (auto const &dir : std::filesystem::directory_iterator(base)) {
      auto res = read_parquet(
          dir.path(),
          {.index_column = std::string(data_sdk::asset::DEFAULT_TIMESTAMP_COLUMN)});
      if (!res.ok())
        continue;
      auto df = filter(res.MoveValueUnsafe());
      if (!df.empty())
        frames.push_back(df);
    }
    if (frames.empty())
      return std::unexpected("No Data Found.");
    std::vector<epoch_frame::FrameOrSeries> fos;
    for (auto &f : frames)
      fos.push_back(f);
    return epoch_frame::concat({.frames = fos});
  }

  // Async version - just calls sync version for archive (local files are fast)
  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  FetchAsync(const asset::Asset &asset, DataCategory category,
             const epoch_frame::Date &fromDate,
             const epoch_frame::Date &toDate,
             const FetchKwargs &kwargs = NoKwargs{}) const override {
    // Archive fetching is local file I/O, relatively fast
    // For simplicity, just call sync version directly
    co_return Fetch(asset, category, fromDate, toDate, kwargs);
  }

private:
  std::string m_archivePath;
};

} // namespace data_sdk::dataloader
