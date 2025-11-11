#pragma once
#include "archive_fetcher.h"
#include <epoch_data_sdk/dataloader/fetcher.hpp>
#include "polygon_fetcher.h"

namespace data_sdk::dataloader {

class DefaultFetcherProvider : public IFetcherProvider {
public:
  DefaultFetcherProvider(std::string const &archivePath)
      : m_archive(archivePath) {}

  IDataFetcher &Get(const asset::Asset &asset, DataCategory) const override {
    // Simple shared instances - the cache race condition was the actual issue
    switch (asset.GetSpec().GetMarketDataVendor()) {
    case epoch_core::MarketDataVendor::Polygon:
      return m_polygon;
    default:
      return m_archive;
    }
  }

private:
  mutable PolygonDataFetcher m_polygon;
  mutable ArchiveDataFetcher m_archive;
};

} // namespace data_sdk::dataloader
