#pragma once
//
// Created by dewe on 5/15/23.
//
#include <epoch_data_sdk/model/asset/currencies.hpp>

// Category,Exchanges
// Currencies,"ICESI, ICEUS, GBLX, CME"
// Indices,"EUREX, ICEUS, CBOT, HKFE, GBLX, CME, CBOTM"
// Financials,"ICEUS, CBOT, CME"
// Metals,"NYMEX, CXMI, COMEX"
// Meats,CME
// Softs,"NYMEX, ICEUS, CME"
// Energies,"NYMEX, NYMI, CBOT"
// Grains,"KCBT, CBOT, CBOTM"
// COINBASE MUST BE KEPT LAST -> ORDER BY EOD CLOSE TIME
CREATE_ENUM(Exchange, HKFE, EUREX, NYSE, NASDAQ, XNAS, XNYS, AMEX, ARCX, BATS, XASE, CBOT, CBOTM, KCBT, CME, NYMEX,
  CBOE, COMEX, NYMI, ICEUS, ICESI, GBLX, CXMI, FX, INDEX, CRYPTO, COINBASE);


/*
EXTRACTION STATISTICS:
==============================
Total exchanges found: 25
FX base currencies: 141 - ['AED', 'AFN', 'ALL', 'AMD', 'ANG', 'ARS', 'AUD', 'AWG', 'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BRL', 'BSD', 'BTN', 'BWP', 'BZD', 'CAD', 'CDF', 'CHF', 'CLP', 'CNH', 'CNY', 'COP', 'CRC', 'CUP', 'CVE', 'CYP', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'ECB', 'EGP', 'ETB', 'EUR', 'FJD', 'GBP', 'GHS', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF', 'IDR', 'ILS', 'INR', 'IQD', 'ISK', 'JMD', 'JOD', 'JPY', 'KES', 'KHR', 'KMF', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MOP', 'MTL', 'MUR', 'MVR', 'MWK', 'MXN', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SCR', 'SEK', 'SGD', 'SHP', 'SOS', 'SSG', 'SUS', 'SVC', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'UYU', 'UZS', 'VES', 'VND', 'XAF', 'XAG', 'XAU', 'XCD', 'XOF', 'XPF', 'YER', 'ZAR', 'ZMW']
FX counter currencies: 146 - ['AED', 'AFN', 'ALL', 'AMD', 'ANG', 'ARS', 'AUD', 'AWG', 'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BOC', 'BRL', 'BRP', 'BSD', 'BTN', 'BWP', 'BZD', 'CAD', 'CDF', 'CHF', 'CLP', 'CNH', 'CNY', 'COP', 'CRC', 'CUP', 'CVE', 'CYP', 'CZK', 'DCN', 'DJF', 'DKK', 'DOP', 'DTR', 'DZD', 'EGP', 'ETB', 'EUR', 'FJD', 'GBP', 'GEL', 'GHS', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF', 'HUY', 'IDR', 'ILS', 'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD', 'JPY', 'KES', 'KGS', 'KHR', 'KMF', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MOP', 'MTL', 'MUR', 'MVR', 'MWK', 'MXN', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SCR', 'SDG', 'SEK', 'SGD', 'SOS', 'SVC', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'UYU', 'UZS', 'VES', 'VND', 'XAF', 'XCD', 'XDR', 'XOF', 'XPF', 'YER', 'ZAR', 'ZMW']
Crypto base currencies: 21 - ['ADA', 'ATOM', 'BCH', 'BTC', 'BUSD', 'DAI', 'DOGE', 'DOT', 'ETC', 'ETH', 'LTC', 'MATIC', 'SOL', 'TRX', 'TUSD', 'UNI', 'USDC', 'USDT', 'XLM', 'XMR', 'XRP']
Crypto counter currencies: 1 - ['USD']
Final country currencies: 152
Final crypto currencies: 22

NOTE: All 'currency' fields in asset specs should be 'USD' for now.
Base/counter currencies are extracted from ticker symbols and trading pairs.
*/

namespace data_sdk::asset {
inline epoch_core::CountryCurrency
GetCurrencyFromExchange(epoch_core::Exchange) {
  return epoch_core::CountryCurrency::USD;
}

template <class T>
using ExchangeMap = std::unordered_map<epoch_core::Exchange, T>;
} // namespace data_sdk::asset