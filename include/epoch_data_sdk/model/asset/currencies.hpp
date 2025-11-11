#pragma once
//
// Created by dewe on 5/15/23.
//
#include <epoch_core/enum_wrapper.h>

// Country Currencies (from FX pairs + required)
CREATE_ENUM(CountryCurrency, AED, AFN, ALL, AMD, ANG, ARS, AUD, AWG, BAM, BBD, BDT, BGN, BHD, BIF, BMD, BND, BOB,
    BOC, BRL, BRP, BSD, BTN, BWP, BZD, CAD, CDF, CHF, CLP, CNH, CNY, COP, CRC, CUP, CVE, CYP, CZK, DCN, DJF, DKK,
    DOP, DTR, DZD, ECB, EGP, ETB, EUR, FJD, GBP, GEL, GHS, GMD, GNF, GTQ, GYD, HKD, HNL, HRK, HTG, HUF, HUY, IDR,
    ILS, INR, IQD, IRR, ISK, JMD, JOD, JPY, KES, KGS, KHR, KMF, KRW, KWD, KYD, KZT, LAK, LBP, LKR, LRD, LSL, LTL,
    LYD, MAD, MDL, MGA, MKD, MMK, MOP, MTL, MUR, MVR, MWK, MXN, MYR, MZN, NAD, NGN, NIO, NOK, NPR, NZD, OMR, PAB, PEN,
    PGK, PHP, PKR, PLN, PYG, QAR, RON, RSD, RUB, RWF, SAR, SCR, SDG, SEK, SGD, SHP, SOS, SSG, SUS, SVC, SZL, THB, TJS,
    TMT, TND, TRY, TTD, TWD, TZS, UAH, UGX, USD, UYU, UZS, VES, VND, XAF, XAG, XAU, XCD, XDR, XOF, XPF, YER, ZAR, ZMW);

// Crypto Currencies (from crypto pairs + required)
CREATE_ENUM(CryptoCurrency, ADA, ATOM, BCH, BNB, BTC, BUSD, DAI, DOGE, DOT, ETC, ETH, LTC, MATIC, SOL, TRX, TUSD, UNI,
    USDC, USDT, XLM, XMR, XRP);
