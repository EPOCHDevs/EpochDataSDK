#include "catch.hpp"
#include "epoch_script/core/symbol.h"

using namespace data_sdk;

TEST_CASE("Symbol class construction and assignment", "[symbol]") {
  SECTION("Construction with string") {
    std::string symbol_string = "AAPL";
    Symbol s(symbol_string);
    REQUIRE(s.get() == symbol_string);
  }

  SECTION("Assignment with Symbol") {
    std::string symbol_string = "AAPL";
    Symbol s1("IBM"), s2(symbol_string);
    s1 = s2;
    REQUIRE(s1.get() == symbol_string);
  }
}

TEST_CASE("Symbol class comparison operators", "[symbol]") {
  std::string symbol_string1 = "AAPL";
  std::string symbol_string2 = "GOOG";
  Symbol s1(symbol_string1), s2(symbol_string2);

  REQUIRE(s1 < s2);
  REQUIRE(s1 < symbol_string2);
  REQUIRE(symbol_string1 < s2);
}

TEST_CASE("Symbol class stream output", "[symbol]") {
  std::string symbol_string = "AAPL";
  Symbol s(symbol_string);

  std::stringstream ss;
  ss << s;

  REQUIRE(ss.str() == symbol_string);
}

TEST_CASE("Symbols with currencies", "[symbol]") {
  SECTION("With Valid Seperators") {
    REQUIRE(Symbol("BTC-USD").get() == "BTC-USD");
    REQUIRE(Symbol("XYZ   C22012001000000").get() == "XYZ   C22012001000000");
  }

  SECTION("With Invalid Seperators") {
    REQUIRE_THROWS_AS(Symbol("BTC/USD"), InvalidSymbol);
    //        REQUIRE_THROWS_AS(Symbol("BTC USD"), InvalidSymbol);
    REQUIRE_THROWS_AS(Symbol("BTC/USD/USD"), InvalidSymbol);
    REQUIRE_THROWS_AS(Symbol("BTC-USD-USD"), InvalidSymbol);
  }

  SECTION("No Seperators") { REQUIRE(Symbol("BTCUSD").get() == "BTCUSD"); }
}

TEST_CASE("Symbol from literal", "[symbol]") {
  REQUIRE(Symbol("AAPL") == "AAPL"_sym);
}

TEST_CASE("Add Prefix to Symbol", "[symbol]") {
  REQUIRE(Symbol("BTC-USD").AddPrefix('X') == "X:BTC-USD");
}

TEST_CASE("Remove Seperator of Symbol", "[symbol]") {
  REQUIRE(Symbol("BTC-USD").RemoveSeperator() == "BTCUSD"_sym);
  REQUIRE(Symbol("BTCUSD").RemoveSeperator() == "BTCUSD"_sym);
  REQUIRE(Symbol("BTC.USD.20220111").RemoveSeperator() ==
          "BTC.USD.20220111"_sym);
}