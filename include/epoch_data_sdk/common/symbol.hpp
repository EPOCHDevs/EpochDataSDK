#pragma once
//
// Created by dewe on 7/29/22.
//
#include "algorithm"
#include "ranges"
#include "set"
#include "sstream"
#include "string"
#include "unordered_map"
#include "unordered_set"
#include <filesystem>
#include <format>
#include <regex>
#include <vector>

namespace data_sdk {

struct InvalidSymbol : std::exception {
  std::string symbol_base;
  mutable std::string msg;

  explicit InvalidSymbol(std::string _symbol_base);

  const char *what() const noexcept override;
};

struct Symbol {
  Symbol(std::string const &_sym);

  explicit Symbol(const char *_sym) : Symbol(std::string(_sym)) {}

  [[nodiscard]] inline auto get() const { return m_symbol; }

  Symbol(Symbol const &) = default;

  Symbol &operator=(Symbol const &s) = default;

  Symbol &operator=(std::string const &s);

  std::string AddPrefix(char prefix) const {
    return std::string(1, prefix).append(1, ':').append(m_symbol);
  }

  Symbol RemoveSeperator() const {
    size_t loc = m_symbol.find('-');
    return loc == std::string::npos ? m_symbol
                                    : std::string{m_symbol}.replace(loc, 1, "");
  }

  std::string operator+(std::string const &s) const;

  std::string operator+(const char *s) const;

  auto operator<=>(Symbol const &s) const = default;

  inline auto operator<=>(std::string const &other) const {
    return m_symbol <=> other;
  }

  friend std::ostream &operator<<(std::ostream &os, Symbol const &sym);

private:
  Symbol() = default;

  std::string m_symbol{};
};

inline Symbol operator"" _sym(const char *s, size_t n) {
  return Symbol{std::string(s, n)};
}

struct SymbolHash {
  inline size_t operator()(Symbol const &k) const {
    return std::hash<std::string>{}(k.get());
  }
};

template <class V> using SymbolMap = std::unordered_map<Symbol, V, SymbolHash>;
using SymbolSet = std::set<Symbol>;
using SymbolList = std::vector<Symbol>;

std::ostream &operator<<(std::ostream &os, SymbolSet const &symbols);
std::ostream &operator<<(std::ostream &os, SymbolList const &symbols);

template <template <class> class T>
static SymbolSet toSymbolSet(T<std::string> const &container) {
  SymbolList _symbols(container.size());
  std::ranges::transform(container, _symbols.begin(),
                         [](std::string const &s) { return Symbol(s); });
  return {std::set(_symbols.begin(), _symbols.end())};
}

} // namespace data_sdk

// std::formatter specialization for Symbol
namespace std {
template <>
struct formatter<data_sdk::Symbol> : formatter<std::string> {
  auto format(data_sdk::Symbol const &sym, auto &ctx) const {
    return formatter<std::string>::format(sym.get(), ctx);
  }
};
} // namespace std