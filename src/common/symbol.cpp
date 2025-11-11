//
// Created by dewe on 1/10/23.
//
#include <epoch_data_sdk/common/symbol.hpp>

namespace data_sdk {

InvalidSymbol::InvalidSymbol(std::string _symbol_base)
    : symbol_base(std::move(_symbol_base)) {}

const char *InvalidSymbol::what() const noexcept {
  std::stringstream ss;
  ss << "InvalidSymbolError: " << symbol_base;
  msg = ss.str();
  return msg.c_str();
}

Symbol::Symbol(std::string const &_sym) : m_symbol(_sym) {
  if (_sym.empty()) {
    throw InvalidSymbol("Empty string");
  }
  static std::string VALID_SYMBOLS{" .~^_$@!"};
  std::vector<char> non_alphanumerics{};
  std::ranges::copy_if(
      m_symbol, std::back_inserter(non_alphanumerics), [](char c) noexcept {
        return !(std::isalnum(c) || VALID_SYMBOLS.find(c) != std::string::npos);
      });
  if (non_alphanumerics.size() > 1) {
    std::stringstream ss;
    ss << m_symbol << " has more than one alpha num character";
    throw InvalidSymbol(ss.str());
  }
  if (non_alphanumerics.size() == 1 && non_alphanumerics[0] != '-') {
    std::stringstream ss;
    ss << m_symbol << " has invalid character " << non_alphanumerics.front();
    throw InvalidSymbol(ss.str());
  }
}

Symbol &Symbol::operator=(std::string const &s) {
  *this = Symbol(s);
  return *this;
}

std::string Symbol::operator+(std::string const &s) const {
  auto temp = this->m_symbol;
  return temp.append("::").append(s);
}

std::string Symbol::operator+(const char *s) const {
  auto temp = this->m_symbol;
  return temp.append("::").append(s);
}

std::ostream &operator<<(std::ostream &os, Symbol const &sym) {
  os << sym.m_symbol;
  return os;
}

std::ostream &operator<<(std::ostream &os, SymbolSet const &symbols) {
  os << SymbolList(symbols.begin(), symbols.end());
  return os;
}

std::ostream &operator<<(std::ostream &os, SymbolList const &symbols) {
  os << "Symbols(";
  std::copy(symbols.begin(), symbols.end(),
            std::ostream_iterator<Symbol>(os, " "));
  os << ")\n";
  return os;
}

} // namespace data_sdk