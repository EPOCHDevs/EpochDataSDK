//
// Created by adesola on 6/10/25.
//

#pragma once
#include <epoch_data_sdk/common/symbol.hpp>
#include <epoch_data_sdk/common/decimal_utils.hpp>
#include <decimal.hh>
#include <epoch_core/macros.h>
#include <epoch_frame/datetime.h>

#include <glaze/glaze.hpp>

namespace glz
{
  template <>
  struct to<JSON, epoch_frame::DateTime>
  {
    template <auto Opts>
    static void op(const epoch_frame::DateTime &x, auto &&...args) noexcept
    {
      const auto y = x.tz().empty() ? x : x.tz_convert("");
      serialize<JSON>::op<Opts>(y.repr(), args...);
    }
  };

  template <>
  struct from<JSON, epoch_frame::DateTime>
  {
    template <auto Opts>
    static void op(epoch_frame::DateTime &value, auto &&...args)
    {
      std::string human_readable;
      parse<JSON>::op<Opts>(human_readable, args...);
      value = epoch_frame::DateTime::from_str(human_readable);
    }
  };

  template <>
  struct to<JSON, epoch_frame::Date>
  {
    template <auto Opts>
    static void op(const epoch_frame::Date &x, auto &&...args) noexcept
    {
      serialize<JSON>::op<Opts>(x.repr(), args...);
    }
  };

  template <>
  struct from<JSON, epoch_frame::Date>
  {
    template <auto Opts>
    static void op(epoch_frame::Date &value, auto &&...args)
    {
      std::string human_readable;
      parse<JSON>::op<Opts>(human_readable, args...);
      value = epoch_frame::DateTime::from_date_str(human_readable).date();
    }
  };

  template <>
  struct meta<epoch_frame::Time>
  {
    using T = epoch_frame::Time;
    static constexpr auto read_x = [](T &value, glz::generic const &time,
                                      glz::context &ctx)
    {
      if (time.is_string())
      {
        for (auto const &[i, comp] : std::views::enumerate(
                 std::views::split(time.as<std::string>(), ':')))
        {
          std::string compStr{comp.begin(), comp.end()};

          switch (i)
          {
          case 0:
            value.hour = chrono_hour(std::stoi(compStr));
            break;
          case 1:
            value.minute = chrono_minute(std::stoi(compStr));
            break;
          case 2:
          {
            auto fraction = compStr.find('.');
            auto seconds = compStr.substr(0, fraction);
            value.second = chrono_second(std::stoi(seconds));

            if (fraction != std::string::npos)
            {
              value.microsecond =
                  chrono_microsecond(std::stoi(seconds.substr(fraction + 1)));
            }
            break;
          }
          default:
            break;
          }
        }
      }
      else if (time.is_object())
      {
        if (auto hour = time["hour"].get_if<double>())
        {
          value.hour = chrono_hour(static_cast<int>(*hour));
        }
        if (auto minute = time["minute"].get_if<double>())
        {
          value.minute = chrono_minute(static_cast<int>(*minute));
        }
        if (auto second = time["second"].get_if<double>())
        {
          value.second = chrono_second(static_cast<int>(*second));
        }
        if (auto microsecond = time["microsecond"].get_if<double>())
        {
          value.microsecond = chrono_microsecond(static_cast<int>(*microsecond));
        }
        if (auto tz = time["tz"].get_if<std::string>())
        {
          value.tz = *tz;
        }
      }
      else
      {
        ctx.error = glz::error_code::constraint_violated;
        ctx.custom_error_message =
            "Invalid time format: " + time.dump().value_or("");
      }
    };

    static constexpr auto write_x = [](const T &value) -> glz::generic
    {
      glz::generic out;
      out["hour"] = value.hour.count();
      out["minute"] = value.minute.count();
      out["second"] = value.second.count();
      out["microsecond"] = value.microsecond.count();
      out["tz"] = value.tz;
      return out;
    };

    static constexpr auto value = glz::custom<read_x, write_x>;
  };

  template <>
  struct to<JSON, decimal::Decimal>
  {
    template <auto Opts>
    static void op(const decimal::Decimal &x, auto &&...args) noexcept
    {
      if (x.isnan())
      {
        serialize<JSON>::op<Opts>(nullptr, args...);
      }
      else
      {
        // Direct serialization without intermediate string conversion
        serialize<JSON>::op<Opts>(data_sdk::fromDecimal(x), args...);
      }
    }
  };

  template <>
  struct from<JSON, decimal::Decimal>
  {
    template <auto Opts>
    static void op(decimal::Decimal &value, auto &&...args)
    {
      generic val;
      parse<JSON>::op<Opts>(val, args...);
      if (val.is_number())
      {
        value = data_sdk::toDecimal(val.get_number());
      }
      else if (val.is_string())
      {
        value = decimal::Decimal(val.get_string());
      }
      else if (val.is_null())
      {
        value = decimal::Decimal();
      }
      else
      {
        throw std::runtime_error("Invalid decimal type");
      }
    }
  };

  template <>
  struct to<JSON, data_sdk::Symbol>
  {
    template <auto Opts>
    static void op(const data_sdk::Symbol &x, auto &&...args) noexcept
    {
      serialize<JSON>::op<Opts>(x.get(), args...);
    }
  };

  template <>
  struct from<JSON, data_sdk::Symbol>
  {
    template <auto Opts>
    static void op(data_sdk::Symbol &value, auto &&...args)
    {
      std::string human_readable;
      parse<JSON>::op<Opts>(human_readable, args...);
      value = data_sdk::Symbol{human_readable};
    }
  };

  inline std::basic_string_view<uint8_t>
  ToUint8tStringView(const std::string &input)
  {
    return {reinterpret_cast<const uint8_t *>(input.data()), input.size()};
  }
  std::string prettify(std::string const &name, auto &&data)
  {
    return std::format("{}:\n{}", name,
                       glz::prettify_json(glz::write_json(data).value()));
  }

  std::string prettify(auto &&data)
  {
    return glz::prettify_json(glz::write_json(data).value());
  }
} // namespace glz
