//
// Created by adesola on 1/4/25.
//

#include <epoch_data_sdk/common/time_frame.hpp>
#include <epoch_core/error_type.h>

#include "date_time/date_offsets.h"
#include "epoch_core/macros.h"
#include "epoch_frame/factory/date_offset_factory.h"
#include <cctype>
#include <glaze/json/generic.hpp>
#include <utility>
#include <variant>

namespace data_sdk
{
  SessionRegistry::SessionRegistry()
  {
    // Trading Sessions (in their local timezones)
    // FX-style business hours for regional sessions
    registry[epoch_core::SessionType::Sydney] =
        SessionRange{
            Time{std::chrono::hours(8), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "Australia/Sydney"},
            Time{std::chrono::hours(17), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "Australia/Sydney"}};

    registry[epoch_core::SessionType::Tokyo] =
        SessionRange{
            Time{std::chrono::hours(9), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "Asia/Tokyo"},
            Time{std::chrono::hours(18), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "Asia/Tokyo"}};

    registry[epoch_core::SessionType::London] =
        SessionRange{
            Time{std::chrono::hours(8), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "Europe/London"},
            Time{std::chrono::hours(17), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "Europe/London"}};

    // New York = FX regional session (NOT equities RTH)
    registry[epoch_core::SessionType::NewYork] =
        SessionRange{
            Time{std::chrono::hours(8), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"},
            Time{std::chrono::hours(17), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"}};

    // --------------------------------------------------------------------
    // ICT/SMC Kill Zones (fixed in ET; use America/New_York for DST handling)
    // Asian Kill Zone: 19:00–23:00 ET
    registry[epoch_core::SessionType::AsianKillZone] =
        SessionRange{
            Time{std::chrono::hours(19), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"},
            Time{std::chrono::hours(23), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"}};

    // London Open Kill Zone: 02:00–05:00 ET
    registry[epoch_core::SessionType::LondonOpenKillZone] =
        SessionRange{
            Time{std::chrono::hours(2), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"},
            Time{std::chrono::hours(5), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"}};

    // New York Kill Zone: 07:00–10:00 ET
    registry[epoch_core::SessionType::NewYorkKillZone] =
        SessionRange{
            Time{std::chrono::hours(7), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"},
            Time{std::chrono::hours(10), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"}};

    // London Close Kill Zone: 10:00–12:00 ET
    registry[epoch_core::SessionType::LondonCloseKillZone] =
        SessionRange{
            Time{std::chrono::hours(10), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"},
            Time{std::chrono::hours(12), std::chrono::minutes(0),
                 std::chrono::seconds(0), std::chrono::microseconds(0),
                 "America/New_York"}};
  }

  std::unordered_map<std::string, epoch_frame::DateOffsetHandlerPtr>
      TIMEFRAME_MAPPING{
          {std::string(tf_str::k1Min), epoch_frame::factory::offset::minutes(1)},
          {std::string(tf_str::k2Min), epoch_frame::factory::offset::minutes(2)},
          {std::string(tf_str::k3Min), epoch_frame::factory::offset::minutes(3)},
          {std::string(tf_str::k5Min), epoch_frame::factory::offset::minutes(5)},
          {std::string(tf_str::k10Min),
           epoch_frame::factory::offset::minutes(10)},
          {std::string(tf_str::k15Min),
           epoch_frame::factory::offset::minutes(15)},
          {std::string(tf_str::k30Min),
           epoch_frame::factory::offset::minutes(30)},
          {std::string(tf_str::k45Min),
           epoch_frame::factory::offset::minutes(45)},
          {std::string(tf_str::k1H), epoch_frame::factory::offset::hours(1)},
          {std::string(tf_str::k2H), epoch_frame::factory::offset::hours(2)},
          {std::string(tf_str::k3H), epoch_frame::factory::offset::hours(3)},
          {std::string(tf_str::k4H), epoch_frame::factory::offset::hours(4)},
          {std::string(tf_str::k1W_SUN),
           epoch_frame::factory::offset::weeks(
               1, epoch_core::EpochDayOfWeek::Sunday)},
          {std::string(tf_str::k1W_MON),
           epoch_frame::factory::offset::weeks(
               1, epoch_core::EpochDayOfWeek::Monday)},
          {std::string(tf_str::k1W_FRI),
           epoch_frame::factory::offset::weeks(
               1, epoch_core::EpochDayOfWeek::Friday)},
          {std::string(tf_str::k1W_MON_1st),
           epoch_frame::factory::offset::week_of_month(
               1, 0, epoch_core::EpochDayOfWeek::Monday)},
          {std::string(tf_str::k1W_MON_2nd),
           epoch_frame::factory::offset::week_of_month(
               1, 1, epoch_core::EpochDayOfWeek::Monday)},
          {std::string(tf_str::k1W_MON_3rd),
           epoch_frame::factory::offset::week_of_month(
               1, 2, epoch_core::EpochDayOfWeek::Monday)},
          {std::string(tf_str::k1W_FRI_Last),
           epoch_frame::factory::offset::last_week_of_month(
               1, epoch_core::EpochDayOfWeek::Friday)},
          {std::string(tf_str::k1D), epoch_frame::factory::offset::days(1)},
          {std::string(tf_str::k1ME), epoch_frame::factory::offset::month_end(1)},
          {std::string(tf_str::k1MS),
           epoch_frame::factory::offset::month_start(1)},
          {std::string(tf_str::k1QE),
           epoch_frame::factory::offset::quarter_end(1)},
          {std::string(tf_str::k1QS),
           epoch_frame::factory::offset::quarter_start(1)},
          {std::string(tf_str::k1YE), epoch_frame::factory::offset::year_end(1)},
          {std::string(tf_str::k1YS),
           epoch_frame::factory::offset::year_start(1)}};

  bool IsIntraday(epoch_core::EpochOffsetType type)
  {
    return (type == epoch_core::EpochOffsetType::Hour ||
            type == epoch_core::EpochOffsetType::Minute ||
            type == epoch_core::EpochOffsetType::Second ||
            type == epoch_core::EpochOffsetType::Milli ||
            type == epoch_core::EpochOffsetType::Micro ||
            type == epoch_core::EpochOffsetType::Nano);
  }

  // Helper: Parse pandas-style offset string (e.g., "5Min", "W-FRI", "1ME")
  // Returns nullptr if the string is not a valid pandas offset pattern
  static epoch_frame::DateOffsetHandlerPtr ParsePandasOffset(const std::string& offset_str)
  {
    if (offset_str.empty())
    {
      return nullptr;
    }

    // Helper: case-insensitive string comparison
    auto iequals = [](const std::string& a, const std::string& b) {
      if (a.size() != b.size()) return false;
      for (size_t i = 0; i < a.size(); ++i)
      {
        if (std::tolower(a[i]) != std::tolower(b[i])) return false;
      }
      return true;
    };

    // Extract number prefix (default to 1 if not present)
    size_t pos = 0;
    int number = 0;
    bool has_number = false;

    while (pos < offset_str.size() && std::isdigit(offset_str[pos]))
    {
      number = number * 10 + (offset_str[pos] - '0');
      has_number = true;
      ++pos;
    }

    if (!has_number)
    {
      number = 1;
    }

    if (pos >= offset_str.size())
    {
      return nullptr; // No unit after number
    }

    // Extract unit and optional anchor
    std::string rest = offset_str.substr(pos);
    std::string unit;
    std::string anchor;

    // Split on '-' for anchored offsets (e.g., "W-FRI", "1W-MON")
    size_t dash_pos = rest.find('-');
    if (dash_pos != std::string::npos)
    {
      unit = rest.substr(0, dash_pos);
      anchor = rest.substr(dash_pos + 1);
    }
    else
    {
      unit = rest;
    }

    // Map unit to DateOffsetHandler (case-insensitive)
    // Minutes: "Min", "min", "MIN", "T"
    if (iequals(unit, "Min") || iequals(unit, "T"))
    {
      return epoch_frame::factory::offset::minutes(number);
    }
    // Hours: "H", "h"
    else if (iequals(unit, "H"))
    {
      return epoch_frame::factory::offset::hours(number);
    }
    // Days: "D", "d"
    else if (iequals(unit, "D"))
    {
      return epoch_frame::factory::offset::days(number);
    }
    // Weeks: "W", "w" (with optional anchor)
    else if (iequals(unit, "W"))
    {
      std::optional<epoch_core::EpochDayOfWeek> weekday = std::nullopt;

      if (!anchor.empty())
      {
        // Map anchor string to day of week (case-insensitive)
        if (iequals(anchor, "MON")) weekday = epoch_core::EpochDayOfWeek::Monday;
        else if (iequals(anchor, "TUE")) weekday = epoch_core::EpochDayOfWeek::Tuesday;
        else if (iequals(anchor, "WED")) weekday = epoch_core::EpochDayOfWeek::Wednesday;
        else if (iequals(anchor, "THU")) weekday = epoch_core::EpochDayOfWeek::Thursday;
        else if (iequals(anchor, "FRI")) weekday = epoch_core::EpochDayOfWeek::Friday;
        else if (iequals(anchor, "SAT")) weekday = epoch_core::EpochDayOfWeek::Saturday;
        else if (iequals(anchor, "SUN")) weekday = epoch_core::EpochDayOfWeek::Sunday;
        else
        {
          return nullptr; // Invalid anchor
        }
      }

      return epoch_frame::factory::offset::weeks(number, weekday);
    }
    // Month End: "ME", "me", "M" (pandas uses ME for month-end)
    else if (iequals(unit, "ME") || iequals(unit, "M"))
    {
      return epoch_frame::factory::offset::month_end(number);
    }
    // Month Start: "MS", "ms"
    else if (iequals(unit, "MS"))
    {
      return epoch_frame::factory::offset::month_start(number);
    }
    // Quarter End: "QE", "qe", "Q"
    else if (iequals(unit, "QE") || iequals(unit, "Q"))
    {
      return epoch_frame::factory::offset::quarter_end(number);
    }
    // Quarter Start: "QS", "qs"
    else if (iequals(unit, "QS"))
    {
      return epoch_frame::factory::offset::quarter_start(number);
    }
    // Year End: "YE", "ye", "Y", "A" (pandas uses A for annual)
    else if (iequals(unit, "YE") || iequals(unit, "Y") || iequals(unit, "A"))
    {
      return epoch_frame::factory::offset::year_end(number);
    }
    // Year Start: "YS", "ys", "AS" (pandas AS for annual start)
    else if (iequals(unit, "YS") || iequals(unit, "AS"))
    {
      return epoch_frame::factory::offset::year_start(number);
    }

    return nullptr; // Unrecognized unit
  }

  TimeFrame::TimeFrame(epoch_frame::DateOffsetHandlerPtr offset)
      : m_offset(std::move(offset))
  {
    AssertFromStream(m_offset != nullptr, "TimeFrame offset cannot be "
                                          "nullptr");
  }

  TimeFrame::TimeFrame(std::string mapping_key)
      : m_created_from_string(true), m_mapping_key(std::move(mapping_key))
  {
    // Fast path: check hardcoded mapping first
    if (TIMEFRAME_MAPPING.contains(m_mapping_key))
    {
      m_offset = TIMEFRAME_MAPPING.at(m_mapping_key);
      return;
    }

    // Slow path: try pandas-style offset parsing
    m_offset = ParsePandasOffset(m_mapping_key);
    AssertFromStream(m_offset != nullptr,
                     "Invalid timeframe: " + m_mapping_key);
  }

  bool TimeFrame::IsIntraDay() const { return IsIntraday(m_offset->type()); }

  std::string TimeFrame::ToString() const
  {
    return m_offset ? m_offset->name() : "";
  }

  bool TimeFrame::operator==(TimeFrame const &other) const
  {
    return m_offset->name() == other.m_offset->name();
  }

  bool TimeFrame::operator!=(TimeFrame const &other) const
  {
    return !(*this == other);
  }

  std::string TimeFrame::Serialize() const
  {
    if (m_created_from_string)
    {
      return m_mapping_key;
    }
    std::string result;
    const auto error = glz::write_json(m_offset, result);
    if (error)
    {
      const auto desc = format_error(error);
      throw std::runtime_error("Failed to serialize timeframe: " + desc);
    }
    return result;
  }

  std::optional<epoch_core::EpochOffsetType>
  toEpochOffsetType(epoch_core::StratifyxTimeFrameType type)
  {
    switch (type)
    {
    case epoch_core::StratifyxTimeFrameType::minute:
      return epoch_core::EpochOffsetType::Minute;
    case epoch_core::StratifyxTimeFrameType::hour:
      return epoch_core::EpochOffsetType::Hour;
    case epoch_core::StratifyxTimeFrameType::day:
      return epoch_core::EpochOffsetType::Day;
    case epoch_core::StratifyxTimeFrameType::week:
      return epoch_core::EpochOffsetType::Week;
    case epoch_core::StratifyxTimeFrameType::week_of_month:
      return epoch_core::EpochOffsetType::WeekOfMonth;
    case epoch_core::StratifyxTimeFrameType::month:
      return epoch_core::EpochOffsetType::Month;
    case epoch_core::StratifyxTimeFrameType::bmonth:
      return epoch_core::EpochOffsetType::BusinessMonth;
    case epoch_core::StratifyxTimeFrameType::quarter:
      return epoch_core::EpochOffsetType::Quarter;
    case epoch_core::StratifyxTimeFrameType::year:
      return epoch_core::EpochOffsetType::Year;
    case epoch_core::StratifyxTimeFrameType::bday:
      return epoch_core::EpochOffsetType::BusinessDay;
    case epoch_core::StratifyxTimeFrameType::session:
      return epoch_core::EpochOffsetType::SessionAnchor;
    default:
      break;
    }
    return std::nullopt;
  }

  epoch_core::StratifyxTimeFrameType
  fromOffset(epoch_core::EpochOffsetType type)
  {
    switch (type)
    {
    case epoch_core::EpochOffsetType::Minute:
      return epoch_core::StratifyxTimeFrameType::minute;
    case epoch_core::EpochOffsetType::Hour:
      return epoch_core::StratifyxTimeFrameType::hour;
    case epoch_core::EpochOffsetType::Day:
      return epoch_core::StratifyxTimeFrameType::day;
    case epoch_core::EpochOffsetType::Week:
      return epoch_core::StratifyxTimeFrameType::week;
    case epoch_core::EpochOffsetType::WeekOfMonth:
    case epoch_core::EpochOffsetType::LastWeekOfMonth:
      return epoch_core::StratifyxTimeFrameType::week_of_month;
    case epoch_core::EpochOffsetType::Month:
    case epoch_core::EpochOffsetType::MonthStart:
    case epoch_core::EpochOffsetType::MonthEnd:
      return epoch_core::StratifyxTimeFrameType::month;
    case epoch_core::EpochOffsetType::BusinessMonth:
    case epoch_core::EpochOffsetType::BusinessMonthStart:
    case epoch_core::EpochOffsetType::BusinessMonthEnd:
      return epoch_core::StratifyxTimeFrameType::bmonth;
    case epoch_core::EpochOffsetType::Quarter:
    case epoch_core::EpochOffsetType::QuarterStart:
    case epoch_core::EpochOffsetType::QuarterEnd:
      return epoch_core::StratifyxTimeFrameType::quarter;
    case epoch_core::EpochOffsetType::Year:
    case epoch_core::EpochOffsetType::YearStart:
    case epoch_core::EpochOffsetType::YearEnd:
      return epoch_core::StratifyxTimeFrameType::year;
    case epoch_core::EpochOffsetType::BusinessDay:
      return epoch_core::StratifyxTimeFrameType::bday;
    case epoch_core::EpochOffsetType::SessionAnchor:
      return epoch_core::StratifyxTimeFrameType::session;
    case epoch_core::EpochOffsetType::RelativeDelta:
      return epoch_core::StratifyxTimeFrameType::week;
    default:
      break;
    }
    throw std::runtime_error("Invalid Timeframe Type: " +
                             epoch_core::EpochOffsetTypeWrapper::ToString(type));
  }

  std::chrono::month toChronoMonth(epoch_core::StratifyxMonth type)
  {
    return std::chrono::month(epoch_core::StratifyxMonthWrapper::toNumber(type) +
                              1);
  }

  epoch_core::StratifyxMonth fromChronoMonth(std::chrono::month type)
  {
    return static_cast<epoch_core::StratifyxMonth>(static_cast<unsigned>(type) -
                                                   1);
  }

  // Helper: build handler from parsed options
  static epoch_frame::DateOffsetHandlerPtr
  MakeHandlerFromOption(DateOffsetOption const &option)
  {
    switch (option.type)
    {
    case epoch_core::StratifyxTimeFrameType::day:
      return epoch_frame::factory::offset::days(option.interval);
    case epoch_core::StratifyxTimeFrameType::hour:
      return epoch_frame::factory::offset::hours(option.interval);
    case epoch_core::StratifyxTimeFrameType::minute:
      return epoch_frame::factory::offset::minutes(option.interval);
    case epoch_core::StratifyxTimeFrameType::week:
    {
      // Convert Null enum value to std::nullopt
      std::optional<epoch_core::EpochDayOfWeek> weekday =
          (option.weekday == epoch_core::EpochDayOfWeek::Null)
              ? std::nullopt
              : std::optional<epoch_core::EpochDayOfWeek>{option.weekday};
      return epoch_frame::factory::offset::weeks(option.interval, weekday);
    }
    case epoch_core::StratifyxTimeFrameType::week_of_month:
    {
      if (option.week_of_month == epoch_core::WeekOfMonth::Last)
      {
        return epoch_frame::factory::offset::last_week_of_month(option.interval,
                                                                option.weekday);
      }

      int week_index = 0; // 0-based index
      switch (option.week_of_month)
      {
      case epoch_core::WeekOfMonth::First:
        week_index = 0;
        break;
      case epoch_core::WeekOfMonth::Second:
        week_index = 1;
        break;
      case epoch_core::WeekOfMonth::Third:
        week_index = 2;
        break;
      case epoch_core::WeekOfMonth::Fourth:
        week_index = 3;
        break;
      default:
        week_index = 0;
        break;
      }
      return epoch_frame::factory::offset::week_of_month(
          option.interval, week_index, option.weekday);
    }
    case epoch_core::StratifyxTimeFrameType::month:
    {
      if (option.anchor == epoch_core::AnchoredTimeFrameType::Start)
      {
        return epoch_frame::factory::offset::month_start(option.interval);
      }
      return epoch_frame::factory::offset::month_end(option.interval);
    }
    case epoch_core::StratifyxTimeFrameType::bmonth:
    {
      // Business month begin/end
      if (option.anchor == epoch_core::AnchoredTimeFrameType::Start)
      {
        return epoch_frame::factory::offset::bmonth_begin(option.interval);
      }
      return epoch_frame::factory::offset::bmonth_end(option.interval);
    }
    case epoch_core::StratifyxTimeFrameType::quarter:
    {
      std::optional<std::chrono::month> m = std::nullopt;
      if (option.month != epoch_core::StratifyxMonth::Null)
      {
        m = toChronoMonth(option.month);
      }
      if (option.anchor == epoch_core::AnchoredTimeFrameType::Start)
      {
        return epoch_frame::factory::offset::quarter_start(option.interval, m);
      }
      return epoch_frame::factory::offset::quarter_end(option.interval, m);
    }
    case epoch_core::StratifyxTimeFrameType::year:
    {
      std::optional<std::chrono::month> m = std::nullopt;
      if (option.month != epoch_core::StratifyxMonth::Null)
      {
        m = toChronoMonth(option.month);
      }
      if (option.anchor == epoch_core::AnchoredTimeFrameType::Start)
      {
        return epoch_frame::factory::offset::year_start(option.interval, m);
      }
      return epoch_frame::factory::offset::year_end(option.interval, m);
    }
    case epoch_core::StratifyxTimeFrameType::bday:
    {
      return epoch_frame::factory::offset::bday(option.interval,
                                                option.time_offset);
    }
    case epoch_core::StratifyxTimeFrameType::session:
    {
      auto which =
          option.session_anchor == epoch_core::SessionAnchorType::AfterOpen
              ? epoch_frame::SessionAnchorWhich::AfterOpen
              : epoch_frame::SessionAnchorWhich::BeforeClose;
      const auto delta = option.time_offset.value_or(
          epoch_frame::TimeDelta{epoch_frame::TimeDelta::Components{}});
      AssertFromStream(option.session.has_value(),
                       "Session timeframe requires a valid session");
      epoch_frame::SessionRange session_range;
      if (std::holds_alternative<epoch_frame::SessionRange>(
              option.session.value()))
      {
        session_range =
            std::get<epoch_frame::SessionRange>(option.session.value());
      }
      else
      {
        session_range = kSessionRegistry.at(
            std::get<epoch_core::SessionType>(option.session.value()));
      }
      return epoch_frame::factory::offset::session_anchor(session_range, which,
                                                          delta, option.interval);
    }
    default:
      break;
    }
    std::unreachable();
  }

  template <typename Serializer>
  epoch_frame::DateOffsetHandlerPtr
  CreateDateOffsetHandler(Serializer const &buffer)
  {
    DateOffsetOption option;
    if constexpr (std::is_same_v<Serializer, glz::generic>)
    {
      if (buffer.is_string())
      {
        auto str = buffer.get_string();
        // Build TimeFrame from string - constructor handles validation & pandas parsing
        try
        {
          return TimeFrame(str).GetOffset();
        }
        catch (const std::exception &e)
        {
          // Re-throw with proper format
          AssertFromStream(false, e.what());
        }
      }
      option = buffer.template as<DateOffsetOption>();
    }
    else if constexpr (std::is_same_v<Serializer, YAML::Node>)
    {
      if (buffer.IsScalar())
      {
        auto str = buffer.template as<std::string>();
        // Build TimeFrame from string - constructor handles validation & pandas parsing
        try
        {
          return TimeFrame(str).GetOffset();
        }
        catch (const std::exception &e)
        {
          // Re-throw with proper format
          AssertFromStream(false, e.what());
        }
      }

      option = buffer.template as<DateOffsetOption>();
    }
    else
    {
      static_assert(false, "Invalid serializer type");
    }

    return MakeHandlerFromOption(option);
  }

  bool TimeFrame::operator<(TimeFrame const &other) const
  {
    const auto m_type = fromOffset(m_offset->type());
    const auto other_type = fromOffset(other.m_offset->type());
    if (m_type == other_type)
      return m_offset->n() < other.m_offset->n();

    return epoch_core::StratifyxTimeFrameTypeWrapper::toNumber(m_type) <
           epoch_core::StratifyxTimeFrameTypeWrapper::toNumber(other_type);
  }

  epoch_frame::DateOffsetHandlerPtr
  CreateDateOffsetHandlerFromJSON(glz::generic const &buffer)
  {
    if (buffer.is_null())
    {
      return nullptr;
    }
    if (buffer.is_string())
    {
      auto str = buffer.as<std::string>();
      // Build TimeFrame from string - constructor handles validation & pandas parsing
      try
      {
        return TimeFrame(str).GetOffset();
      }
      catch (const std::exception &e)
      {
        // Re-throw with proper format
        AssertFromStream(false, e.what());
      }
    }

    // Manually parse JSON into DateOffsetOption to avoid requiring glaze meta
    DateOffsetOption option;
    AssertFromFormat(
        buffer.contains(std::string(tf_str::kType)) &&
            buffer.contains(std::string(tf_str::kInterval)),
        "DateOffsetHandler JSON must contain type and interval fields");

    const auto &type_node = buffer[std::string(tf_str::kType)];
    const auto &interval_node = buffer[std::string(tf_str::kInterval)];
    AssertFromStream(!type_node.is_null(), "Missing required field: type");
    AssertFromStream(!interval_node.is_null(),
                     "Missing required field: interval");
    option.type = epoch_core::StratifyxTimeFrameTypeWrapper::FromString(
        type_node.as<std::string>());
    option.interval = static_cast<uint32_t>(interval_node.as<int64_t>());

    if (buffer.contains(std::string(tf_str::kAnchor)))
    {
      const auto &anchor_node = buffer[std::string(tf_str::kAnchor)];
      if (!anchor_node.is_null())
      {
        option.anchor = epoch_core::AnchoredTimeFrameTypeWrapper::FromString(
            anchor_node.as<std::string>());
      }
    }

    if (buffer.contains(std::string(tf_str::kWeekOfMonth)))
    {
      const auto &wom_node = buffer[std::string(tf_str::kWeekOfMonth)];
      if (!wom_node.is_null())
      {
        option.week_of_month = epoch_core::WeekOfMonthWrapper::FromString(
            wom_node.as<std::string>());
      }
    }

    if (buffer.contains(std::string(tf_str::kWeekday)))
    {
      const auto &weekday_node = buffer[std::string(tf_str::kWeekday)];
      if (!weekday_node.is_null())
      {
        auto weekday_str = weekday_node.as<std::string>();
        // Skip if the string is "Null" - treat it same as JSON null
        if (weekday_str != "Null")
        {
          option.weekday = epoch_core::EpochDayOfWeekWrapper::FromString(weekday_str);
        }
      }
    }

    if (buffer.contains(std::string(tf_str::kMonth)))
    {
      const auto &month_node = buffer[std::string(tf_str::kMonth)];
      if (!month_node.is_null())
      {
        option.month = epoch_core::StratifyxMonthWrapper::FromString(
            month_node.as<std::string>());
      }
    }

    // Optional time_offset for business day offsets
    if (buffer.contains(std::string(tf_str::kTimeOffset)))
    {
      const auto &to_node = buffer[std::string(tf_str::kTimeOffset)];
      if (!to_node.is_null() && to_node.is_object())
      {
        epoch_frame::TimeDelta::Components c{};
        if (to_node.contains(std::string(tf_str::kDays)))
          c.days = to_node[std::string(tf_str::kDays)].as<double>();
        if (to_node.contains(std::string(tf_str::kHours)))
          c.hours = to_node[std::string(tf_str::kHours)].as<double>();
        if (to_node.contains(std::string(tf_str::kMinutes)))
          c.minutes = to_node[std::string(tf_str::kMinutes)].as<double>();
        if (to_node.contains(std::string(tf_str::kSeconds)))
          c.seconds = to_node[std::string(tf_str::kSeconds)].as<double>();
        if (to_node.contains(std::string(tf_str::kMilliseconds)))
          c.milliseconds =
              to_node[std::string(tf_str::kMilliseconds)].as<double>();
        if (to_node.contains(std::string(tf_str::kMicroseconds)))
          c.microseconds =
              to_node[std::string(tf_str::kMicroseconds)].as<double>();
        if (to_node.contains(std::string(tf_str::kWeeks)))
          c.weeks = to_node[std::string(tf_str::kWeeks)].as<double>();
        option.time_offset = epoch_frame::TimeDelta{c};
      }
    }

    // Session-anchored fields
    if (buffer.contains(std::string(tf_str::kSession)))
    {
      const auto &session_node = buffer[std::string(tf_str::kSession)];
      if (!session_node.is_null())
      {
        option.session = epoch_core::SessionTypeWrapper::FromString(
            session_node.as<std::string>());
      }
    }
    if (buffer.contains(std::string(tf_str::kSessionAnchor)))
    {
      const auto &sa_node = buffer[std::string(tf_str::kSessionAnchor)];
      if (!sa_node.is_null())
      {
        option.session_anchor = epoch_core::SessionAnchorTypeWrapper::FromString(
            sa_node.as<std::string>());
      }
    }

    return MakeHandlerFromOption(option);
  }

  glz::generic
  CreateDateOffsetHandlerJSON(epoch_frame::DateOffsetHandlerPtr const &x)
  {
    glz::generic result;
    if (!x)
    {
      return result; // null json
    }

    const auto tf_type = fromOffset(x->type());
    result[std::string(tf_str::kType)] =
        epoch_core::StratifyxTimeFrameTypeWrapper::ToString(tf_type);
    result[std::string(tf_str::kInterval)] = static_cast<int64_t>(x->n());

    // Add anchor information for anchored types
    if (tf_type == epoch_core::StratifyxTimeFrameType::month ||
        tf_type == epoch_core::StratifyxTimeFrameType::quarter ||
        tf_type == epoch_core::StratifyxTimeFrameType::year)
    {
      const auto anchor = x->is_end() ? epoch_core::AnchoredTimeFrameType::End
                                      : epoch_core::AnchoredTimeFrameType::Start;
      result[std::string(tf_str::kAnchor)] =
          epoch_core::AnchoredTimeFrameTypeWrapper::ToString(anchor);
      if (tf_type == epoch_core::StratifyxTimeFrameType::quarter)
      {
        auto handler = dynamic_cast<epoch_frame::QuarterOffsetHandler *>(x.get());
        if (handler)
        {
          result[std::string(tf_str::kMonth)] =
              epoch_core::StratifyxMonthWrapper::ToString(
                  fromChronoMonth(handler->get_starting_month()));
        }
      }
      if (tf_type == epoch_core::StratifyxTimeFrameType::year)
      {
        auto handler = dynamic_cast<epoch_frame::YearOffsetHandler *>(x.get());
        if (handler)
        {
          result[std::string(tf_str::kMonth)] =
              epoch_core::StratifyxMonthWrapper::ToString(
                  fromChronoMonth(handler->get_month()));
        }
      }
    }

    // Best-effort: include weekday for weekly offsets if present in the code
    // string
    if (tf_type == epoch_core::StratifyxTimeFrameType::week)
    {
      if (auto week_handler = dynamic_cast<epoch_frame::WeekHandler *>(x.get()))
      {
        auto weekday = week_handler->get_weekday();
        if (weekday && weekday.value() != epoch_core::EpochDayOfWeek::Null)
        {
          result[std::string(tf_str::kWeekday)] =
              epoch_core::EpochDayOfWeekWrapper::ToString(weekday.value());
        }
      }
      if (auto rd_handler =
              dynamic_cast<epoch_frame::RelativeDeltaOffsetHandler *>(x.get()))
      {
        auto weekday = rd_handler->get_relative_delta().weekday();
        if (weekday)
        {
          result[std::string(tf_str::kWeekday)] =
              epoch_core::EpochDayOfWeekWrapper::ToString(weekday->weekday());
          auto n = weekday->n().value_or(1);
          auto wom = epoch_core::WeekOfMonth::Null;
          if (n == 1)
          {
            wom = epoch_core::WeekOfMonth::First;
          }
          else if (n == 2)
          {
            wom = epoch_core::WeekOfMonth::Second;
          }
          else if (n == 3)
          {
            wom = epoch_core::WeekOfMonth::Third;
          }
          else if (n == 4)
          {
            wom = epoch_core::WeekOfMonth::Fourth;
          }
          result[std::string(tf_str::kWeekOfMonth)] =
              epoch_core::WeekOfMonthWrapper::ToString(wom);
        }
      }
    }

    return result;
  }

} // namespace data_sdk

// YAML helper surface for a concise timeframe mapping
namespace data_sdk
{
  TimeFrame CreateTimeFrameFromYAML(YAML::Node const &node)
  {
    auto offset = CreateDateOffsetHandler(node);
    return TimeFrame(offset);
  }
} // namespace data_sdk

namespace YAML
{
  bool convert<epoch_frame::DateOffsetHandlerPtr>::decode(
      const Node &node, epoch_frame::DateOffsetHandlerPtr &rhs)
  {
    rhs = data_sdk::CreateDateOffsetHandler(node);
    return true;
  }

  bool convert<DateOffsetOption>::decode(const Node &node,
                                         DateOffsetOption &rhs)
  {
    rhs.type = epoch_core::StratifyxTimeFrameTypeWrapper::FromString(
        node[std::string(data_sdk::tf_str::kType)].as<std::string>());
    rhs.interval =
        node[std::string(data_sdk::tf_str::kInterval)].as<uint32_t>(1);
    rhs.anchor = epoch_core::AnchoredTimeFrameTypeWrapper::FromString(
        node[std::string(data_sdk::tf_str::kAnchor)].as<std::string>(
            std::string(data_sdk::tf_str::kAnchorStart)));
    rhs.week_of_month = epoch_core::WeekOfMonthWrapper::FromString(
        node[std::string(data_sdk::tf_str::kWeekOfMonth)].as<std::string>(
            std::string(data_sdk::tf_str::kNull)));
    rhs.weekday = epoch_core::EpochDayOfWeekWrapper::FromString(
        node[std::string(data_sdk::tf_str::kWeekday)].as<std::string>(
            std::string(data_sdk::tf_str::kNull)));
    rhs.month = epoch_core::StratifyxMonthWrapper::FromString(
        node[std::string(data_sdk::tf_str::kMonth)].as<std::string>(
            std::string(data_sdk::tf_str::kNull)));
    if (node[std::string(data_sdk::tf_str::kTimeOffset)])
    {
      const auto &to_node =
          node[std::string(data_sdk::tf_str::kTimeOffset)];
      epoch_frame::TimeDelta::Components c{};
      c.days = to_node[std::string(data_sdk::tf_str::kDays)].as<double>(0);
      c.hours =
          to_node[std::string(data_sdk::tf_str::kHours)].as<double>(0);
      c.minutes =
          to_node[std::string(data_sdk::tf_str::kMinutes)].as<double>(0);
      c.seconds =
          to_node[std::string(data_sdk::tf_str::kSeconds)].as<double>(0);
      c.milliseconds =
          to_node[std::string(data_sdk::tf_str::kMilliseconds)].as<double>(
              0);
      c.microseconds =
          to_node[std::string(data_sdk::tf_str::kMicroseconds)].as<double>(
              0);
      c.weeks =
          to_node[std::string(data_sdk::tf_str::kWeeks)].as<double>(0);
      rhs.time_offset = epoch_frame::TimeDelta{c};
    }
    rhs.session = epoch_core::SessionTypeWrapper::FromString(
        node[std::string(data_sdk::tf_str::kSession)].as<std::string>(
            std::string(data_sdk::tf_str::kNull)));
    rhs.session_anchor = epoch_core::SessionAnchorTypeWrapper::FromString(
        node[std::string(data_sdk::tf_str::kSessionAnchor)].as<std::string>(
            std::string(data_sdk::tf_str::kNull)));
    return true;
  }
} // namespace YAML