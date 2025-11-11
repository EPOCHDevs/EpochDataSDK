#pragma once
#include <chrono>
#include <memory>
#include <mutex>
#include <epoch_frame/datetime.h>

namespace data_sdk {

// Interface for getting current time (allows mocking in tests)
class ITimeProvider {
public:
    virtual ~ITimeProvider() = default;

    virtual epoch_frame::DateTime now() const = 0;
    virtual epoch_frame::Date today() const = 0;
    virtual std::chrono::system_clock::time_point now_timepoint() const = 0;
};

using TimeProviderPtr = std::shared_ptr<ITimeProvider>;

// Real-time provider implementation
class RealTimeProvider : public ITimeProvider {
public:
    epoch_frame::DateTime now() const override {
        return epoch_frame::DateTime::now();
    }

    epoch_frame::Date today() const override {
        return epoch_frame::DateTime::now().date();
    }

    std::chrono::system_clock::time_point now_timepoint() const override {
        return std::chrono::system_clock::now();
    }
};

// Fixed time provider for testing
class FixedTimeProvider : public ITimeProvider {
public:
    explicit FixedTimeProvider(epoch_frame::DateTime fixedTime = epoch_frame::DateTime::now())
        : m_fixedTime(fixedTime) {}

    epoch_frame::DateTime now() const override {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_fixedTime;
    }

    epoch_frame::Date today() const override {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_fixedTime.date();
    }

    std::chrono::system_clock::time_point now_timepoint() const override {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_fixedTime.to_time_point();
    }

    // Test helper methods
    void advance_by(std::chrono::milliseconds duration) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration);
        m_fixedTime = m_fixedTime + microseconds;
    }

    void advance_by_days(int days) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto new_date = m_fixedTime.date() + chrono_days(days);
        m_fixedTime = epoch_frame::DateTime(new_date, m_fixedTime.time());
    }

    void set_time(epoch_frame::DateTime new_time) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_fixedTime = new_time;
    }

    void set_date(epoch_frame::Date new_date) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_fixedTime = epoch_frame::DateTime(new_date, m_fixedTime.time());
    }

private:
    mutable std::mutex m_mutex;
    epoch_frame::DateTime m_fixedTime;
};

} // namespace data_sdk
