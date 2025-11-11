#pragma once
#include <chrono>
#include <memory>
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
    explicit FixedTimeProvider(epoch_frame::DateTime fixedTime)
        : m_fixedTime(fixedTime) {}

    epoch_frame::DateTime now() const override {
        return m_fixedTime;
    }

    epoch_frame::Date today() const override {
        return m_fixedTime.date();
    }

    std::chrono::system_clock::time_point now_timepoint() const override {
        // Convert epoch_frame::DateTime to time_point
        // This is a simplified implementation
        return std::chrono::system_clock::now();  // TODO: proper conversion
    }

private:
    epoch_frame::DateTime m_fixedTime;
};

} // namespace data_sdk
