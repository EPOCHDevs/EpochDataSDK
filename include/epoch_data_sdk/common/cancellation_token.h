#pragma once
#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>

namespace data_sdk::events {

class OperationCancelledException : public std::runtime_error {
public:
    OperationCancelledException()
        : std::runtime_error("Pipeline execution was cancelled") {}

    explicit OperationCancelledException(const std::string& message)
        : std::runtime_error(message) {}
};

class CancellationToken {
public:
    CancellationToken() = default;

    CancellationToken(const CancellationToken&) = delete;
    CancellationToken& operator=(const CancellationToken&) = delete;
    CancellationToken(CancellationToken&&) = default;
    CancellationToken& operator=(CancellationToken&&) = default;

    void Cancel() noexcept {
        m_cancelled.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool IsCancelled() const noexcept {
        return m_cancelled.load(std::memory_order_acquire);
    }

    void ThrowIfCancelled() const {
        if (IsCancelled()) {
            throw OperationCancelledException();
        }
    }

    void ThrowIfCancelled(const std::string& context) const {
        if (IsCancelled()) {
            throw OperationCancelledException("Operation cancelled: " + context);
        }
    }

    void Reset() noexcept {
        m_cancelled.store(false, std::memory_order_release);
    }

    explicit operator bool() const noexcept {
        return IsCancelled();
    }

private:
    std::atomic<bool> m_cancelled{false};
};

using CancellationTokenPtr = std::shared_ptr<CancellationToken>;

inline CancellationTokenPtr MakeCancellationToken() {
    return std::make_shared<CancellationToken>();
}

} // namespace data_sdk::events
