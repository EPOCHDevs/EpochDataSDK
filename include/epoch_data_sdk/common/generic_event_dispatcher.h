#pragma once
#include "generic_event_filter.h"
#include <boost/signals2/signal.hpp>
#include <memory>
#include <mutex>

namespace data_sdk::events {

// Signal types are defined in generic_event_types.h

/**
 * IGenericEventDispatcher - Interface for event emission and subscription
 *
 * Thread-safe: Can safely emit and subscribe from multiple threads.
 * Uses boost::signals2 which handles thread safety internally.
 */
class IGenericEventDispatcher {
public:
    virtual ~IGenericEventDispatcher() = default;

    /// Emit an event to all matching subscribers
    virtual void Emit(GenericEvent event) = 0;

    /// Emit an event with a specific path (convenience)
    virtual void Emit(GenericEvent event, const EventPath& path) = 0;

    /// Subscribe to events matching the given filter
    virtual boost::signals2::connection Subscribe(
        GenericEventSlot handler,
        GenericEventFilter filter = GenericEventFilter::All()) = 0;

    /// Subscribe to a specific event type with type-safe handler
    template <typename T>
    boost::signals2::connection SubscribeTo(
        std::function<void(const T&)> handler,
        GenericEventFilter additionalFilter = GenericEventFilter::All()) {

        auto typeFilter = GenericEventFilter::OnlyTypes({GenericEventTypeFor<T>::value});
        auto combinedFilter = typeFilter & additionalFilter;

        return Subscribe(
            [handler = std::move(handler)](const GenericEvent& event) {
                if (const auto* typed = std::get_if<T>(&event)) {
                    handler(*typed);
                }
            },
            combinedFilter
        );
    }

    /// Subscribe to events from a specific path prefix
    boost::signals2::connection SubscribeToPath(
        GenericEventSlot handler,
        const EventPath& pathPrefix) {
        return Subscribe(
            std::move(handler),
            GenericEventFilter::All().WithPathPrefix(pathPrefix)
        );
    }

    /// Subscribe to lifecycle events for a specific path
    boost::signals2::connection SubscribeToLifecycle(
        std::function<void(const LifecycleEvent&)> handler,
        const EventPath& pathPrefix = EventPath{}) {

        auto filter = GenericEventFilter::LifecycleOnly();
        if (!pathPrefix.IsEmpty()) {
            filter = filter.WithPathPrefix(pathPrefix);
        }

        return Subscribe(
            [handler = std::move(handler)](const GenericEvent& event) {
                if (const auto* lifecycle = std::get_if<LifecycleEvent>(&event)) {
                    handler(*lifecycle);
                }
            },
            filter
        );
    }

    /// Subscribe to progress events for a specific path
    boost::signals2::connection SubscribeToProgress(
        std::function<void(const ProgressEvent&)> handler,
        const EventPath& pathPrefix = EventPath{}) {

        auto filter = GenericEventFilter::ProgressOnly();
        if (!pathPrefix.IsEmpty()) {
            filter = filter.WithPathPrefix(pathPrefix);
        }

        return Subscribe(
            [handler = std::move(handler)](const GenericEvent& event) {
                if (const auto* progress = std::get_if<ProgressEvent>(&event)) {
                    handler(*progress);
                }
            },
            filter
        );
    }
};

using IGenericEventDispatcherPtr = std::shared_ptr<IGenericEventDispatcher>;

/**
 * GenericEventDispatcher - Concrete implementation
 *
 * Thread-safe via boost::signals2.
 */
class GenericEventDispatcher : public IGenericEventDispatcher {
public:
    GenericEventDispatcher() = default;
    ~GenericEventDispatcher() override = default;

    GenericEventDispatcher(const GenericEventDispatcher&) = delete;
    GenericEventDispatcher& operator=(const GenericEventDispatcher&) = delete;

    void Emit(GenericEvent event) override {
        m_signal(event);
    }

    void Emit(GenericEvent event, const EventPath& path) override {
        SetEventPath(event, path);
        m_signal(event);
    }

    boost::signals2::connection Subscribe(
        GenericEventSlot handler,
        GenericEventFilter filter = GenericEventFilter::All()) override {

        // Wrap handler with filter check
        return m_signal.connect(
            [handler = std::move(handler), filter = std::move(filter)]
            (const GenericEvent& event) {
                if (filter.Accepts(event)) {
                    handler(event);
                }
            }
        );
    }

    /// Get current subscriber count
    [[nodiscard]] size_t SubscriberCount() const {
        return m_signal.num_slots();
    }

private:
    GenericEventSignal m_signal;
};

/**
 * NullGenericEventDispatcher - No-op implementation for when no dispatcher is set
 *
 * Avoids null checks throughout the code.
 */
class NullGenericEventDispatcher : public IGenericEventDispatcher {
public:
    void Emit(GenericEvent) override {}
    void Emit(GenericEvent, const EventPath&) override {}

    boost::signals2::connection Subscribe(
        GenericEventSlot,
        GenericEventFilter) override {
        return {};
    }
};

/**
 * ThreadSafeGenericEventDispatcher - Extra thread safety with mutex
 *
 * Use this if you need stronger thread safety guarantees than boost::signals2 provides,
 * such as when modifying internal state during emission.
 */
class ThreadSafeGenericEventDispatcher : public IGenericEventDispatcher {
public:
    ThreadSafeGenericEventDispatcher() = default;
    ~ThreadSafeGenericEventDispatcher() override = default;

    ThreadSafeGenericEventDispatcher(const ThreadSafeGenericEventDispatcher&) = delete;
    ThreadSafeGenericEventDispatcher& operator=(const ThreadSafeGenericEventDispatcher&) = delete;

    void Emit(GenericEvent event) override {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_signal(event);
    }

    void Emit(GenericEvent event, const EventPath& path) override {
        SetEventPath(event, path);
        std::lock_guard<std::mutex> lock(m_mutex);
        m_signal(event);
    }

    boost::signals2::connection Subscribe(
        GenericEventSlot handler,
        GenericEventFilter filter = GenericEventFilter::All()) override {

        std::lock_guard<std::mutex> lock(m_mutex);
        return m_signal.connect(
            [handler = std::move(handler), filter = std::move(filter)]
            (const GenericEvent& event) {
                if (filter.Accepts(event)) {
                    handler(event);
                }
            }
        );
    }

    [[nodiscard]] size_t SubscriberCount() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_signal.num_slots();
    }

private:
    mutable std::mutex m_mutex;
    GenericEventSignal m_signal;
};

// Factory functions

inline IGenericEventDispatcherPtr MakeGenericEventDispatcher() {
    return std::make_shared<GenericEventDispatcher>();
}

inline IGenericEventDispatcherPtr MakeThreadSafeGenericEventDispatcher() {
    return std::make_shared<ThreadSafeGenericEventDispatcher>();
}

inline IGenericEventDispatcherPtr MakeNullGenericEventDispatcher() {
    return std::make_shared<NullGenericEventDispatcher>();
}

} // namespace data_sdk::events
