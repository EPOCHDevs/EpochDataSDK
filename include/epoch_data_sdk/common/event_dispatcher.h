#pragma once
#include "event_filter.h"
#include <memory>

namespace data_sdk::events {

class IEventDispatcher {
public:
    virtual ~IEventDispatcher() = default;

    virtual void Emit(OrchestratorEvent event) = 0;

    virtual boost::signals2::connection Subscribe(
        OrchestratorEventSlot handler,
        EventFilter filter = EventFilter::All()) = 0;

    template<typename T>
    boost::signals2::connection SubscribeTo(std::function<void(const T&)> handler) {
        return Subscribe(
            [handler = std::move(handler)](const OrchestratorEvent& event) {
                if (const auto* typed = std::get_if<T>(&event)) {
                    handler(*typed);
                }
            },
            EventFilter::Only({EventTypeFor<T>::value})
        );
    }
};

using IEventDispatcherPtr = std::shared_ptr<IEventDispatcher>;

class EventDispatcher : public IEventDispatcher {
public:
    EventDispatcher() = default;
    ~EventDispatcher() override = default;

    EventDispatcher(const EventDispatcher&) = delete;
    EventDispatcher& operator=(const EventDispatcher&) = delete;

    void Emit(OrchestratorEvent event) override;

    boost::signals2::connection Subscribe(
        OrchestratorEventSlot handler,
        EventFilter filter = EventFilter::All()) override;

    [[nodiscard]] size_t SubscriberCount() const;

private:
    OrchestratorEventSignal m_signal;
};

class NullEventDispatcher : public IEventDispatcher {
public:
    void Emit(OrchestratorEvent) override {}
    boost::signals2::connection Subscribe(
        OrchestratorEventSlot,
        EventFilter) override { return {}; }
};

inline IEventDispatcherPtr MakeEventDispatcher() {
    return std::make_shared<EventDispatcher>();
}

inline IEventDispatcherPtr MakeNullEventDispatcher() {
    return std::make_shared<NullEventDispatcher>();
}

} // namespace data_sdk::events
