#include <epoch_data_sdk/common/event_dispatcher.h>

namespace data_sdk::events {

void EventDispatcher::Emit(OrchestratorEvent event) {
    m_signal(std::move(event));
}

boost::signals2::connection EventDispatcher::Subscribe(
    OrchestratorEventSlot handler,
    EventFilter filter) {

    auto filteredHandler = [filter = std::move(filter),
                            handler = std::move(handler)]
                           (const OrchestratorEvent& event) {
        if (filter.Accepts(event)) {
            handler(event);
        }
    };

    return m_signal.connect(std::move(filteredHandler));
}

size_t EventDispatcher::SubscriberCount() const {
    return m_signal.num_slots();
}

} // namespace data_sdk::events
