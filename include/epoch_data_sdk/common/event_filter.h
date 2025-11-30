#pragma once
#include "event_types.h"
#include <set>

namespace data_sdk::events {

class EventFilter {
public:
    static EventFilter All();
    static EventFilter None();
    static EventFilter Only(std::initializer_list<EventType> types);
    static EventFilter Except(std::initializer_list<EventType> types);

    static EventFilter PipelineOnly();
    static EventFilter NodesOnly();
    static EventFilter ProgressOnly();
    static EventFilter TransformProgressOnly();

    [[nodiscard]] bool Accepts(EventType type) const;
    [[nodiscard]] bool Accepts(const OrchestratorEvent& event) const;

    EventFilter operator|(const EventFilter& other) const;
    EventFilter operator&(const EventFilter& other) const;

private:
    std::set<EventType> m_types;
    bool m_isWhitelist{true};

    EventFilter(std::set<EventType> types, bool whitelist);
};

} // namespace data_sdk::events
