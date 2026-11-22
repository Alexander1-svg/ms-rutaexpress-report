package com.ms_rutaexpress.kafka_analytics.repository;

import com.ms_rutaexpress.kafka_analytics.config.KafkaAnalyticsProperties;
import com.ms_rutaexpress.kafka_analytics.dto.AnalyticsSummaryResponse;
import com.ms_rutaexpress.kafka_analytics.dto.RecentShipmentEventResponse;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentAnalyticsEventV1;
import org.springframework.stereotype.Repository;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

@Repository
public class InMemoryAnalyticsRepository {

    private final Set<String> processedEventIds = ConcurrentHashMap.newKeySet();
    private final Set<String> inFlightEventIds = ConcurrentHashMap.newKeySet();
    private final Map<String, LongAdder> statusCounters = new ConcurrentHashMap<>();
    private final ArrayDeque<RecentShipmentEventResponse> recentEvents = new ArrayDeque<>();
    private final LongAdder duplicateEvents = new LongAdder();
    private final int recentEventsLimit;

    public InMemoryAnalyticsRepository(KafkaAnalyticsProperties properties) {
        this.recentEventsLimit = properties.recentEventsLimit();
    }

    public boolean tryStart(String sourceEventId) {
        if (processedEventIds.contains(sourceEventId) || !inFlightEventIds.add(sourceEventId)) {
            duplicateEvents.increment();
            return false;
        }
        return true;
    }

    public void complete(ShipmentAnalyticsEventV1 event) {
        statusCounters.computeIfAbsent(event.status(), ignored -> new LongAdder()).increment();
        synchronized (recentEvents) {
            recentEvents.addFirst(new RecentShipmentEventResponse(
                    event.sourceEventId(),
                    event.shipmentId(),
                    event.clienteId(),
                    event.trackingNumber(),
                    event.status(),
                    event.occurredAt(),
                    event.processedAt()
            ));
            while (recentEvents.size() > recentEventsLimit) {
                recentEvents.removeLast();
            }
        }
        processedEventIds.add(event.sourceEventId());
        inFlightEventIds.remove(event.sourceEventId());
    }

    public void abort(String sourceEventId) {
        inFlightEventIds.remove(sourceEventId);
    }

    public AnalyticsSummaryResponse snapshot() {
        Map<String, Long> counts = new TreeMap<>();
        statusCounters.forEach((status, counter) -> counts.put(status, counter.sum()));

        List<RecentShipmentEventResponse> recentCopy;
        synchronized (recentEvents) {
            recentCopy = List.copyOf(recentEvents);
        }

        return new AnalyticsSummaryResponse(
                processedEventIds.size(),
                duplicateEvents.sum(),
                Map.copyOf(new LinkedHashMap<>(counts)),
                recentCopy
        );
    }
}