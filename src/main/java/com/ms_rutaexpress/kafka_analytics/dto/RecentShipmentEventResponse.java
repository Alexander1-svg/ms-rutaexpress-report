package com.ms_rutaexpress.kafka_analytics.dto;

import java.time.Instant;

public record RecentShipmentEventResponse(
        String eventId,
        String shipmentId,
        String clienteId,
        String trackingNumber,
        String status,
        Instant occurredAt,
        Instant processedAt
) {
    public RecentShipmentEventResponse(
            String eventId,
            String shipmentId,
            String status,
            Instant occurredAt,
            Instant processedAt
    ) {
        this(eventId, shipmentId, null, null, status, occurredAt, processedAt);
    }
}