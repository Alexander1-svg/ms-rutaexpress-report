package com.ms_rutaexpress.kafka_analytics.event;

import java.time.Instant;

public record ShipmentAnalyticsEventV1(
        String schemaVersion,
        String eventId,
        String sourceEventId,
        String shipmentId,
        String clienteId,
        String trackingNumber,
        String metricType,
        String status,
        Instant occurredAt,
        Instant processedAt,
        String correlationId
) {
    public static final String SCHEMA_VERSION = "1.0";
    public static final String METRIC_TYPE = "SHIPMENT_STATUS_OBSERVED";

    public ShipmentAnalyticsEventV1(
            String schemaVersion,
            String eventId,
            String sourceEventId,
            String shipmentId,
            String metricType,
            String status,
            Instant occurredAt,
            Instant processedAt,
            String correlationId
    ) {
        this(schemaVersion, eventId, sourceEventId, shipmentId, null, null, metricType, status, occurredAt, processedAt, correlationId);
    }
}