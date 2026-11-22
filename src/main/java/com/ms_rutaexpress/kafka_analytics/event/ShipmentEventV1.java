package com.ms_rutaexpress.kafka_analytics.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Instant;

public record ShipmentEventV1(
        @NotBlank String schemaVersion,
        @NotBlank @Size(max = 100) String eventId,
        @NotBlank @Size(max = 100) String eventType,
        @NotNull Instant occurredAt,
        @NotBlank @Size(max = 100) String shipmentId,
        @NotBlank @Size(max = 100) String clienteId,
        @NotBlank @Size(max = 100) String trackingNumber,
        @NotBlank @Size(max = 60) String status,
        @Size(max = 100) String correlationId
) {
    public static final String SCHEMA_VERSION = "1.0";

    public ShipmentEventV1(
            String schemaVersion,
            String eventId,
            String eventType,
            Instant occurredAt,
            String shipmentId,
            String status,
            String correlationId
    ) {
        this(schemaVersion, eventId, eventType, occurredAt, shipmentId, "N/A", "N/A", status, correlationId);
    }
}