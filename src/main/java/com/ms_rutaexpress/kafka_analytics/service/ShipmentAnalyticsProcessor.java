package com.ms_rutaexpress.kafka_analytics.service;

import com.ms_rutaexpress.kafka_analytics.event.ShipmentAnalyticsEventV1;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentEventV1;
import com.ms_rutaexpress.kafka_analytics.producer.ShipmentAnalyticsProducer;
import com.ms_rutaexpress.kafka_analytics.repository.AnalyticsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;

@Service
public class ShipmentAnalyticsProcessor {

    private static final Logger log = LoggerFactory.getLogger(ShipmentAnalyticsProcessor.class);

    private final AnalyticsStore repository;
    private final ShipmentAnalyticsProducer producer;
    private final Clock clock;

    public ShipmentAnalyticsProcessor(
            AnalyticsStore repository,
            ShipmentAnalyticsProducer producer,
            Clock clock
    ) {
        this.repository = repository;
        this.producer = producer;
        this.clock = clock;
    }

    public ProcessingResult process(ShipmentEventV1 source) {
        if (!repository.tryStart(source.eventId())) {
            log.info("Evento duplicado omitido: eventId={}", source.eventId());
            return ProcessingResult.DUPLICATE;
        }

        ShipmentAnalyticsEventV1 analyticsEvent = new ShipmentAnalyticsEventV1(
                ShipmentAnalyticsEventV1.SCHEMA_VERSION,
                source.eventId() + ":shipment-status-observed:v1",
                source.eventId(),
                source.shipmentId(),
                source.clienteId(),
                source.trackingNumber(),
                ShipmentAnalyticsEventV1.METRIC_TYPE,
                source.status(),
                source.occurredAt(),
                Instant.now(clock),
                source.correlationId()
        );

        try {
            producer.publish(analyticsEvent);
            repository.complete(analyticsEvent);
            return ProcessingResult.PROCESSED;
        } catch (RuntimeException exception) {
            repository.abort(source.eventId());
            throw exception;
        }
    }

    public enum ProcessingResult {
        PROCESSED,
        DUPLICATE
    }
}