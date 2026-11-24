package com.ms_rutaexpress.kafka_analytics.service;

import com.ms_rutaexpress.kafka_analytics.config.KafkaAnalyticsProperties;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentAnalyticsEventV1;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentEventV1;
import com.ms_rutaexpress.kafka_analytics.producer.ShipmentAnalyticsProducer;
import com.ms_rutaexpress.kafka_analytics.repository.AnalyticsStore;
import com.ms_rutaexpress.kafka_analytics.repository.FakeShipmentAnalyticsRecordRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ShipmentAnalyticsProcessorTest {

    private final ShipmentAnalyticsProducer producer = mock(ShipmentAnalyticsProducer.class);
    private final AnalyticsStore repository = repository();
    private final Clock clock = Clock.fixed(Instant.parse("2026-11-15T20:31:00Z"), ZoneOffset.UTC);
    private final ShipmentAnalyticsProcessor processor = new ShipmentAnalyticsProcessor(repository, producer, clock);

    @Test
    void publishesAndRegistersAnalyticsEvent() {
        assertThat(processor.process(sourceEvent())).isEqualTo(ShipmentAnalyticsProcessor.ProcessingResult.PROCESSED);

        ArgumentCaptor<ShipmentAnalyticsEventV1> captor = ArgumentCaptor.forClass(ShipmentAnalyticsEventV1.class);
        verify(producer).publish(captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo("evt-1:shipment-status-observed:v1");
        assertThat(captor.getValue().processedAt()).isEqualTo(Instant.parse("2026-11-15T20:31:00Z"));
        assertThat(repository.snapshot().totalProcessed()).isEqualTo(1);
    }

    @Test
    void doesNotPublishDuplicateSourceEvent() {
        processor.process(sourceEvent());

        assertThat(processor.process(sourceEvent())).isEqualTo(ShipmentAnalyticsProcessor.ProcessingResult.DUPLICATE);
        verify(producer, times(1)).publish(org.mockito.ArgumentMatchers.any());
        assertThat(repository.snapshot().duplicateEvents()).isEqualTo(1);
    }

    @Test
    void releasesEventForRetryWhenPublishingFails() {
        doThrow(new IllegalStateException("broker unavailable"))
                .doNothing()
                .when(producer).publish(org.mockito.ArgumentMatchers.any());

        assertThatThrownBy(() -> processor.process(sourceEvent())).isInstanceOf(IllegalStateException.class);
        assertThat(processor.process(sourceEvent())).isEqualTo(ShipmentAnalyticsProcessor.ProcessingResult.PROCESSED);
        assertThat(repository.snapshot().totalProcessed()).isEqualTo(1);
    }

    private ShipmentEventV1 sourceEvent() {
        return new ShipmentEventV1(
                "1.0", "evt-1", "SHIPMENT_STATUS_CHANGED", Instant.parse("2026-11-15T20:30:00Z"),
                "ENV-10", "IN_TRANSIT", "corr-1"
        );
    }

    private AnalyticsStore repository() {
        return new AnalyticsStore(new FakeShipmentAnalyticsRecordRepository(), new KafkaAnalyticsProperties(
                new KafkaAnalyticsProperties.Topics("shipments", "analytics", ".DLT"),
                "group", Duration.ofSeconds(5), Duration.ofSeconds(1), 2, 20
        ));
    }
}

