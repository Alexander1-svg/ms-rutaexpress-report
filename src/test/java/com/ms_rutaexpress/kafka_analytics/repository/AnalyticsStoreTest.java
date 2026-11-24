package com.ms_rutaexpress.kafka_analytics.repository;

import com.ms_rutaexpress.kafka_analytics.config.KafkaAnalyticsProperties;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentAnalyticsEventV1;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AnalyticsStoreTest {

    private final FakeShipmentAnalyticsRecordRepository table = new FakeShipmentAnalyticsRecordRepository();

    @Test
    void countsStatusesIgnoresDuplicatesAndLimitsRecentEvents() {
        AnalyticsStore store = store(2);

        assertThat(store.tryStart("source-1")).isTrue();
        store.complete(event("source-1", "CREADO", "2026-11-15T20:30:01Z"));
        assertThat(store.tryStart("source-1")).isFalse();

        assertThat(store.tryStart("source-2")).isTrue();
        store.complete(event("source-2", "EN_RUTA", "2026-11-15T20:30:02Z"));
        assertThat(store.tryStart("source-3")).isTrue();
        store.complete(event("source-3", "ENTREGADO", "2026-11-15T20:30:03Z"));

        var snapshot = store.snapshot();
        assertThat(snapshot.totalProcessed()).isEqualTo(3);
        assertThat(snapshot.duplicateEvents()).isEqualTo(1);
        assertThat(snapshot.byStatus()).containsEntry("CREADO", 1L)
                .containsEntry("EN_RUTA", 1L)
                .containsEntry("ENTREGADO", 1L);
        assertThat(snapshot.recentEvents()).extracting(event -> event.eventId())
                .containsExactly("source-3", "source-2");
        assertThat(store.recent(10)).hasSize(3);
    }

    @Test
    void metricsAndDeduplicationSurviveARestart() {
        AnalyticsStore beforeRestart = store(20);
        beforeRestart.tryStart("source-1");
        beforeRestart.complete(event("source-1", "ACEPTADO", "2026-11-15T20:30:01Z"));

        // Nueva instancia del servicio sobre la misma tabla: lo que antes se perdía al reiniciar.
        AnalyticsStore afterRestart = store(20);

        assertThat(afterRestart.snapshot().totalProcessed()).isEqualTo(1);
        assertThat(afterRestart.byStatus()).containsEntry("ACEPTADO", 1L);
        assertThat(afterRestart.tryStart("source-1")).isFalse();
    }

    @Test
    void eventInProgressIsNotStartedTwice() {
        AnalyticsStore store = store(20);

        assertThat(store.tryStart("source-1")).isTrue();
        assertThat(store.tryStart("source-1")).isFalse();
        store.abort("source-1");
        assertThat(store.tryStart("source-1")).isTrue();
    }

    @Test
    void primaryKeyConflictFromAnotherInstanceCountsAsDuplicate() {
        AnalyticsStore first = store(20);
        AnalyticsStore second = store(20);
        assertThat(first.tryStart("source-1")).isTrue();
        assertThat(second.tryStart("source-1")).isTrue();

        first.complete(event("source-1", "CREADO", "2026-11-15T20:30:01Z"));
        second.complete(event("source-1", "CREADO", "2026-11-15T20:30:01Z"));

        assertThat(second.snapshot().totalProcessed()).isEqualTo(1);
        assertThat(second.snapshot().duplicateEvents()).isEqualTo(1);
    }

    @Test
    void databaseFailurePropagatesForKafkaRetryAndReleasesTheEvent() {
        AnalyticsStore store = store(20);
        assertThat(store.tryStart("source-1")).isTrue();
        table.setDatabaseDown(true);

        assertThatThrownBy(() -> store.complete(event("source-1", "CREADO", "2026-11-15T20:30:01Z")))
                .isInstanceOf(DataAccessResourceFailureException.class);
        assertThatThrownBy(() -> store.tryStart("source-2"))
                .isInstanceOf(DataAccessResourceFailureException.class);

        table.setDatabaseDown(false);
        assertThat(store.tryStart("source-1")).isTrue();
        assertThat(store.tryStart("source-2")).isTrue();
    }

    private AnalyticsStore store(int recentLimit) {
        return new AnalyticsStore(table, new KafkaAnalyticsProperties(
                new KafkaAnalyticsProperties.Topics("shipments", "analytics", ".DLT"),
                "group", Duration.ofSeconds(5), Duration.ofSeconds(1), 2, recentLimit
        ));
    }

    static ShipmentAnalyticsEventV1 event(String sourceEventId, String status, String processedAt) {
        return new ShipmentAnalyticsEventV1(
                "1.0", sourceEventId + ":shipment-status-observed:v1", sourceEventId, "ENV-10",
                "cliente-1", "RUTA-1", "SHIPMENT_STATUS_OBSERVED", status,
                Instant.parse("2026-11-15T20:30:00Z"), Instant.parse(processedAt), null
        );
    }
}
