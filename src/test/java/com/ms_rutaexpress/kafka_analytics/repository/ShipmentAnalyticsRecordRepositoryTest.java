package com.ms_rutaexpress.kafka_analytics.repository;

import com.ms_rutaexpress.kafka_analytics.entity.ShipmentAnalyticsRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Limit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Mapeo JPA y consultas reales contra H2 en modo Oracle. */
@DataJpaTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:analytics-jpa;MODE=Oracle;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
class ShipmentAnalyticsRecordRepositoryTest {

    @Autowired
    private ShipmentAnalyticsRecordRepository repository;

    @Test
    void storesEventsAndAggregatesThemByStatus() {
        repository.saveAndFlush(record("source-1", "CREADO", "2026-11-15T20:30:01Z"));
        repository.saveAndFlush(record("source-2", "EN_RUTA", "2026-11-15T20:30:02Z"));
        repository.saveAndFlush(record("source-3", "EN_RUTA", "2026-11-15T20:30:03Z"));

        assertThat(repository.count()).isEqualTo(3);
        assertThat(repository.existsById("source-2")).isTrue();
        assertThat(repository.existsById("missing")).isFalse();
        assertThat(repository.countGroupedByStatus())
                .extracting(row -> row.getStatus() + "=" + row.getTotal())
                .containsExactlyInAnyOrder("CREADO=1", "EN_RUTA=2");
        assertThat(repository.findAllByOrderByProcessedAtDesc(Limit.of(2)))
                .extracting(ShipmentAnalyticsRecord::getSourceEventId)
                .containsExactly("source-3", "source-2");
        ShipmentAnalyticsRecord stored = repository.findAllByOrderByProcessedAtDesc(Limit.of(1)).getFirst();
        assertThat(stored.getClienteId()).isEqualTo("cliente-1");
        assertThat(stored.getTrackingNumber()).isEqualTo("RUTA-1");
    }

    @Test
    void sameSourceEventCannotBeStoredTwice() {
        repository.saveAndFlush(record("source-1", "CREADO", "2026-11-15T20:30:01Z"));

        assertThatThrownBy(() -> repository.saveAndFlush(record("source-1", "CREADO", "2026-11-15T20:30:01Z")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private static ShipmentAnalyticsRecord record(String sourceEventId, String status, String processedAt) {
        return ShipmentAnalyticsRecord.from(AnalyticsStoreTest.event(sourceEventId, status, processedAt));
    }
}
