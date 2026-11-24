package com.ms_rutaexpress.kafka_analytics.repository;

import com.ms_rutaexpress.kafka_analytics.config.KafkaAnalyticsProperties;
import com.ms_rutaexpress.kafka_analytics.dto.AnalyticsSummaryResponse;
import com.ms_rutaexpress.kafka_analytics.dto.RecentShipmentEventResponse;
import com.ms_rutaexpress.kafka_analytics.entity.ShipmentAnalyticsRecord;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentAnalyticsEventV1;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Métricas de envíos persistidas en Oracle (tabla ANALYTICS_SHIPMENT_EVENTS).
 * Sobreviven a reinicios del servicio y se comparten entre réplicas.
 *
 * En memoria solo quedan dos cosas de corta vida: los eventos que se están procesando ahora
 * (para no publicar dos veces el mismo evento en paralelo) y el contador de duplicados
 * descartados desde el último arranque, que es solo informativo.
 */
@Component
public class AnalyticsStore {
    private final ShipmentAnalyticsRecordRepository records;
    private final Set<String> inFlightEventIds = ConcurrentHashMap.newKeySet();
    private final LongAdder duplicateEvents = new LongAdder();
    private final int recentEventsLimit;

    public AnalyticsStore(ShipmentAnalyticsRecordRepository records, KafkaAnalyticsProperties properties) {
        this.records = records;
        this.recentEventsLimit = properties.recentEventsLimit();
    }

    /** false si el evento ya está en Oracle o se está procesando en este momento. */
    public boolean tryStart(String sourceEventId) {
        if (!inFlightEventIds.add(sourceEventId)) {
            duplicateEvents.increment();
            return false;
        }
        boolean alreadyStored;
        try {
            alreadyStored = records.existsById(sourceEventId);
        } catch (RuntimeException exception) {
            inFlightEventIds.remove(sourceEventId);
            throw exception;
        }
        if (alreadyStored) {
            inFlightEventIds.remove(sourceEventId);
            duplicateEvents.increment();
            return false;
        }
        return true;
    }

    /**
     * Registra en Oracle un evento ya publicado en el topic analítico.
     * Si otra instancia lo registró primero, la clave primaria lo rechaza y se cuenta como duplicado.
     * Cualquier otro error de base de datos se propaga para que Kafka reintente el mensaje.
     */
    public void complete(ShipmentAnalyticsEventV1 event) {
        try {
            records.saveAndFlush(ShipmentAnalyticsRecord.from(event));
        } catch (DataIntegrityViolationException alreadyStored) {
            duplicateEvents.increment();
        } finally {
            inFlightEventIds.remove(event.sourceEventId());
        }
    }

    public void abort(String sourceEventId) {
        inFlightEventIds.remove(sourceEventId);
    }

    public AnalyticsSummaryResponse snapshot() {
        return new AnalyticsSummaryResponse(
                records.count(),
                duplicateEvents.sum(),
                byStatus(),
                recent(recentEventsLimit)
        );
    }

    public Map<String, Long> byStatus() {
        Map<String, Long> counts = new TreeMap<>();
        records.countGroupedByStatus().forEach(row -> counts.put(row.getStatus(), row.getTotal()));
        return Collections.unmodifiableMap(counts);
    }

    public List<RecentShipmentEventResponse> recent(int limit) {
        return records.findAllByOrderByProcessedAtDesc(Limit.of(limit)).stream()
                .map(record -> new RecentShipmentEventResponse(
                        record.getSourceEventId(),
                        record.getShipmentId(),
                        record.getClienteId(),
                        record.getTrackingNumber(),
                        record.getStatus(),
                        record.getOccurredAt(),
                        record.getProcessedAt()
                ))
                .toList();
    }
}
