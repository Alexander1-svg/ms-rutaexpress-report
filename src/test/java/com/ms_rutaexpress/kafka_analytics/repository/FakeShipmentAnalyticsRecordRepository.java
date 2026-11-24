package com.ms_rutaexpress.kafka_analytics.repository;

import com.ms_rutaexpress.kafka_analytics.entity.ShipmentAnalyticsRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.domain.Limit;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/** Sustituto en memoria de la tabla Oracle para pruebas unitarias, con su clave primaria. */
public class FakeShipmentAnalyticsRecordRepository implements ShipmentAnalyticsRecordRepository {
    private final Map<String, ShipmentAnalyticsRecord> rows = new ConcurrentHashMap<>();
    private volatile boolean databaseDown;

    public void setDatabaseDown(boolean databaseDown) {
        this.databaseDown = databaseDown;
    }

    @Override
    public boolean existsById(String sourceEventId) {
        failIfDown();
        return rows.containsKey(sourceEventId);
    }

    @Override
    public ShipmentAnalyticsRecord saveAndFlush(ShipmentAnalyticsRecord record) {
        failIfDown();
        if (rows.putIfAbsent(record.getSourceEventId(), record) != null) {
            throw new DataIntegrityViolationException("PK violada: " + record.getSourceEventId());
        }
        return record;
    }

    @Override
    public long count() {
        failIfDown();
        return rows.size();
    }

    @Override
    public List<StatusCount> countGroupedByStatus() {
        failIfDown();
        Map<String, Long> counts = new TreeMap<>();
        rows.values().forEach(row -> counts.merge(row.getStatus(), 1L, Long::sum));
        List<StatusCount> result = new ArrayList<>();
        counts.forEach((status, total) -> result.add(new StatusCount() {
            @Override
            public String getStatus() {
                return status;
            }

            @Override
            public Long getTotal() {
                return total;
            }
        }));
        return result;
    }

    @Override
    public List<ShipmentAnalyticsRecord> findAllByOrderByProcessedAtDesc(Limit limit) {
        failIfDown();
        return rows.values().stream()
                .sorted(Comparator.comparing(ShipmentAnalyticsRecord::getProcessedAt).reversed())
                .limit(limit.max())
                .toList();
    }

    private void failIfDown() {
        if (databaseDown) {
            throw new DataAccessResourceFailureException("Oracle no disponible");
        }
    }
}
