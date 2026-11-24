package com.ms_rutaexpress.kafka_analytics.repository;

import com.ms_rutaexpress.kafka_analytics.entity.ShipmentAnalyticsRecord;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

import java.util.List;

/**
 * Acceso JPA a ANALYTICS_SHIPMENT_EVENTS en Oracle.
 * Solo expone las operaciones que usa el servicio (no se borran ni modifican eventos).
 */
public interface ShipmentAnalyticsRecordRepository extends Repository<ShipmentAnalyticsRecord, String> {

    boolean existsById(String sourceEventId);

    ShipmentAnalyticsRecord saveAndFlush(ShipmentAnalyticsRecord record);

    long count();

    @Query("select r.status as status, count(r) as total from ShipmentAnalyticsRecord r group by r.status")
    List<StatusCount> countGroupedByStatus();

    List<ShipmentAnalyticsRecord> findAllByOrderByProcessedAtDesc(Limit limit);

    interface StatusCount {
        String getStatus();

        Long getTotal();
    }
}
