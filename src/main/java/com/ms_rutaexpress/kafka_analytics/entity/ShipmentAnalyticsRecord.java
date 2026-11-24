package com.ms_rutaexpress.kafka_analytics.entity;

import com.ms_rutaexpress.kafka_analytics.event.ShipmentAnalyticsEventV1;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;

/**
 * Un evento de envío ya procesado por kafka-analytics, guardado en Oracle.
 * La clave es el eventId del evento de origen: Oracle impide registrarlo dos veces,
 * lo que hace la deduplicación persistente entre reinicios.
 * Las métricas por estado se calculan con COUNT(*) GROUP BY status sobre esta tabla.
 */
@Entity
@Table(name = "ANALYTICS_SHIPMENT_EVENTS", indexes = {
        @Index(name = "IX_ANALYTICS_EVT_STATUS", columnList = "STATUS"),
        @Index(name = "IX_ANALYTICS_EVT_PROCESSED", columnList = "PROCESSED_AT")
})
public class ShipmentAnalyticsRecord implements Persistable<String> {

    @Id
    @Column(name = "SOURCE_EVENT_ID", length = 100, nullable = false, updatable = false)
    private String sourceEventId;

    @Column(name = "ANALYTICS_EVENT_ID", length = 200, nullable = false)
    private String analyticsEventId;

    @Column(name = "SHIPMENT_ID", length = 100, nullable = false)
    private String shipmentId;

    @Column(name = "CLIENTE_ID", length = 100)
    private String clienteId;

    @Column(name = "TRACKING_NUMBER", length = 100)
    private String trackingNumber;

    @Column(name = "METRIC_TYPE", length = 60, nullable = false)
    private String metricType;

    @Column(name = "STATUS", length = 60, nullable = false)
    private String status;

    @Column(name = "OCCURRED_AT", nullable = false)
    private Instant occurredAt;

    @Column(name = "PROCESSED_AT", nullable = false)
    private Instant processedAt;

    @Column(name = "CORRELATION_ID", length = 100)
    private String correlationId;

    // Con id asignado, Spring Data haría merge (UPDATE). Así fuerza INSERT y un duplicado falla.
    @Transient
    private boolean newRecord = true;

    protected ShipmentAnalyticsRecord() {
    }

    public static ShipmentAnalyticsRecord from(ShipmentAnalyticsEventV1 event) {
        ShipmentAnalyticsRecord record = new ShipmentAnalyticsRecord();
        record.sourceEventId = event.sourceEventId();
        record.analyticsEventId = event.eventId();
        record.shipmentId = event.shipmentId();
        record.clienteId = event.clienteId();
        record.trackingNumber = event.trackingNumber();
        record.metricType = event.metricType();
        record.status = event.status();
        record.occurredAt = event.occurredAt();
        record.processedAt = event.processedAt();
        record.correlationId = event.correlationId();
        return record;
    }

    @Override
    public String getId() {
        return sourceEventId;
    }

    @Override
    public boolean isNew() {
        return newRecord;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        this.newRecord = false;
    }

    public String getSourceEventId() {
        return sourceEventId;
    }

    public String getAnalyticsEventId() {
        return analyticsEventId;
    }

    public String getShipmentId() {
        return shipmentId;
    }

    public String getClienteId() {
        return clienteId;
    }

    public String getTrackingNumber() {
        return trackingNumber;
    }

    public String getMetricType() {
        return metricType;
    }

    public String getStatus() {
        return status;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public String getCorrelationId() {
        return correlationId;
    }
}
