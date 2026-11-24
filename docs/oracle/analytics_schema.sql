-- Tabla de métricas de kafka-analytics en Oracle (RutaExpress).
--
-- Opcional: con JPA_DDL_AUTO=update (valor por defecto) Hibernate crea esta tabla
-- automáticamente la primera vez que arranca el servicio. Usa este script solo si el DBA
-- prefiere crearla a mano; en ese caso arranca el servicio con JPA_DDL_AUTO=validate.
--
-- Ejecutar conectado con el mismo usuario que usa el servicio (DB_USERNAME).

CREATE TABLE ANALYTICS_SHIPMENT_EVENTS (
    SOURCE_EVENT_ID     VARCHAR2(100 CHAR)          NOT NULL,
    ANALYTICS_EVENT_ID  VARCHAR2(200 CHAR)          NOT NULL,
    SHIPMENT_ID         VARCHAR2(100 CHAR)          NOT NULL,
    CLIENTE_ID          VARCHAR2(100 CHAR),
    TRACKING_NUMBER     VARCHAR2(100 CHAR),
    METRIC_TYPE         VARCHAR2(60 CHAR)           NOT NULL,
    STATUS              VARCHAR2(60 CHAR)           NOT NULL,
    OCCURRED_AT         TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    PROCESSED_AT        TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    CORRELATION_ID      VARCHAR2(100 CHAR),
    -- Un evento de origen se registra una sola vez: deduplicación persistente.
    CONSTRAINT PK_ANALYTICS_SHIPMENT_EVENTS PRIMARY KEY (SOURCE_EVENT_ID)
);

CREATE INDEX IX_ANALYTICS_EVT_STATUS ON ANALYTICS_SHIPMENT_EVENTS (STATUS);
CREATE INDEX IX_ANALYTICS_EVT_PROCESSED ON ANALYTICS_SHIPMENT_EVENTS (PROCESSED_AT);

-- Consultas útiles para la demo:
-- SELECT STATUS, COUNT(*) FROM ANALYTICS_SHIPMENT_EVENTS GROUP BY STATUS ORDER BY STATUS;
-- SELECT * FROM ANALYTICS_SHIPMENT_EVENTS ORDER BY PROCESSED_AT DESC FETCH FIRST 20 ROWS ONLY;
