package com.ms_rutaexpress.kafka_analytics.consumer;

import com.ms_rutaexpress.kafka_analytics.event.ShipmentEventV1;
import com.ms_rutaexpress.kafka_analytics.exception.InvalidShipmentEventException;
import com.ms_rutaexpress.kafka_analytics.service.ShipmentAnalyticsProcessor;
import com.ms_rutaexpress.kafka_analytics.service.ShipmentEventValidator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
public class ShipmentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ShipmentEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final ShipmentEventValidator validator;
    private final ShipmentAnalyticsProcessor processor;

    public ShipmentEventConsumer(
            ObjectMapper objectMapper,
            ShipmentEventValidator validator,
            ShipmentAnalyticsProcessor processor
    ) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.processor = processor;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.shipments}",
            groupId = "${app.kafka.consumer-group}"
    )
    public void consume(ConsumerRecord<String, String> record) {
        ShipmentEventV1 event = deserialize(record.value());
        validator.validate(event);

        ShipmentAnalyticsProcessor.ProcessingResult result = processor.process(event);
        log.info(
                "Evento de envío procesado: eventId={}, shipmentId={}, clienteId={}, trackingNumber={}, status={}, result={}, partition={}, offset={}",
                event.eventId(),
                event.shipmentId(),
                event.clienteId(),
                event.trackingNumber(),
                event.status(),
                result,
                record.partition(),
                record.offset()
        );
    }

    private ShipmentEventV1 deserialize(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new InvalidShipmentEventException("Payload Kafka vacío");
        }
        try {
            return objectMapper.readValue(payload, ShipmentEventV1.class);
        } catch (JacksonException exception) {
            throw new InvalidShipmentEventException("JSON de evento inválido", exception);
        }
    }
}