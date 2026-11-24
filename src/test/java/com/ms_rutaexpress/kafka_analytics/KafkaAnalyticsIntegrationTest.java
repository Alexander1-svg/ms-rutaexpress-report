package com.ms_rutaexpress.kafka_analytics;

import com.ms_rutaexpress.kafka_analytics.event.ShipmentAnalyticsEventV1;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentEventV1;
import com.ms_rutaexpress.kafka_analytics.repository.AnalyticsStore;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "app.kafka.topics.shipments=test.rutaexpress.shipments.v1",
        "app.kafka.topics.analytics=test.rutaexpress.analytics.v1",
        "app.kafka.consumer-group=test-rutaexpress-analytics-group",
        "app.kafka.retry-interval=10ms",
        "app.kafka.retry-max-attempts=1",
        // Oracle se reemplaza por H2 en memoria durante las pruebas.
        "spring.datasource.url=jdbc:h2:mem:analytics-integration;MODE=Oracle;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "test.rutaexpress.shipments.v1",
                "test.rutaexpress.analytics.v1",
                "test.rutaexpress.shipments.v1.DLT"
        },
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DirtiesContext
class KafkaAnalyticsIntegrationTest {

    private static final String SHIPMENTS_TOPIC = "test.rutaexpress.shipments.v1";
    private static final String ANALYTICS_TOPIC = "test.rutaexpress.analytics.v1";
    private static final String DLT_TOPIC = "test.rutaexpress.shipments.v1.DLT";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private AnalyticsStore repository;

    @Test
    void processesValidEventDeduplicatesItAndRoutesInvalidJsonToDlt() throws Exception {
        try (
                Consumer<String, String> analyticsConsumer = consumer("analytics-verifier", ANALYTICS_TOPIC);
                Consumer<String, String> dltConsumer = consumer("dlt-verifier", DLT_TOPIC)
        ) {
            ShipmentEventV1 source = new ShipmentEventV1(
                    "1.0",
                    "integration-event-1",
                    "SHIPMENT_STATUS_CHANGED",
                    Instant.parse("2026-11-15T20:30:00Z"),
                    "ENV-INTEGRATION-1",
                    "IN_TRANSIT",
                    "integration-correlation-1"
            );
            String payload = objectMapper.writeValueAsString(source);

            kafkaTemplate.send(SHIPMENTS_TOPIC, source.shipmentId(), payload).get();
            kafkaTemplate.send(SHIPMENTS_TOPIC, source.shipmentId(), payload).get();
            kafkaTemplate.send(SHIPMENTS_TOPIC, "ENV-INVALID", "{invalid-json").get();

            var analyticsRecord = KafkaTestUtils.getSingleRecord(
                    analyticsConsumer,
                    ANALYTICS_TOPIC,
                    Duration.ofSeconds(15)
            );
            ShipmentAnalyticsEventV1 analytics = objectMapper.readValue(
                    analyticsRecord.value(),
                    ShipmentAnalyticsEventV1.class
            );
            assertThat(analytics.sourceEventId()).isEqualTo(source.eventId());
            assertThat(analytics.status()).isEqualTo("IN_TRANSIT");
            assertThat(analyticsRecord.key()).isEqualTo(source.shipmentId());

            var dltRecord = KafkaTestUtils.getSingleRecord(dltConsumer, DLT_TOPIC, Duration.ofSeconds(15));
            assertThat(dltRecord.value()).isEqualTo("{invalid-json");

            awaitAnalytics(1, 1, Duration.ofSeconds(10));
            assertThat(repository.snapshot().byStatus()).containsEntry("IN_TRANSIT", 1L);
        }
    }

    private Consumer<String, String> consumer(String group, String topic) {
        Map<String, Object> properties = KafkaTestUtils.consumerProps(embeddedKafka, group, true);
        Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                properties,
                new StringDeserializer(),
                new StringDeserializer()
        ).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic);
        return consumer;
    }

    private void awaitAnalytics(long total, long duplicates, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            var snapshot = repository.snapshot();
            if (snapshot.totalProcessed() == total && snapshot.duplicateEvents() == duplicates) {
                return;
            }
            Thread.sleep(50);
        }
        assertThat(repository.snapshot().totalProcessed()).isEqualTo(total);
        assertThat(repository.snapshot().duplicateEvents()).isEqualTo(duplicates);
    }
}
