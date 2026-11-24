package com.ms_rutaexpress.kafka_analytics;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
		"spring.kafka.listener.auto-startup=false",
		// Oracle se reemplaza por H2 en memoria durante las pruebas.
		"spring.datasource.url=jdbc:h2:mem:analytics-context;MODE=Oracle;DB_CLOSE_DELAY=-1",
		"spring.datasource.driver-class-name=org.h2.Driver",
		"spring.datasource.username=sa",
		"spring.datasource.password=",
		"spring.jpa.hibernate.ddl-auto=create-drop"
})
class KafkaAnalyticsApplicationTests {

	@Test
	void contextLoads() {
	}

}
