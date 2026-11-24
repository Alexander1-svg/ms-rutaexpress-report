# ms-rutaexpress-kafka-analytics

> Repositorio `ms-rutaexpress-report`. La aplicación se llama `kafka-analytics`.

Microservicio de RutaExpress que consume eventos de envíos desde Kafka, valida y deduplica los mensajes, publica eventos analíticos y expone estadísticas mediante una API REST.

## Tecnologías

- Java 21
- Spring Boot 4.1.2
- Spring for Apache Kafka
- Spring Data JPA con Oracle Autonomous Database (H2 en las pruebas)
- Maven Wrapper
- Kafka embebido en modo KRaft para pruebas de integración

## Flujo

```text
rutaexpress.shipments.events.v1
              |
              v
    ShipmentEventConsumer
              |
       validación / DLT
              |
              v
 ShipmentAnalyticsProcessor
       |              |
       v              v
 Oracle         rutaexpress.shipments.analytics.v1
 (ANALYTICS_SHIPMENT_EVENTS)
       |
       v
 GET /api/v1/analytics/*
```

Cada evento procesado se guarda como una fila en la tabla Oracle `ANALYTICS_SHIPMENT_EVENTS`, la misma base de datos de RutaExpress que usa Shipments. Los conteos por estado se calculan con `COUNT(*) ... GROUP BY STATUS`, así que **las métricas no se pierden al reiniciar el servicio** ni dependen de qué réplica responda. La clave primaria es el `eventId` del evento de origen, por lo que la deduplicación también es persistente.

En memoria solo quedan los eventos que se están procesando en ese instante y el contador `duplicateEvents`, que es informativo y vuelve a 0 al reiniciar.

## Oracle

| Pieza | Archivo |
| --- | --- |
| Entidad | `entity/ShipmentAnalyticsRecord.java` → tabla `ANALYTICS_SHIPMENT_EVENTS` |
| Repositorio | `repository/ShipmentAnalyticsRecordRepository.java` (Spring Data JPA) |
| Lógica | `repository/AnalyticsStore.java` |
| Propiedades | `spring.datasource.*` y `spring.jpa.*` en `application.yml` |
| Script opcional | `docs/oracle/analytics_schema.sql` |

La conexión es igual a la de Shipments: `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` y el wallet de Oracle montado en `/opt/oracle/wallet`. Con `JPA_DDL_AUTO=update` (por defecto) Hibernate crea la tabla en el primer arranque; si el DBA la crea con el script, usar `JPA_DDL_AUTO=validate`.

El wallet se extrae (no el ZIP) en `./wallet`, que está en `.gitignore` y nunca entra en la imagen Docker. El contenedor corre como UID 10001, así que en la EC2:

```sh
sudo chown -R 10001:10001 ./wallet
sudo find ./wallet -type d -exec chmod 700 {} \;
sudo find ./wallet -type f -exec chmod 400 {} \;
chmod 600 .env
```

## Contratos

Los contratos JSON `v1` y ejemplos están documentados en [docs/EVENT_CONTRACTS.md](docs/EVENT_CONTRACTS.md).

La key Kafka de los eventos de entrada y salida es `shipmentId`. El productor analítico genera un `eventId` determinista usando el `eventId` de origen.

## Configuración

| Variable | Valor local predeterminado | Descripción |
| --- | --- | --- |
| `SERVER_PORT` | `8091` | Puerto HTTP |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers Kafka |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Protocolo de seguridad |
| `KAFKA_TOPIC_SHIPMENTS` | `rutaexpress.shipments.events.v1` | Topic de entrada |
| `KAFKA_TOPIC_ANALYTICS` | `rutaexpress.shipments.analytics.v1` | Topic analítico |
| `KAFKA_DLT_SUFFIX` | `.DLT` | Sufijo de Dead Letter Topic |
| `KAFKA_CONSUMER_GROUP` | `rutaexpress-kafka-analytics-v1` | Grupo consumidor |
| `KAFKA_LISTENER_CONCURRENCY` | `1` | Consumidores concurrentes |
| `KAFKA_PUBLISH_TIMEOUT` | `10s` | Espera máxima del ACK del productor |
| `KAFKA_RETRY_INTERVAL` | `1s` | Intervalo entre reintentos |
| `KAFKA_RETRY_MAX_ATTEMPTS` | `2` | Reintentos antes de DLT |
| `ANALYTICS_RECENT_EVENTS_LIMIT` | `20` | Eventos recientes conservados |

Para una conexión administrada, las propiedades adicionales de Spring Kafka se pueden proporcionar como variables externas, por ejemplo `SPRING_KAFKA_PROPERTIES_SASL_MECHANISM` y `SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG`. Las credenciales nunca deben guardarse en el repositorio.

Los topics de entrada, salida y DLT los crea `kafka-init-topics` en el compose de `ms-rutaexpress-audit`, con 3 particiones cada uno (la DLT tiene las mismas particiones que su topic de origen). Este microservicio no crea ni administra topics.

## API REST

- `GET /api/v1/analytics/summary`: total procesado, duplicados, conteo por estado y eventos recientes.
- `GET /api/v1/analytics/statuses`: conteo por estado.
- `GET /api/v1/analytics/recent?limit=20`: últimos eventos; `limit` acepta valores entre 1 y 100.
- `GET /actuator/health`: estado de la aplicación.

Ejemplo:

```powershell
Invoke-RestMethod http://localhost:8091/api/v1/analytics/summary
```

## Ejecución con Docker Compose

La arquitectura Kafka (Zookeeper, 3 brokers, tópicos y Kafka-UI) está en el repositorio `ms-rutaexpress-audit`, porque ese microservicio es el administrador de Kafka.

```powershell
# En ms-rutaexpress-audit
docker compose up -d
docker compose -f docker-compose.kafka-ui.yml up -d   # Kafka-UI en http://localhost:8089

# En este repositorio: primero copiar .env.example como .env con los datos de Oracle
# y extraer el wallet en ./wallet
docker compose up -d --build
Invoke-RestMethod http://localhost:8091/api/v1/analytics/summary
```

### Probar el flujo completo desde Kafka-UI

1. Abrir http://localhost:8089, entrar a **Topics → rutaexpress.shipments.events.v1 → Produce Message**.
2. Key: `ENV-100045`. Value: el JSON de `ShipmentEventV1` de [docs/EVENT_CONTRACTS.md](docs/EVENT_CONTRACTS.md).
3. Revisar `GET /api/v1/analytics/summary`: el evento aparece procesado.
4. En `rutaexpress.shipments.analytics.v1` aparece el evento analítico publicado por este servicio.
5. Publicar un value inválido, por ejemplo `{"hola":1}`: no se reintenta, llega a `rutaexpress.shipments.events.v1.DLT` y el log muestra `Enrutando evento a DLT`.

## Ejecución desde IntelliJ IDEA

1. Abrir el `pom.xml` de este repositorio como proyecto Maven.
2. Configurar el Project SDK en Java 21.
3. Esperar a que IntelliJ importe las dependencias Maven.
4. Levantar la infraestructura con `docker compose up -d` en `ms-rutaexpress-audit`.
5. Definir en la configuración de ejecución `KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094`, `DB_URL` (con `TNS_ADMIN` apuntando a la carpeta local del wallet), `DB_USERNAME` y `DB_PASSWORD`.
6. Ejecutar `KafkaAnalyticsApplication`.
7. Consultar la API en `http://localhost:8091/api/v1/analytics/summary`.

## Pruebas

Desde IntelliJ se puede ejecutar el directorio `src/test/java` o cada clase individualmente. Desde PowerShell:

```powershell
.\mvnw.cmd clean verify
```

Las pruebas no necesitan AWS, Oracle ni un Kafka instalado: usan H2 en memoria (modo Oracle) en lugar de Oracle, y la integración inicia temporalmente un broker Kafka KRaft, comprueba el flujo completo y lo detiene al finalizar. `ShipmentAnalyticsRecordRepositoryTest` verifica el mapeo JPA y las consultas reales; `AnalyticsStoreTest` comprueba que las métricas y la deduplicación sobreviven a un reinicio.

## Garantías y límites actuales

- El productor espera confirmación del broker con `acks=all` e idempotencia habilitada.
- Los offsets se confirman por registro después de que el listener termina correctamente.
- Los mensajes inválidos no se reintentan; se enrutan a DLT.
- Los fallos recuperables se reintentan antes de usar la DLT.
- La deduplicación es persistente: la clave primaria de Oracle impide registrar dos veces el mismo `eventId`, también entre réplicas.
- El evento analítico se publica en Kafka antes de guardarse en Oracle. Si Oracle falla, el mensaje se reintenta y, agotados los reintentos, va a la DLT; al reintentar, el evento analítico puede publicarse dos veces con el mismo `eventId` (entrega al menos una vez).
- Si Oracle no está disponible, el servicio no puede registrar métricas: los eventos terminan en la DLT y se pueden reprocesar después.
