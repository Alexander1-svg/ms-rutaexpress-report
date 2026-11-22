FROM maven:3.9.16-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY src src
# Ejecuta las pruebas: si fallan, la imagen no se construye.
RUN mvn -B -ntp clean package

FROM eclipse-temurin:21-jre-jammy
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home app
WORKDIR /app
COPY --from=build /workspace/target/kafka-analytics-0.0.1-SNAPSHOT.jar app.jar
USER 10001:10001
EXPOSE 8091
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
