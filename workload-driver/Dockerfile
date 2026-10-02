# Build context is the repo root (multi-module Maven reactor) -- see docker-compose.yml.
# The dependency layer is primed before sources are copied so an ordinary code edit does not
# re-download the entire dependency tree.

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY common-events/pom.xml common-events/
COPY order-service/pom.xml order-service/
COPY payment-service/pom.xml payment-service/
COPY inventory-service/pom.xml inventory-service/
COPY warehouse-service/pom.xml warehouse-service/
COPY logistics-service/pom.xml logistics-service/
COPY notification-service/pom.xml notification-service/
COPY workload-driver/pom.xml workload-driver/
RUN mvn -B -q dependency:go-offline -pl workload-driver -am || true

COPY common-events common-events
COPY workload-driver workload-driver
RUN mvn -B -q -pl workload-driver -am package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /build/workload-driver/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
