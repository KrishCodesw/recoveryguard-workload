# Build context is the repo root (multi-module Maven reactor) -- see docker-compose.yml, which points
# here with "context: ." and this Dockerfile path. Do not build this Dockerfile standalone from
# inside inventory-service/; it will not find the sibling common-events module.
#
# Dependencies are resolved in their own layer, primed from the POMs alone, so an ordinary source
# edit no longer re-downloads the entire dependency tree on every build.

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
RUN mvn -B -q dependency:go-offline -pl inventory-service -am -DskipTests || true

COPY common-events common-events
COPY inventory-service inventory-service
RUN mvn -B -q -pl inventory-service -am package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
# The acknowledgement ledger is written here and bind-mounted by docker-compose so it survives the
# container -- ground truth that dies with the container is not ground truth.
RUN mkdir -p /app/runs
COPY --from=build /build/inventory-service/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
