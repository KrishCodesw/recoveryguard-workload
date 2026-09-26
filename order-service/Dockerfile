# Build context is the repo root (multi-module Maven reactor) -- see
# docker-compose.yml, which points here with "context: ." and this
# Dockerfile path. Do not build this Dockerfile standalone from inside
# order-service/; it won't find the sibling common-events module.

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY common-events common-events
COPY order-service order-service
COPY payment-service payment-service
COPY inventory-service inventory-service
COPY warehouse-service warehouse-service
COPY logistics-service logistics-service
COPY notification-service notification-service

RUN mvn -q -pl order-service -am package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /build/order-service/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
