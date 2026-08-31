FROM gradle:9.7.1-jdk21 AS builder
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties gradle gradle/
COPY common-events common-events
COPY producer producer
RUN ./gradlew :producer:bootJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /workspace/producer/build/libs/*.jar app.jar
EXPOSE 8080 9090
ENTRYPOINT ["java", "-jar", "app.jar"]
