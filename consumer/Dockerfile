FROM gradle:9.7.1-jdk21 AS builder
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties gradle gradle/
COPY common-events common-events
COPY consumer consumer
RUN ./gradlew :consumer:bootJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /workspace/consumer/build/libs/*.jar app.jar
EXPOSE 8081 9091
ENTRYPOINT ["java", "-jar", "app.jar"]
