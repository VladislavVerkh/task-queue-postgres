FROM gradle:9.3.1-jdk21 AS builder
WORKDIR /workspace

COPY --chown=gradle:gradle . .
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle :task-queue-sample-app:bootJar --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=builder /workspace/task-queue-sample-app/build/libs/*.jar /app/app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
