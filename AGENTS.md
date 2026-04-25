# Repository Guidelines

## Project Overview

This repository contains a Java 21 Gradle multi-module library for distributed task processing backed by PostgreSQL.

Modules:

- `task-queue-core`: core APIs and configuration types.
- `task-queue-jdbc`: JDBC/PostgreSQL implementation.
- `task-queue-spring-boot-starter`: Spring Boot autoconfiguration starter.
- `task-queue-testkit`: shared testing utilities.
- `task-queue-sample-app`: sample Spring Boot application.

Documentation lives in `docs/`. Local Kubernetes/kind manifests live in `k8s/kind/`, with helper scripts in `scripts/kind/`.

## Build and Test

- Use the Gradle wrapper (`./gradlew`) for all Gradle tasks.
- Run targeted tests for changed modules first, for example `./gradlew :task-queue-jdbc:test`.
- Run `./gradlew test` for the full test suite.
- Run `./gradlew check` before finalizing broad changes; it includes Spotless, tests, JaCoCo reports, and configured checks.
- Run `./gradlew spotlessApply` to format Java code when needed.
- Run `./gradlew securityCheck` only when vulnerability/SBOM validation is requested, because it enables additional security plugins.

## Coding Style

- Java code targets Java 21.
- Keep changes minimal and focused on the requested task.
- Follow existing package structure under `dev.verkhovskiy.taskqueue`.
- Use existing Lombok, Spring Boot, JDBC, Testcontainers, and Micrometer patterns rather than introducing new dependencies.
- Java formatting is managed by Spotless with Google Java Format.
- Avoid unrelated refactors, renames, or behavior changes.

## Testing Guidance

- Add or update tests near the changed code when behavior changes.
- Prefer module-specific test runs while iterating.
- Testcontainers/PostgreSQL-backed tests may require Docker to be available.
- Do not remove or weaken concurrency, transaction, or PostgreSQL-specific coverage without explicit approval.

## Database and Migrations

- PostgreSQL queue schema/resources are in `task-queue-jdbc/src/main/resources`.
- Treat migration/schema changes as compatibility-sensitive.
- Keep task leasing, locking, partition-key ordering, retry, and transaction-boundary semantics aligned with the docs in `docs/`.

## Documentation

- Update `README.md` or files under `docs/` when public behavior, configuration properties, transaction semantics, or local run workflows change.
- Keep documentation concise and consistent with the existing Russian-language style unless asked otherwise.

## Local Runtime

- `docker-compose.yml` supports local infrastructure.
- kind-based local testing is documented in `docs/kind-local-testing.md`.
- Prefer existing scripts in `scripts/kind/` for kind lifecycle tasks.

## Safety

- Do not commit changes unless explicitly asked.
- Do not delete generated/build artifacts unless they are part of the requested cleanup.
- Do not change deployment manifests, monitoring config, or Docker/kind scripts unless the task requires it.
