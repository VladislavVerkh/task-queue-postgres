package dev.verkhovskiy.taskqueue.testkit;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PostgreSQLContainer;

/** Утилиты testkit для поднятия PostgreSQL и проброса datasource-свойств в тестовый контекст. */
public final class TaskQueuePostgresContainerSupport {

  private TaskQueuePostgresContainerSupport() {}

  /**
   * Создает контейнер PostgreSQL c преднастроенным образом.
   *
   * @return инстанс контейнера PostgreSQL
   */
  public static PostgreSQLContainer<?> createContainer() {
    return new PostgreSQLContainer<>("postgres:16-alpine");
  }

  /**
   * Регистрирует datasource-свойства контейнера в Spring DynamicPropertyRegistry.
   *
   * @param registry реестр динамических свойств
   * @param postgres запущенный контейнер PostgreSQL
   */
  public static void registerDatasource(
      DynamicPropertyRegistry registry, PostgreSQLContainer<?> postgres) {
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", postgres::getUsername);
    registry.add("spring.datasource.password", postgres::getPassword);
    registry.add("spring.datasource.driver-class-name", postgres::getDriverClassName);
  }
}
