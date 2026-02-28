package dev.verkhovskiy.taskqueue.config;

/**
 * Именованные бины инфраструктуры task-queue.
 *
 * <p>Используются для явной привязки библиотечных компонентов к единому {@code DataSource}/{@code
 * TransactionManager}.
 */
public final class TaskQueueBeanNames {

  /** Имя {@link javax.sql.DataSource} для task-queue. */
  public static final String DATA_SOURCE = "taskQueueDataSource";

  /** Имя PlatformTransactionManager для task-queue. */
  public static final String TRANSACTION_MANAGER = "taskQueueTransactionManager";

  /** Имя JdbcTemplate для task-queue. */
  public static final String JDBC_TEMPLATE = "taskQueueJdbcTemplate";

  /** Имя NamedParameterJdbcTemplate для task-queue. */
  public static final String NAMED_PARAMETER_JDBC_TEMPLATE = "taskQueueNamedParameterJdbcTemplate";

  private TaskQueueBeanNames() {}
}
