package dev.verkhovskiy.taskqueue.persistence;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Репозиторий системных metadata-настроек очереди. */
@Repository
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification =
        "Spring JdbcTemplate is an injected infrastructure bean owned by the container.")
public class TaskQueueMetadataRepository {

  private final NamedParameterJdbcTemplate jdbc;

  public TaskQueueMetadataRepository(
      @Qualifier(TaskQueueBeanNames.NAMED_PARAMETER_JDBC_TEMPLATE)
          NamedParameterJdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Создает metadata-запись, если она еще не существует.
   *
   * @param key ключ metadata
   * @param value значение metadata
   * @return {@code true}, если запись создана текущим вызовом
   */
  public boolean insertIfAbsent(String key, String value) {
    int inserted =
        jdbc.update(
            """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            insert into task_queue_metadata(metadata_key, metadata_value, updated_at)
            select :key, :value, runtime_clock.now
              from runtime_clock
            on conflict (metadata_key) do nothing
            """,
            new MapSqlParameterSource().addValue("key", key).addValue("value", value));
    return inserted > 0;
  }

  /**
   * Загружает metadata-значение по ключу.
   *
   * @param key ключ metadata
   * @return значение, если запись существует
   */
  public Optional<String> findValue(String key) {
    return jdbc
        .query(
            """
            select metadata_value
              from task_queue_metadata
             where metadata_key = :key
            """,
            new MapSqlParameterSource("key", key),
            new SingleColumnRowMapper<>(String.class))
        .stream()
        .findFirst();
  }
}
