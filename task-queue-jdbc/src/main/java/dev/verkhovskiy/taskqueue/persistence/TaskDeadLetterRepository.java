package dev.verkhovskiy.taskqueue.persistence;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.exception.TaskOwnershipLostException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Репозиторий операций над dead-letter задачами. */
@Repository
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification =
        "Spring JdbcTemplate is an injected infrastructure bean owned by the container.")
public class TaskDeadLetterRepository {

  private final NamedParameterJdbcTemplate jdbc;

  public TaskDeadLetterRepository(
      @Qualifier(TaskQueueBeanNames.NAMED_PARAMETER_JDBC_TEMPLATE)
          NamedParameterJdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Копирует задачу в dead-letter и удаляет ее только если она все еще закреплена за ожидаемым
   * воркером.
   *
   * @param taskId идентификатор задачи
   * @param workerId идентификатор воркера-владельца
   * @param reason причина финализации задачи
   * @param errorClass класс последней ошибки
   * @param errorMessage сообщение последней ошибки
   * @throws TaskOwnershipLostException если задача отсутствует или закреплена за другим воркером
   */
  public void deadLetterOwnedBy(
      UUID taskId, String workerId, String reason, String errorClass, String errorMessage) {
    int inserted =
        jdbc.update(
            """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            insert into task_queue_dead_letter(
                task_id,
                task_type,
                payload,
                partition_key,
                partition_num,
                delay_count,
                attempt_count,
                failed_worker_id,
                reason,
                error_class,
                error_message,
                original_created_at,
                dead_lettered_at
            )
            select q.task_id,
                   q.task_type,
                   q.payload,
                   q.partition_key,
                   q.partition_num,
                   q.delay_count,
                   q.delay_count + 1,
                   :workerId,
                   :reason,
                   :errorClass,
                   :errorMessage,
                   q.created_at,
                   runtime_clock.now
              from task_queue q
             cross join runtime_clock
             where q.task_id = :taskId
               and q.worker_id = :workerId
            on conflict (task_id)
            do update
                  set task_type = excluded.task_type,
                      payload = excluded.payload,
                      partition_key = excluded.partition_key,
                      partition_num = excluded.partition_num,
                      delay_count = excluded.delay_count,
                      attempt_count = excluded.attempt_count,
                      failed_worker_id = excluded.failed_worker_id,
                      reason = excluded.reason,
                      error_class = excluded.error_class,
                      error_message = excluded.error_message,
                      original_created_at = excluded.original_created_at,
                      dead_lettered_at = excluded.dead_lettered_at
            """,
            new MapSqlParameterSource()
                .addValue("taskId", taskId)
                .addValue("workerId", workerId)
                .addValue("reason", reason)
                .addValue("errorClass", errorClass)
                .addValue("errorMessage", errorMessage));
    if (inserted == 0) {
      throw new TaskOwnershipLostException(taskId, workerId);
    }
    removeOwnedTask(taskId, workerId);
  }

  /**
   * Возвращает dead-letter задачу в основную очередь.
   *
   * @param taskId идентификатор dead-letter задачи
   * @param availableAt время, когда задача станет доступна для обработки, или {@code null} для
   *     текущего времени PostgreSQL
   * @return {@code true}, если задача перенесена в основную очередь
   */
  public boolean requeueDeadLetter(UUID taskId, Instant availableAt) {
    return requeueDeadLetter(taskId, availableAt, null);
  }

  /**
   * Возвращает dead-letter задачу в основную очередь.
   *
   * @param taskId идентификатор dead-letter задачи
   * @param availableAt абсолютное время доступности, или {@code null}
   * @param availableAfter задержка относительно времени PostgreSQL, или {@code null}
   * @return {@code true}, если задача перенесена в основную очередь
   */
  public boolean requeueDeadLetter(UUID taskId, Instant availableAt, Duration availableAfter) {
    Integer moved =
        jdbc.queryForObject(
            """
            with candidate as (
                select task_id,
                       task_type,
                       payload,
                       partition_key,
                       partition_num,
                       original_created_at
                  from task_queue_dead_letter
                 where task_id = :taskId
            ),
            runtime_clock as (
                select clock_timestamp() as now
            ),
            inserted as (
                insert into task_queue(
                    task_id,
                    task_type,
                    payload,
                    partition_key,
                    partition_num,
                    available_at,
                    delay_count,
                    worker_id,
                    created_at,
                    locked_at,
                    lease_until
                )
                select task_id,
                       task_type,
                       payload,
                       partition_key,
                       partition_num,
                       coalesce(
                           cast(:availableAt as timestamptz),
                           case
                               when :availableAfterSet
                               then runtime_clock.now + (:availableAfterMillis * interval '1 millisecond')
                               else runtime_clock.now
                           end
                       ),
                       0,
                       null,
                       original_created_at,
                       null,
                       null
                  from candidate
                 cross join runtime_clock
                on conflict (task_id) do nothing
                returning task_id
            ),
            deleted as (
                delete from task_queue_dead_letter dl
                 using inserted i
                 where dl.task_id = i.task_id
                returning dl.task_id
            )
            select count(*) from deleted
            """,
            new MapSqlParameterSource()
                .addValue("taskId", taskId)
                .addValue("availableAt", toOffsetDateTime(availableAt))
                .addValue("availableAfterSet", availableAfter != null)
                .addValue(
                    "availableAfterMillis",
                    availableAfter == null ? 0L : Math.max(0L, availableAfter.toMillis())),
            Integer.class);
    return moved != null && moved > 0;
  }

  /**
   * Удаляет старые записи dead-letter.
   *
   * @param cutoff удаляются записи старше этого момента
   * @param limit максимальное количество удаляемых записей
   * @return количество удаленных записей
   */
  public int deleteDeadLettersOlderThan(Instant cutoff, int limit) {
    return jdbc.update(
        """
            with expired as (
                select task_id
                  from task_queue_dead_letter
                 where dead_lettered_at < :cutoff
                 order by dead_lettered_at, task_id
                 limit :limit
            )
            delete from task_queue_dead_letter dl
             using expired e
             where dl.task_id = e.task_id
            """,
        new MapSqlParameterSource()
            .addValue("cutoff", toOffsetDateTime(cutoff))
            .addValue("limit", limit));
  }

  /**
   * Удаляет dead-letter записи старше retention относительно текущего времени PostgreSQL.
   *
   * @param retention сколько хранить dead-letter записи
   * @param limit максимальное количество удаляемых записей
   * @return количество удаленных записей
   */
  public int deleteDeadLettersOlderThan(Duration retention, int limit) {
    return jdbc.update(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            ),
            expired as (
                select task_id
                  from task_queue_dead_letter
                 cross join runtime_clock
                 where dead_lettered_at <
                       runtime_clock.now - (:retentionMillis * interval '1 millisecond')
                 order by dead_lettered_at, task_id
                 limit :limit
            )
            delete from task_queue_dead_letter dl
             using expired e
             where dl.task_id = e.task_id
            """,
        new MapSqlParameterSource()
            .addValue("retentionMillis", Math.max(0L, retention.toMillis()))
            .addValue("limit", limit));
  }

  private void removeOwnedTask(UUID taskId, String workerId) {
    int updated =
        jdbc.update(
            """
            delete from task_queue
             where task_id = :taskId
               and worker_id = :workerId
            """,
            new MapSqlParameterSource().addValue("taskId", taskId).addValue("workerId", workerId));
    if (updated == 0) {
      throw new TaskOwnershipLostException(taskId, workerId);
    }
  }

  private static OffsetDateTime toOffsetDateTime(Instant instant) {
    return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
  }
}
