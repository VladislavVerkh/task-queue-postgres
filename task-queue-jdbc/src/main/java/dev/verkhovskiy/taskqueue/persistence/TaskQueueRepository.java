package dev.verkhovskiy.taskqueue.persistence;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.exception.TaskOwnershipLostException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Репозиторий операций над таблицей очереди задач. */
@Repository
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification =
        "Spring JdbcTemplate is an injected infrastructure bean owned by the container.")
public class TaskQueueRepository {

  private static final DataClassRowMapper<QueuedTask> QUEUED_TASK_ROW_MAPPER =
      DataClassRowMapper.newInstance(QueuedTask.class);

  private final NamedParameterJdbcTemplate jdbc;

  public TaskQueueRepository(
      @Qualifier(TaskQueueBeanNames.NAMED_PARAMETER_JDBC_TEMPLATE)
          NamedParameterJdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Добавляет новую задачу в очередь.
   *
   * @param taskId идентификатор задачи
   * @param taskType тип задачи
   * @param payload полезная нагрузка задачи
   * @param partitionKey ключ партиционирования
   * @param partitionNum номер партиции
   * @param availableAt момент, когда задача станет доступна для обработки
   * @param createdAt момент создания записи
   */
  public void enqueue(
      UUID taskId,
      String taskType,
      String payload,
      String partitionKey,
      int partitionNum,
      Instant availableAt,
      Instant createdAt) {
    jdbc.update(
        """
            insert into task_queue(
                task_id,
                task_type,
                payload,
                partition_key,
                partition_num,
                available_at,
                delay_count,
                worker_id,
                created_at
            )
            values(
                :taskId,
                :taskType,
                :payload,
                :partitionKey,
                :partitionNum,
                :availableAt,
                0,
                null,
                :createdAt
            )
            """,
        new MapSqlParameterSource()
            .addValue("taskId", taskId)
            .addValue("taskType", taskType)
            .addValue("payload", payload)
            .addValue("partitionKey", partitionKey)
            .addValue("partitionNum", partitionNum)
            .addValue("availableAt", toOffsetDateTime(availableAt))
            .addValue("createdAt", toOffsetDateTime(createdAt)));
  }

  /**
   * Забирает и блокирует следующую пачку задач для конкретного воркера.
   *
   * <p>Выборка учитывает только партиции, назначенные воркеру, и использует {@code FOR UPDATE SKIP
   * LOCKED}, чтобы исключить конкуренцию между потоками.
   *
   * @param workerId идентификатор воркера
   * @param maxCount максимум задач в ответе
   * @param now текущее время
   * @return список задач, закрепленных за воркером
   */
  public List<QueuedTask> lockNextTasksForWorker(String workerId, int maxCount, Instant now) {
    return jdbc.query(
        """
            with candidate as (
                select q.task_id
                 from task_worker_partition_assignment a
                  join task_queue q on q.partition_num = a.partition_num
                 where a.worker_id = :workerId
                   and a.handoff_state = 'ACTIVE'
                   and q.available_at <= :now
                   and q.worker_id is null
                 order by q.available_at, q.created_at, q.task_id
                 for update of q skip locked
                 limit :maxCount
            )
            update task_queue q
               set worker_id = :workerId
              from candidate c
             where q.task_id = c.task_id
            returning q.task_id, q.task_type, q.payload, q.partition_key, q.partition_num, q.available_at, q.delay_count, q.created_at
            """,
        new MapSqlParameterSource()
            .addValue("workerId", workerId)
            .addValue("maxCount", maxCount)
            .addValue("now", toOffsetDateTime(now)),
        QUEUED_TASK_ROW_MAPPER);
  }

  /**
   * Удаляет задачу только если она все еще закреплена за ожидаемым воркером.
   *
   * @param taskId идентификатор задачи
   * @param workerId идентификатор воркера-владельца
   * @throws TaskOwnershipLostException если задача отсутствует или закреплена за другим воркером
   */
  public void removeOwnedBy(UUID taskId, String workerId) {
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

  /**
   * Переносит задачу на повторную попытку только если она все еще закреплена за ожидаемым воркером.
   *
   * @param taskId идентификатор задачи
   * @param workerId идентификатор воркера-владельца
   * @param availableAt время следующей доступности задачи
   * @throws TaskOwnershipLostException если задача отсутствует или закреплена за другим воркером
   */
  public void delayOwnedBy(UUID taskId, String workerId, Instant availableAt) {
    int updated =
        jdbc.update(
            """
            update task_queue
               set available_at = :availableAt,
                   delay_count = delay_count + 1,
                   worker_id = null
             where task_id = :taskId
               and worker_id = :workerId
            """,
            new MapSqlParameterSource()
                .addValue("taskId", taskId)
                .addValue("workerId", workerId)
                .addValue("availableAt", toOffsetDateTime(availableAt)));
    if (updated == 0) {
      throw new TaskOwnershipLostException(taskId, workerId);
    }
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
   * @param deadLetteredAt время переноса в dead-letter
   * @throws TaskOwnershipLostException если задача отсутствует или закреплена за другим воркером
   */
  public void deadLetterOwnedBy(
      UUID taskId,
      String workerId,
      String reason,
      String errorClass,
      String errorMessage,
      Instant deadLetteredAt) {
    int inserted =
        jdbc.update(
            """
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
                   :deadLetteredAt
              from task_queue q
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
                .addValue("errorMessage", errorMessage)
                .addValue("deadLetteredAt", toOffsetDateTime(deadLetteredAt)));
    if (inserted == 0) {
      throw new TaskOwnershipLostException(taskId, workerId);
    }
    removeOwnedBy(taskId, workerId);
  }

  /**
   * Снимает закрепление всех задач, ранее взятых конкретным воркером.
   *
   * @param workerId идентификатор воркера
   */
  public void releaseLockedByWorker(String workerId) {
    jdbc.update(
        """
                update task_queue
                   set worker_id = null
                 where worker_id = :workerId
                """,
        new MapSqlParameterSource("workerId", workerId));
  }

  /**
   * Проверяет наличие in-flight задач конкретной партиции у текущего владельца.
   *
   * @param partitionNum номер партиции
   * @param workerId идентификатор текущего владельца
   * @return {@code true}, если есть хотя бы одна захваченная задача
   */
  public boolean hasInFlightTasks(int partitionNum, String workerId) {
    Boolean exists =
        jdbc.queryForObject(
            """
            select exists(
                select 1
                  from task_queue
                 where partition_num = :partitionNum
                   and worker_id = :workerId
            )
            """,
            new MapSqlParameterSource()
                .addValue("partitionNum", partitionNum)
                .addValue("workerId", workerId),
            Boolean.class);
    return Boolean.TRUE.equals(exists);
  }

  /**
   * Преобразует {@link Instant} в {@link OffsetDateTime} UTC для корректного биндинга в PostgreSQL
   * timestamptz через JDBC-драйвер.
   *
   * @param instant момент времени
   * @return момент времени в представлении offset date-time (UTC)
   */
  private static OffsetDateTime toOffsetDateTime(Instant instant) {
    return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
  }
}
