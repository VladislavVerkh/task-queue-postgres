package dev.verkhovskiy.taskqueue.persistence;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.exception.TaskOwnershipLostException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
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
   * @param availableAt момент, когда задача станет доступна для обработки, или {@code null} для
   *     текущего времени PostgreSQL
   */
  public void enqueue(
      UUID taskId,
      String taskType,
      String payload,
      String partitionKey,
      int partitionNum,
      Instant availableAt) {
    enqueue(taskId, taskType, payload, partitionKey, partitionNum, availableAt, null);
  }

  /**
   * Добавляет новую задачу в очередь.
   *
   * @param taskId идентификатор задачи
   * @param taskType тип задачи
   * @param payload полезная нагрузка задачи
   * @param partitionKey ключ партиционирования
   * @param partitionNum номер партиции
   * @param availableAt абсолютный момент доступности, или {@code null}
   * @param availableAfter задержка относительно времени PostgreSQL, или {@code null}
   */
  public void enqueue(
      UUID taskId,
      String taskType,
      String payload,
      String partitionKey,
      int partitionNum,
      Instant availableAt,
      Duration availableAfter) {
    jdbc.update(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            )
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
            select
                :taskId,
                :taskType,
                :payload,
                :partitionKey,
                :partitionNum,
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
                runtime_clock.now
              from runtime_clock
            """,
        new MapSqlParameterSource()
            .addValue("taskId", taskId)
            .addValue("taskType", taskType)
            .addValue("payload", payload)
            .addValue("partitionKey", partitionKey)
            .addValue("partitionNum", partitionNum)
            .addValue("availableAt", toOffsetDateTime(availableAt))
            .addValue("availableAfterSet", availableAfter != null)
            .addValue(
                "availableAfterMillis",
                availableAfter == null ? 0L : Math.max(0L, availableAfter.toMillis())));
  }

  /**
   * Забирает и блокирует следующую пачку задач для конкретного воркера.
   *
   * <p>Выборка учитывает только партиции, назначенные воркеру, и использует {@code FOR UPDATE SKIP
   * LOCKED}, чтобы исключить конкуренцию между потоками.
   *
   * @param workerId идентификатор воркера
   * @param maxCount максимум задач в ответе
   * @param maxTasksPerPartition максимум задач из одной партиции за один poll
   * @param leaseTimeout длительность lease для захваченных задач
   * @return список задач, закрепленных за воркером
   */
  public List<QueuedTask> lockNextTasksForWorker(
      String workerId, int maxCount, int maxTasksPerPartition, Duration leaseTimeout) {
    return jdbc.query(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            ),
            candidate as (
                select q.task_id
                 from runtime_clock
                  join task_worker_partition_assignment a on true
                  cross join lateral (
                      select q.task_id,
                             q.available_at,
                             q.created_at
                        from task_queue q
                       where q.partition_num = a.partition_num
                         and q.available_at <= runtime_clock.now
                         and q.worker_id is null
                       order by q.available_at, q.created_at, q.task_id
                       for update of q skip locked
                       limit :maxTasksPerPartition
                  ) q
                 where a.worker_id = :workerId
                   and a.handoff_state = 'ACTIVE'
                 order by q.available_at, q.created_at, q.task_id
                 limit :maxCount
            )
            update task_queue q
               set worker_id = :workerId,
                   locked_at = runtime_clock.now,
                   lease_until =
                       runtime_clock.now + (:leaseTimeoutMillis * interval '1 millisecond')
              from candidate c, runtime_clock
             where q.task_id = c.task_id
            returning q.task_id, q.task_type, q.payload, q.partition_key, q.partition_num, q.available_at, q.delay_count, q.created_at
            """,
        new MapSqlParameterSource()
            .addValue("workerId", workerId)
            .addValue("maxCount", maxCount)
            .addValue("maxTasksPerPartition", maxTasksPerPartition)
            .addValue("leaseTimeoutMillis", Math.max(1L, leaseTimeout.toMillis())),
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
   * @param delayMillis задержка до следующей доступности задачи
   * @throws TaskOwnershipLostException если задача отсутствует или закреплена за другим воркером
   */
  public void delayOwnedBy(UUID taskId, String workerId, long delayMillis) {
    int updated =
        jdbc.update(
            """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            update task_queue
               set available_at =
                       runtime_clock.now + (:delayMillis * interval '1 millisecond'),
                   delay_count = delay_count + 1,
                   worker_id = null,
                   locked_at = null,
                   lease_until = null
              from runtime_clock
             where task_id = :taskId
               and worker_id = :workerId
            """,
            new MapSqlParameterSource()
                .addValue("taskId", taskId)
                .addValue("workerId", workerId)
                .addValue("delayMillis", Math.max(0L, delayMillis)));
    if (updated == 0) {
      throw new TaskOwnershipLostException(taskId, workerId);
    }
  }

  /**
   * Продлевает lease задачи, если она все еще закреплена за ожидаемым воркером.
   *
   * @param taskId идентификатор задачи
   * @param workerId идентификатор воркера-владельца
   * @param leaseTimeout длительность нового lease
   * @throws TaskOwnershipLostException если задача отсутствует или закреплена за другим воркером
   */
  public void renewLeaseOwnedBy(UUID taskId, String workerId, Duration leaseTimeout) {
    int updated =
        jdbc.update(
            """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            update task_queue
               set lease_until =
                       runtime_clock.now + (:leaseTimeoutMillis * interval '1 millisecond')
              from runtime_clock
             where task_id = :taskId
               and worker_id = :workerId
            """,
            new MapSqlParameterSource()
                .addValue("taskId", taskId)
                .addValue("workerId", workerId)
                .addValue("leaseTimeoutMillis", Math.max(1L, leaseTimeout.toMillis())));
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
    removeOwnedBy(taskId, workerId);
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
                       coalesce(cast(:availableAt as timestamptz), runtime_clock.now),
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
                .addValue("availableAt", toOffsetDateTime(availableAt)),
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

  /**
   * Снимает закрепление всех задач, ранее взятых конкретным воркером.
   *
   * @param workerId идентификатор воркера
   */
  public void releaseLockedByWorker(String workerId) {
    jdbc.update(
        """
                update task_queue
                   set worker_id = null,
                       locked_at = null,
                       lease_until = null
                 where worker_id = :workerId
                """,
        new MapSqlParameterSource("workerId", workerId));
  }

  /**
   * Освобождает задачи, у которых истек lease обработки.
   *
   * @param limit максимальное количество задач за один cleanup
   * @return количество освобожденных задач
   */
  public int releaseExpiredTaskLeases(int limit) {
    return jdbc.update(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            ),
            expired as (
                select q.task_id
                  from task_queue q
                 cross join runtime_clock
                 where q.worker_id is not null
                   and q.lease_until is not null
                   and q.lease_until < runtime_clock.now
                 order by q.lease_until, q.task_id
                 for update of q skip locked
                 limit :limit
            )
            update task_queue q
               set worker_id = null,
                   locked_at = null,
                   lease_until = null
              from expired e
             where q.task_id = e.task_id
            """,
        new MapSqlParameterSource("limit", limit));
  }

  /** Загружает текущие aggregate-метрики состояния очереди. */
  public QueueStateMetrics loadQueueStateMetrics() {
    return jdbc.queryForObject(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            ),
            aggregates as (
                select runtime_clock.now,
                       count(q.task_id)
                           filter (
                               where q.worker_id is null
                                 and q.available_at <= runtime_clock.now
                           ) as ready_tasks,
                       count(q.task_id)
                           filter (where q.worker_id is not null) as in_flight_tasks,
                       min(q.created_at)
                           filter (
                               where q.worker_id is null
                                 and q.available_at <= runtime_clock.now
                           ) as oldest_ready_created_at,
                       min(q.locked_at)
                           filter (
                               where q.worker_id is not null
                                 and q.locked_at is not null
                           ) as oldest_in_flight_locked_at
                  from runtime_clock
                  left join task_queue q on true
                 group by runtime_clock.now
            )
            select ready_tasks,
                   in_flight_tasks,
                   coalesce(
                       floor(
                           extract(
                               epoch from now - oldest_ready_created_at
                           )
                       ),
                       0
                   )::bigint as oldest_ready_age_seconds,
                   coalesce(
                       floor(
                           extract(
                               epoch from now - oldest_in_flight_locked_at
                           )
                       ),
                       0
                   )::bigint as oldest_in_flight_age_seconds
              from aggregates
            """,
        new MapSqlParameterSource(),
        (rs, rowNum) ->
            new QueueStateMetrics(
                rs.getLong("ready_tasks"),
                rs.getLong("in_flight_tasks"),
                rs.getLong("oldest_ready_age_seconds"),
                rs.getLong("oldest_in_flight_age_seconds")));
  }

  /**
   * Загружает lag-метрики по каждой партиции.
   *
   * @param partitionCount количество логических партиций
   * @return список снимков по партициям
   */
  public List<PartitionLagMetrics> loadPartitionLagMetrics(int partitionCount) {
    return jdbc.query(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            select q.partition_num,
                   count(q.task_id) as ready_tasks,
                   coalesce(
                       floor(
                           extract(
                               epoch from runtime_clock.now - min(q.created_at)
                           )
                       ),
                       0
                   )::bigint as oldest_ready_age_seconds
              from task_queue q
             cross join runtime_clock
             where q.worker_id is null
               and q.available_at <= runtime_clock.now
               and q.partition_num between 1 and :partitionCount
             group by q.partition_num, runtime_clock.now
             order by q.partition_num
            """,
        new MapSqlParameterSource("partitionCount", partitionCount),
        (rs, rowNum) ->
            new PartitionLagMetrics(
                rs.getInt("partition_num"),
                rs.getLong("ready_tasks"),
                rs.getLong("oldest_ready_age_seconds")));
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
    return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
  }

  /**
   * Aggregate-снимок состояния очереди для метрик.
   *
   * @param readyTasks количество доступных незахваченных задач
   * @param inFlightTasks количество задач, закрепленных за воркерами
   * @param oldestReadyAgeSeconds возраст самой старой доступной задачи в секундах
   * @param oldestInFlightAgeSeconds возраст самой старой in-flight задачи в секундах
   */
  public record QueueStateMetrics(
      long readyTasks,
      long inFlightTasks,
      long oldestReadyAgeSeconds,
      long oldestInFlightAgeSeconds) {}

  /**
   * Снимок lag-метрик конкретной партиции.
   *
   * @param partitionNum номер партиции
   * @param readyTasks количество готовых задач в партиции
   * @param oldestReadyAgeSeconds возраст самой старой готовой задачи в секундах
   */
  public record PartitionLagMetrics(
      int partitionNum, long readyTasks, long oldestReadyAgeSeconds) {}
}
