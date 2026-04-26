package dev.verkhovskiy.taskqueue.persistence;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.exception.WorkerRegistrationAlreadyExistsException;
import dev.verkhovskiy.taskqueue.exception.WorkerRegistrationNotFoundException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Репозиторий операций по регистрации воркеров и закреплению партиций. */
@Repository
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification = "Spring JdbcTemplate collaborators are injected infrastructure beans.")
public class WorkerRegistryRepository {

  private final NamedParameterJdbcTemplate jdbc;
  private final JdbcTemplate plainJdbc;

  public WorkerRegistryRepository(
      @Qualifier(TaskQueueBeanNames.NAMED_PARAMETER_JDBC_TEMPLATE) NamedParameterJdbcTemplate jdbc,
      @Qualifier(TaskQueueBeanNames.JDBC_TEMPLATE) JdbcTemplate plainJdbc) {
    this.jdbc = jdbc;
    this.plainJdbc = plainJdbc;
  }

  /**
   * Берет разделяемую advisory-блокировку в рамках текущей транзакции.
   *
   * @param lockKey ключ advisory-блокировки
   */
  public void lockShared(long lockKey) {
    plainJdbc.query("select pg_advisory_xact_lock_shared(?)", rs -> {}, lockKey);
  }

  /**
   * Берет эксклюзивную advisory-блокировку в рамках текущей транзакции.
   *
   * @param lockKey ключ advisory-блокировки
   */
  public void lockExclusive(long lockKey) {
    plainJdbc.query("select pg_advisory_xact_lock(?)", rs -> {}, lockKey);
  }

  /**
   * Регистрирует нового воркера.
   *
   * @param workerId идентификатор воркера
   * @param timeoutSeconds таймаут heartbeat в секундах
   * @throws WorkerRegistrationAlreadyExistsException если воркер уже зарегистрирован
   */
  public void registerWorker(String workerId, long timeoutSeconds) {
    try {
      jdbc.update(
          """
                with runtime_clock as (
                    select clock_timestamp() as now
                )
                insert into task_worker_registry(worker_id, heartbeat_last, timeout_sec, created_at)
                select :workerId, runtime_clock.now, :timeoutSec, runtime_clock.now
                  from runtime_clock
                """,
          new MapSqlParameterSource()
              .addValue("workerId", workerId)
              .addValue("timeoutSec", timeoutSeconds));
    } catch (DuplicateKeyException e) {
      throw new WorkerRegistrationAlreadyExistsException(workerId);
    }
  }

  /**
   * Обновляет heartbeat воркера.
   *
   * @param workerId идентификатор воркера
   * @throws WorkerRegistrationNotFoundException если воркер не найден
   */
  public void heartbeatWorker(String workerId) {
    int updated =
        jdbc.update(
            """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            update task_worker_registry
               set heartbeat_last = runtime_clock.now
              from runtime_clock
             where worker_id = :workerId
            """,
            new MapSqlParameterSource().addValue("workerId", workerId));
    if (updated == 0) {
      throw new WorkerRegistrationNotFoundException(workerId);
    }
  }

  /**
   * Возвращает список просроченных воркеров и блокирует выбранные строки.
   *
   * @param heartbeatDeviationSec допустимое отклонение heartbeat в секундах
   * @param timeoutMultiplier множитель timeout-а из конфигурации воркера
   * @param limit максимум возвращаемых воркеров
   * @return список идентификаторов "мертвых" воркеров
   */
  public List<String> findExpiredWorkerIdsForUpdate(
      long heartbeatDeviationSec, int timeoutMultiplier, int limit) {
    return jdbc.query(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            select w.worker_id
              from task_worker_registry w
             cross join runtime_clock
             where w.heartbeat_last < runtime_clock.now
                                     - make_interval(secs => w.timeout_sec * :timeoutMultiplier)
                                     - make_interval(secs => :heartbeatDeviationSec)
             order by w.heartbeat_last
             for update of w skip locked
             limit :limit
            """,
        new MapSqlParameterSource()
            .addValue("heartbeatDeviationSec", heartbeatDeviationSec)
            .addValue("timeoutMultiplier", timeoutMultiplier)
            .addValue("limit", limit),
        new SingleColumnRowMapper<>(String.class));
  }

  /**
   * Возвращает все живые воркеры в стабильном порядке регистрации.
   *
   * @return список идентификаторов воркеров
   */
  public List<String> findAllWorkerIdsOrdered() {
    List<String> workerIds =
        jdbc.query(
            """
            select worker_id
              from task_worker_registry
             order by created_at
            """,
            (rs, rowNum) -> rs.getString("worker_id"));
    return workerIds.stream().map(Objects::requireNonNull).toList();
  }

  /**
   * Загружает текущее закрепление партиций, включая состояние handoff.
   *
   * @param maxPartitionNum верхняя граница номера партиции
   * @return отображение {@code partitionNum -> assignment}
   */
  public Map<Integer, PartitionAssignment> findPartitionAssignments(int maxPartitionNum) {
    return jdbc.query(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            select a.partition_num,
                   a.worker_id,
                   a.handoff_state,
                   a.pending_worker_id,
                   a.drain_started_at,
                   a.drain_deadline_at,
                   (
                       a.drain_deadline_at is null
                       or runtime_clock.now >= a.drain_deadline_at
                   ) as drain_deadline_reached,
                   coalesce(
                       greatest(
                           (extract(epoch from runtime_clock.now - a.drain_started_at) * 1000)::bigint,
                           0::bigint
                       ),
                       0
                   ) as drain_duration_millis
              from task_worker_partition_assignment a
             cross join runtime_clock
             where a.partition_num <= :maxPartitionNum
             order by a.partition_num
            """,
        new MapSqlParameterSource("maxPartitionNum", maxPartitionNum),
        rs -> {
          Map<Integer, PartitionAssignment> result = new LinkedHashMap<>();
          while (rs.next()) {
            int partitionNum = rs.getInt("partition_num");
            result.put(
                partitionNum,
                new PartitionAssignment(
                    partitionNum,
                    rs.getString("worker_id"),
                    HandoffState.fromDbValue(rs.getString("handoff_state")),
                    rs.getString("pending_worker_id"),
                    toInstant(rs.getObject("drain_started_at", OffsetDateTime.class)),
                    toInstant(rs.getObject("drain_deadline_at", OffsetDateTime.class)),
                    rs.getBoolean("drain_deadline_reached"),
                    Duration.ofMillis(rs.getLong("drain_duration_millis"))));
          }
          return result;
        });
  }

  /**
   * Создает или переводит assignment в ACTIVE с указанным владельцем.
   *
   * <p>Если владелец меняется, увеличивает owner_change_count и обновляет owner_changed_at.
   */
  public void upsertActiveAssignment(int partitionNum, String workerId) {
    jdbc.update(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            insert into task_worker_partition_assignment(
                partition_num,
                worker_id,
                handoff_state,
                pending_worker_id,
                drain_started_at,
                drain_deadline_at,
                owner_changed_at,
                owner_change_count
            )
            select
                :partitionNum,
                :workerId,
                'ACTIVE',
                null,
                null,
                null,
                runtime_clock.now,
                1
              from runtime_clock
            on conflict (partition_num)
            do update
                  set worker_id = excluded.worker_id,
                      handoff_state = 'ACTIVE',
                      pending_worker_id = null,
                      drain_started_at = null,
                      drain_deadline_at = null,
                      owner_changed_at =
                          case
                              when task_worker_partition_assignment.worker_id is distinct from excluded.worker_id
                              then excluded.owner_changed_at
                              else task_worker_partition_assignment.owner_changed_at
                          end,
                      owner_change_count =
                          case
                              when task_worker_partition_assignment.worker_id is distinct from excluded.worker_id
                              then task_worker_partition_assignment.owner_change_count + 1
                              else task_worker_partition_assignment.owner_change_count
                          end
            """,
        new MapSqlParameterSource()
            .addValue("partitionNum", partitionNum)
            .addValue("workerId", workerId));
  }

  /** Переводит assignment партиции в состояние DRAINING. */
  public void startDraining(int partitionNum, String pendingWorkerId, Duration drainTimeout) {
    jdbc.update(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            update task_worker_partition_assignment a
               set handoff_state = 'DRAINING',
                   pending_worker_id = :pendingWorkerId,
                   drain_started_at = runtime_clock.now,
                   drain_deadline_at =
                       runtime_clock.now + (:drainTimeoutMillis * interval '1 millisecond')
              from runtime_clock
             where a.partition_num = :partitionNum
            """,
        new MapSqlParameterSource()
            .addValue("partitionNum", partitionNum)
            .addValue("pendingWorkerId", pendingWorkerId)
            .addValue("drainTimeoutMillis", Math.max(1L, drainTimeout.toMillis())));
  }

  /** Обновляет pending-owner для уже начатого дренажа. */
  public void updatePendingWorker(int partitionNum, String pendingWorkerId) {
    jdbc.update(
        """
            update task_worker_partition_assignment
               set pending_worker_id = :pendingWorkerId
             where partition_num = :partitionNum
            """,
        new MapSqlParameterSource()
            .addValue("partitionNum", partitionNum)
            .addValue("pendingWorkerId", pendingWorkerId));
  }

  /** Продлевает дедлайн дренажа для партиции. */
  public void extendDrainDeadline(int partitionNum, Duration drainTimeout) {
    jdbc.update(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            update task_worker_partition_assignment
               set drain_deadline_at =
                       runtime_clock.now + (:drainTimeoutMillis * interval '1 millisecond')
              from runtime_clock
             where partition_num = :partitionNum
            """,
        new MapSqlParameterSource()
            .addValue("partitionNum", partitionNum)
            .addValue("drainTimeoutMillis", Math.max(1L, drainTimeout.toMillis())));
  }

  /** Отменяет handoff и возвращает assignment в ACTIVE у текущего владельца. */
  public void cancelDraining(int partitionNum) {
    jdbc.update(
        """
            update task_worker_partition_assignment
               set handoff_state = 'ACTIVE',
                   pending_worker_id = null,
                   drain_started_at = null,
                   drain_deadline_at = null
             where partition_num = :partitionNum
            """,
        new MapSqlParameterSource("partitionNum", partitionNum));
  }

  /** Завершает handoff партиции на нового владельца. */
  public void completeHandoff(int partitionNum, String newWorkerId) {
    jdbc.update(
        """
            with runtime_clock as (
                select clock_timestamp() as now
            )
            update task_worker_partition_assignment
               set worker_id = :newWorkerId,
                   handoff_state = 'ACTIVE',
                   pending_worker_id = null,
                   drain_started_at = null,
                   drain_deadline_at = null,
                   owner_changed_at =
                       case
                           when worker_id is distinct from :newWorkerId
                           then runtime_clock.now
                           else owner_changed_at
                       end,
                   owner_change_count =
                       case
                           when worker_id is distinct from :newWorkerId
                           then owner_change_count + 1
                           else owner_change_count
                       end
              from runtime_clock
             where partition_num = :partitionNum
            """,
        new MapSqlParameterSource()
            .addValue("partitionNum", partitionNum)
            .addValue("newWorkerId", newWorkerId));
  }

  /** Возвращает количество партиций в состоянии DRAINING. */
  public int countDrainingAssignments() {
    Integer count =
        jdbc.queryForObject(
            """
            select count(*)
              from task_worker_partition_assignment
             where handoff_state = 'DRAINING'
            """,
            new MapSqlParameterSource(),
            Integer.class);
    return count == null ? 0 : count;
  }

  /**
   * Удаляет воркера из реестра.
   *
   * @param workerId идентификатор воркера
   */
  public void removeWorker(String workerId) {
    jdbc.update(
        "delete from task_worker_registry where worker_id = :workerId",
        new MapSqlParameterSource("workerId", workerId));
  }

  /** Удаляет все закрепления партиций. */
  public void clearAssignments() {
    jdbc.update("delete from task_worker_partition_assignment", new MapSqlParameterSource());
  }

  /**
   * Удаляет закрепления для партиций выше заданной границы.
   *
   * @param maxPartitionNum максимально допустимый номер партиции
   */
  public void removeAssignmentsAbovePartition(int maxPartitionNum) {
    jdbc.update(
        "delete from task_worker_partition_assignment where partition_num > :maxPartitionNum",
        new MapSqlParameterSource("maxPartitionNum", maxPartitionNum));
  }

  private static Instant toInstant(OffsetDateTime value) {
    return value == null ? null : value.toInstant();
  }

  /** Состояние handoff закрепления партиции. */
  public enum HandoffState {
    ACTIVE,
    DRAINING;

    static HandoffState fromDbValue(String value) {
      return HandoffState.valueOf(value);
    }
  }

  /**
   * Снимок текущего assignment партиции.
   *
   * @param partitionNum номер партиции
   * @param workerId текущий владелец
   * @param handoffState состояние handoff
   * @param pendingWorkerId ожидающий владелец при DRAINING
   * @param drainStartedAt время старта дренажа
   * @param drainDeadlineAt дедлайн дренажа
   * @param drainDeadlineReached {@code true}, если PostgreSQL-время достигло дедлайна дренажа
   * @param drainDuration длительность дренажа относительно текущего PostgreSQL-времени
   */
  public record PartitionAssignment(
      int partitionNum,
      String workerId,
      HandoffState handoffState,
      String pendingWorkerId,
      Instant drainStartedAt,
      Instant drainDeadlineAt,
      boolean drainDeadlineReached,
      Duration drainDuration) {

    /** Возвращает {@code true}, если партиция находится в DRAINING. */
    public boolean draining() {
      return handoffState == HandoffState.DRAINING;
    }
  }
}
