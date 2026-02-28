package dev.verkhovskiy.taskqueue.persistence;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.exception.WorkerRegistrationAlreadyExistsException;
import dev.verkhovskiy.taskqueue.exception.WorkerRegistrationNotFoundException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
   * @param now текущее время
   * @throws WorkerRegistrationAlreadyExistsException если воркер уже зарегистрирован
   */
  public void registerWorker(String workerId, long timeoutSeconds, Instant now) {
    OffsetDateTime nowUtc = toOffsetDateTime(now);
    try {
      jdbc.update(
          """
                insert into task_worker_registry(worker_id, heartbeat_last, timeout_sec, created_at)
                values(:workerId, :heartbeatLast, :timeoutSec, :createdAt)
                """,
          new MapSqlParameterSource()
              .addValue("workerId", workerId)
              .addValue("heartbeatLast", nowUtc)
              .addValue("timeoutSec", timeoutSeconds)
              .addValue("createdAt", nowUtc));
    } catch (DuplicateKeyException e) {
      throw new WorkerRegistrationAlreadyExistsException(workerId);
    }
  }

  /**
   * Обновляет heartbeat воркера.
   *
   * @param workerId идентификатор воркера
   * @param heartbeatLast текущее значение heartbeat
   * @throws WorkerRegistrationNotFoundException если воркер не найден
   */
  public void heartbeatWorker(String workerId, Instant heartbeatLast) {
    int updated =
        jdbc.update(
            """
            update task_worker_registry
               set heartbeat_last = :heartbeatLast
             where worker_id = :workerId
            """,
            new MapSqlParameterSource()
                .addValue("heartbeatLast", toOffsetDateTime(heartbeatLast))
                .addValue("workerId", workerId));
    if (updated == 0) {
      throw new WorkerRegistrationNotFoundException(workerId);
    }
  }

  /**
   * Возвращает список просроченных воркеров и блокирует выбранные строки.
   *
   * @param now текущее время
   * @param heartbeatDeviationSec допустимое отклонение heartbeat в секундах
   * @param timeoutMultiplier множитель timeout-а из конфигурации воркера
   * @param limit максимум возвращаемых воркеров
   * @return список идентификаторов "мертвых" воркеров
   */
  public List<String> findExpiredWorkerIdsForUpdate(
      Instant now, long heartbeatDeviationSec, int timeoutMultiplier, int limit) {
    return jdbc.query(
        """
            select worker_id
              from task_worker_registry
             where heartbeat_last < :now
                                   - make_interval(secs => timeout_sec * :timeoutMultiplier)
                                   - make_interval(secs => :heartbeatDeviationSec)
             order by heartbeat_last
             for update skip locked
             limit :limit
            """,
        new MapSqlParameterSource()
            .addValue("now", toOffsetDateTime(now))
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
   * Загружает текущее закрепление партиций по воркерам.
   *
   * @param maxPartitionNum верхняя граница номера партиции
   * @return отображение {@code partitionNum -> workerId}
   */
  public Map<Integer, String> findCurrentPartitionAssignments(int maxPartitionNum) {
    return jdbc.query(
        """
            select partition_num, worker_id
              from task_worker_partition_assignment
             where partition_num <= :maxPartitionNum
             order by partition_num
            """,
        new MapSqlParameterSource("maxPartitionNum", maxPartitionNum),
        rs -> {
          Map<Integer, String> result = new LinkedHashMap<>();
          while (rs.next()) {
            result.put(rs.getInt("partition_num"), rs.getString("worker_id"));
          }
          return result;
        });
  }

  /**
   * Создает или обновляет закрепление партиции за воркером.
   *
   * @param partitionNum номер партиции
   * @param workerId идентификатор воркера
   * @param ownerChangedAt момент смены владельца
   */
  public void upsertPartitionAssignment(int partitionNum, String workerId, Instant ownerChangedAt) {
    jdbc.update(
        """
            insert into task_worker_partition_assignment(
                partition_num,
                worker_id,
                owner_changed_at,
                owner_change_count
            )
            values(
                :partitionNum,
                :workerId,
                :ownerChangedAt,
                1
            )
            on conflict (partition_num)
            do update
                  set worker_id = excluded.worker_id,
                      owner_changed_at = :ownerChangedAt,
                      owner_change_count = task_worker_partition_assignment.owner_change_count + 1
                where task_worker_partition_assignment.worker_id is distinct from excluded.worker_id
            """,
        new MapSqlParameterSource()
            .addValue("partitionNum", partitionNum)
            .addValue("workerId", workerId)
            .addValue("ownerChangedAt", toOffsetDateTime(ownerChangedAt)));
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
