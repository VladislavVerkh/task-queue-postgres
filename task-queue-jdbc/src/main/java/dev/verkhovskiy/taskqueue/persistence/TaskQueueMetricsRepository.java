package dev.verkhovskiy.taskqueue.persistence;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Репозиторий запросов снимков состояния очереди для метрик. */
@Repository
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification =
        "Spring JdbcTemplate is an injected infrastructure bean owned by the container.")
public class TaskQueueMetricsRepository {

  private final NamedParameterJdbcTemplate jdbc;

  public TaskQueueMetricsRepository(
      @Qualifier(TaskQueueBeanNames.NAMED_PARAMETER_JDBC_TEMPLATE)
          NamedParameterJdbcTemplate jdbc) {
    this.jdbc = jdbc;
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
