package dev.verkhovskiy.taskqueue.metrics;

import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository.PartitionLagMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository.QueueStateMetrics;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/** Обертка над Micrometer-метриками подсистемы очереди задач. */
@Component
@SuppressFBWarnings(
    value = "EI_EXPOSE_REP2",
    justification = "MeterRegistry is an injected infrastructure bean owned by the container.")
public class TaskQueueMetrics {

  private final Counter registerSuccess;
  private final Counter registerFailure;
  private final Counter heartbeatSuccess;
  private final Counter heartbeatFailure;
  private final Counter heartbeatTimeout;
  private final Counter heartbeatNotFound;
  private final Counter cleanupRuns;
  private final Counter cleanupRemoved;
  private final Counter cleanupExpiredTaskLeasesReleased;
  private final Counter retryScheduled;
  private final Counter retryExhausted;
  private final Counter nonRetryable;
  private final Counter deadLettered;
  private final Counter deadLetterDeleted;
  private final Counter deadLetterRequeued;
  private final Counter rebalanceRuns;
  private final Counter rebalanceFailures;
  private final Counter handoffStarted;
  private final Counter handoffCompleted;
  private final Counter handoffCancelled;
  private final Counter handoffTimeout;
  private final Counter handoffTimeoutExtended;
  private final Counter handoffTimeoutAborted;
  private final Counter handoffTimeoutForced;
  private final Timer heartbeatLatency;
  private final Timer rebalanceLatency;
  private final Timer handoffDuration;
  private final AtomicInteger drainingAssignments;
  private final AtomicLong readyTasks;
  private final AtomicLong inFlightTasks;
  private final AtomicLong oldestReadyTaskAgeSeconds;
  private final AtomicLong oldestInFlightTaskAgeSeconds;
  private final MeterRegistry meterRegistry;
  private final Map<Integer, AtomicLong> partitionReadyTasks = new ConcurrentHashMap<>();
  private final Map<Integer, AtomicLong> partitionOldestReadyAgeSeconds = new ConcurrentHashMap<>();

  /**
   * Создает и регистрирует счетчики/таймеры очереди задач.
   *
   * @param meterRegistry реестр метрик
   */
  public TaskQueueMetrics(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    registerSuccess = meterRegistry.counter("task.queue.process.register.success");
    registerFailure = meterRegistry.counter("task.queue.process.register.failure");
    heartbeatSuccess = meterRegistry.counter("task.queue.process.heartbeat.success");
    heartbeatFailure = meterRegistry.counter("task.queue.process.heartbeat.failure");
    heartbeatTimeout = meterRegistry.counter("task.queue.process.heartbeat.timeout");
    heartbeatNotFound = meterRegistry.counter("task.queue.process.heartbeat.not_found");
    cleanupRuns = meterRegistry.counter("task.queue.process.cleanup.runs");
    cleanupRemoved = meterRegistry.counter("task.queue.process.cleanup.removed");
    cleanupExpiredTaskLeasesReleased =
        meterRegistry.counter("task.queue.process.cleanup.expired_task_leases.released");
    retryScheduled = meterRegistry.counter("task.queue.tasks.retry.scheduled");
    retryExhausted = meterRegistry.counter("task.queue.tasks.retry.exhausted");
    nonRetryable = meterRegistry.counter("task.queue.tasks.non_retryable");
    deadLettered = meterRegistry.counter("task.queue.tasks.dead_lettered");
    deadLetterDeleted = meterRegistry.counter("task.queue.tasks.dead_letter.deleted");
    deadLetterRequeued = meterRegistry.counter("task.queue.tasks.dead_letter.requeued");
    rebalanceRuns = meterRegistry.counter("task.queue.process.rebalance.runs");
    rebalanceFailures = meterRegistry.counter("task.queue.process.rebalance.failures");
    handoffStarted = meterRegistry.counter("task.queue.process.handoff.started");
    handoffCompleted = meterRegistry.counter("task.queue.process.handoff.completed");
    handoffCancelled = meterRegistry.counter("task.queue.process.handoff.cancelled");
    handoffTimeout = meterRegistry.counter("task.queue.process.handoff.timeout");
    handoffTimeoutExtended = meterRegistry.counter("task.queue.process.handoff.timeout.extended");
    handoffTimeoutAborted = meterRegistry.counter("task.queue.process.handoff.timeout.aborted");
    handoffTimeoutForced = meterRegistry.counter("task.queue.process.handoff.timeout.forced");
    heartbeatLatency = meterRegistry.timer("task.queue.process.heartbeat.latency");
    rebalanceLatency = meterRegistry.timer("task.queue.process.rebalance.latency");
    handoffDuration = meterRegistry.timer("task.queue.process.handoff.duration");
    drainingAssignments = new AtomicInteger(0);
    readyTasks = new AtomicLong(0);
    inFlightTasks = new AtomicLong(0);
    oldestReadyTaskAgeSeconds = new AtomicLong(0);
    oldestInFlightTaskAgeSeconds = new AtomicLong(0);
    meterRegistry.gauge("task.queue.process.handoff.draining.partitions", drainingAssignments);
    meterRegistry.gauge("task.queue.tasks.ready", readyTasks);
    meterRegistry.gauge("task.queue.tasks.in_flight", inFlightTasks);
    meterRegistry.gauge("task.queue.tasks.oldest_ready_age.seconds", oldestReadyTaskAgeSeconds);
    meterRegistry.gauge(
        "task.queue.tasks.oldest_in_flight_age.seconds", oldestInFlightTaskAgeSeconds);
  }

  /** Увеличивает счетчик успешной регистрации воркера. */
  public void registerSuccess() {
    registerSuccess.increment();
  }

  /** Увеличивает счетчик неуспешной регистрации воркера. */
  public void registerFailure() {
    registerFailure.increment();
  }

  /**
   * Фиксирует успешный heartbeat и его длительность.
   *
   * @param durationNanos длительность в наносекундах
   */
  public void heartbeatSuccess(long durationNanos) {
    heartbeatSuccess.increment();
    heartbeatLatency.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  /** Увеличивает счетчик ошибок heartbeat. */
  public void heartbeatFailure() {
    heartbeatFailure.increment();
  }

  /** Увеличивает счетчик heartbeat-таймаутов. */
  public void heartbeatTimeout() {
    heartbeatTimeout.increment();
  }

  /** Увеличивает счетчик heartbeat для отсутствующего воркера. */
  public void heartbeatNotFound() {
    heartbeatNotFound.increment();
  }

  /**
   * Фиксирует запуск очистки "мертвых" воркеров и количество удаленных записей.
   *
   * @param removedCount количество удаленных воркеров
   */
  public void cleanupRun(int removedCount) {
    cleanupRuns.increment();
    if (removedCount > 0) {
      cleanupRemoved.increment(removedCount);
    }
  }

  /**
   * Фиксирует количество освобожденных задач с истекшим lease.
   *
   * @param releasedCount количество освобожденных задач
   */
  public void expiredTaskLeasesReleased(int releasedCount) {
    if (releasedCount > 0) {
      cleanupExpiredTaskLeasesReleased.increment(releasedCount);
    }
  }

  /** Увеличивает счетчик задач, запланированных на retry. */
  public void retryScheduled() {
    retryScheduled.increment();
  }

  /** Увеличивает счетчик задач с исчерпанными retry. */
  public void retryExhausted() {
    retryExhausted.increment();
  }

  /** Увеличивает счетчик non-retryable задач. */
  public void nonRetryable() {
    nonRetryable.increment();
  }

  /** Увеличивает счетчик задач, перенесенных в dead-letter. */
  public void deadLettered() {
    deadLettered.increment();
  }

  /**
   * Увеличивает счетчик удаленных dead-letter задач.
   *
   * @param deletedCount количество удаленных записей
   */
  public void deadLetterDeleted(int deletedCount) {
    if (deletedCount > 0) {
      deadLetterDeleted.increment(deletedCount);
    }
  }

  /** Увеличивает счетчик задач, возвращенных из dead-letter в основную очередь. */
  public void deadLetterRequeued() {
    deadLetterRequeued.increment();
  }

  /**
   * Оборачивает выполнение ребаланса для учета количества и длительности.
   *
   * @param callable бизнес-логика ребаланса
   * @param <T> тип результата
   * @throws Exception ошибка выполнения callable
   */
  public <T> void recordRebalance(RebalanceCallable<T> callable) throws Exception {
    rebalanceRuns.increment();
    rebalanceLatency.recordCallable(callable::call);
  }

  /** Увеличивает счетчик ошибок ребаланса. */
  public void rebalanceFailure() {
    rebalanceFailures.increment();
  }

  /** Увеличивает счетчик запусков handoff в DRAINING. */
  public void handoffStarted() {
    handoffStarted.increment();
  }

  /** Фиксирует успешное завершение handoff и его длительность. */
  public void handoffCompleted(Duration duration) {
    handoffCompleted.increment();
    recordHandoffDuration(duration);
  }

  /** Фиксирует отмену handoff и его длительность. */
  public void handoffCancelled(Duration duration) {
    handoffCancelled.increment();
    recordHandoffDuration(duration);
  }

  /** Увеличивает счетчик таймаутов handoff. */
  public void handoffTimeout() {
    handoffTimeout.increment();
  }

  /** Увеличивает счетчик продлений дедлайна handoff после таймаута. */
  public void handoffTimeoutExtended() {
    handoffTimeoutExtended.increment();
  }

  /** Фиксирует таймаут с политикой ABORT и длительность handoff. */
  public void handoffTimeoutAborted(Duration duration) {
    handoffTimeoutAborted.increment();
    recordHandoffDuration(duration);
  }

  /** Фиксирует таймаут с политикой FORCE и длительность handoff. */
  public void handoffTimeoutForced(Duration duration) {
    handoffTimeoutForced.increment();
    recordHandoffDuration(duration);
  }

  /** Обновляет gauge количества партиций в DRAINING. */
  public void setDrainingAssignments(int count) {
    drainingAssignments.set(Math.max(0, count));
  }

  /**
   * Обновляет aggregate-gauges состояния очереди.
   *
   * @param snapshot снимок состояния очереди
   */
  public void setQueueState(QueueStateMetrics snapshot) {
    if (snapshot == null) {
      return;
    }
    readyTasks.set(Math.max(0L, snapshot.readyTasks()));
    inFlightTasks.set(Math.max(0L, snapshot.inFlightTasks()));
    oldestReadyTaskAgeSeconds.set(Math.max(0L, snapshot.oldestReadyAgeSeconds()));
    oldestInFlightTaskAgeSeconds.set(Math.max(0L, snapshot.oldestInFlightAgeSeconds()));
  }

  /**
   * Обновляет per-partition lag gauges.
   *
   * @param snapshots снимки по партициям
   */
  public void setPartitionLag(List<PartitionLagMetrics> snapshots) {
    if (snapshots == null) {
      return;
    }
    Set<Integer> updatedPartitions = new HashSet<>(snapshots.size());
    snapshots.forEach(
        snapshot -> {
          updatedPartitions.add(snapshot.partitionNum());
          partitionGauge(partitionReadyTasks, "task.queue.partition.ready", snapshot.partitionNum())
              .set(Math.max(0L, snapshot.readyTasks()));
          partitionGauge(
                  partitionOldestReadyAgeSeconds,
                  "task.queue.partition.oldest_ready_age.seconds",
                  snapshot.partitionNum())
              .set(Math.max(0L, snapshot.oldestReadyAgeSeconds()));
        });
    partitionReadyTasks.forEach(
        (partitionNum, gauge) -> {
          if (!updatedPartitions.contains(partitionNum)) {
            gauge.set(0L);
          }
        });
    partitionOldestReadyAgeSeconds.forEach(
        (partitionNum, gauge) -> {
          if (!updatedPartitions.contains(partitionNum)) {
            gauge.set(0L);
          }
        });
  }

  private AtomicLong partitionGauge(
      Map<Integer, AtomicLong> gauges, String name, int partitionNum) {
    return gauges.computeIfAbsent(
        partitionNum,
        key -> {
          AtomicLong value = new AtomicLong(0);
          meterRegistry.gauge(name, Tags.of("partition", Integer.toString(key)), value);
          return value;
        });
  }

  private void recordHandoffDuration(Duration duration) {
    if (duration != null && !duration.isNegative()) {
      handoffDuration.record(duration);
    }
  }

  /**
   * Функциональный интерфейс для оборачивания ребаланса с возможностью checked-исключений.
   *
   * @param <T> тип результата выполнения
   */
  @FunctionalInterface
  public interface RebalanceCallable<T> {
    /**
     * Выполняет действие ребаланса.
     *
     * @return результат выполнения
     * @throws Exception ошибка выполнения
     */
    T call() throws Exception;
  }
}
