package dev.verkhovskiy.taskqueue.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/** Обертка над Micrometer-метриками подсистемы очереди задач. */
@Component
public class TaskQueueMetrics {

  private final Counter registerSuccess;
  private final Counter registerFailure;
  private final Counter heartbeatSuccess;
  private final Counter heartbeatFailure;
  private final Counter heartbeatTimeout;
  private final Counter heartbeatNotFound;
  private final Counter cleanupRuns;
  private final Counter cleanupRemoved;
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

  /**
   * Создает и регистрирует счетчики/таймеры очереди задач.
   *
   * @param meterRegistry реестр метрик
   */
  public TaskQueueMetrics(MeterRegistry meterRegistry) {
    registerSuccess = meterRegistry.counter("task.queue.process.register.success");
    registerFailure = meterRegistry.counter("task.queue.process.register.failure");
    heartbeatSuccess = meterRegistry.counter("task.queue.process.heartbeat.success");
    heartbeatFailure = meterRegistry.counter("task.queue.process.heartbeat.failure");
    heartbeatTimeout = meterRegistry.counter("task.queue.process.heartbeat.timeout");
    heartbeatNotFound = meterRegistry.counter("task.queue.process.heartbeat.not_found");
    cleanupRuns = meterRegistry.counter("task.queue.process.cleanup.runs");
    cleanupRemoved = meterRegistry.counter("task.queue.process.cleanup.removed");
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
    meterRegistry.gauge("task.queue.process.handoff.draining.partitions", drainingAssignments);
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
