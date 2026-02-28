package dev.verkhovskiy.taskqueue.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
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
  private final Timer heartbeatLatency;
  private final Timer rebalanceLatency;

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
    heartbeatLatency = meterRegistry.timer("task.queue.process.heartbeat.latency");
    rebalanceLatency = meterRegistry.timer("task.queue.process.rebalance.latency");
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
