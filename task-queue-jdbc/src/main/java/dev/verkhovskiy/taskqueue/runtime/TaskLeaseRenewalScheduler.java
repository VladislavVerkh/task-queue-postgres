package dev.verkhovskiy.taskqueue.runtime;

import dev.verkhovskiy.taskqueue.exception.TaskOwnershipLostException;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.service.TaskQueueService;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import lombok.extern.slf4j.Slf4j;

/** Общий scheduler продления lease для всех in-flight задач runtime. */
@Slf4j
final class TaskLeaseRenewalScheduler {

  private final ScheduledExecutorService executor;
  private final Duration renewInterval;
  private final BooleanSupplier running;
  private final TaskQueueService queueService;
  private final TaskQueueMetrics metrics;
  private final LeaseRenewalFailureHandler failureHandler;
  private final List<TaskLeaseMonitor> monitors = new CopyOnWriteArrayList<>();

  private ScheduledFuture<?> future;

  TaskLeaseRenewalScheduler(
      ScheduledExecutorService executor,
      Duration renewInterval,
      BooleanSupplier running,
      TaskQueueService queueService,
      TaskQueueMetrics metrics,
      LeaseRenewalFailureHandler failureHandler) {
    this.executor = Objects.requireNonNull(executor, "executor");
    this.renewInterval = Objects.requireNonNull(renewInterval, "renewInterval");
    this.running = Objects.requireNonNull(running, "running");
    this.queueService = Objects.requireNonNull(queueService, "queueService");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.failureHandler = Objects.requireNonNull(failureHandler, "failureHandler");
  }

  void start() {
    long renewIntervalMillis = Math.max(1L, renewInterval.toMillis());
    future =
        executor.scheduleWithFixedDelay(
            this::renewActiveLeases,
            renewIntervalMillis,
            renewIntervalMillis,
            TimeUnit.MILLISECONDS);
  }

  TaskLeaseMonitor register(String workerId, UUID taskId, Thread processingThread) {
    TaskLeaseMonitor monitor = new TaskLeaseMonitor(workerId, taskId, processingThread);
    monitors.add(monitor);
    updateActiveLeaseMonitorMetric();
    return monitor;
  }

  void stop() {
    ScheduledFuture<?> scheduledFuture = future;
    if (scheduledFuture != null) {
      scheduledFuture.cancel(true);
    }
    monitors.forEach(TaskLeaseMonitor::cancel);
    monitors.clear();
    updateActiveLeaseMonitorMetric();
  }

  void renewActiveLeases() {
    if (!running.getAsBoolean() || Thread.currentThread().isInterrupted()) {
      return;
    }

    removeCancelledMonitors();
    try {
      for (TaskLeaseMonitor monitor : monitors) {
        monitor.renewLease();
      }
    } finally {
      removeCancelledMonitors();
    }
  }

  private void unregister(TaskLeaseMonitor monitor) {
    monitor.cancel();
    monitors.remove(monitor);
    updateActiveLeaseMonitorMetric();
  }

  private void removeCancelledMonitors() {
    monitors.removeIf(TaskLeaseMonitor::cancelled);
    updateActiveLeaseMonitorMetric();
  }

  private void updateActiveLeaseMonitorMetric() {
    metrics.setActiveLeaseMonitors(monitors.size());
  }

  /** Callback фатальной ошибки renewal, привязанный к политике runtime. */
  @FunctionalInterface
  interface LeaseRenewalFailureHandler {

    void onFailure(UUID taskId, Throwable failure);
  }

  /** Монитор продления lease конкретной in-flight задачи. */
  final class TaskLeaseMonitor {
    private final String workerId;
    private final UUID taskId;
    private final Thread processingThread;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private TaskLeaseMonitor(String workerId, UUID taskId, Thread processingThread) {
      this.workerId = workerId;
      this.taskId = taskId;
      this.processingThread = processingThread;
    }

    void stop() {
      unregister(this);
    }

    private void cancel() {
      cancelled.set(true);
    }

    private boolean cancelled() {
      return cancelled.get();
    }

    private void renewLease() {
      if (!running.getAsBoolean() || cancelled.get() || Thread.currentThread().isInterrupted()) {
        return;
      }

      try {
        queueService.renewLease(taskId, workerId);
      } catch (TaskOwnershipLostException e) {
        metrics.leaseRenewalError();
        log.warn(
            "Task {} lease ownership lost by worker {}, interrupting processing thread",
            taskId,
            workerId);
        processingThread.interrupt();
        cancelled.set(true);
      } catch (RuntimeException e) {
        metrics.leaseRenewalError();
        cancelled.set(true);
        failureHandler.onFailure(taskId, e);
        throw e;
      } catch (Throwable e) {
        metrics.leaseRenewalError();
        cancelled.set(true);
        failureHandler.onFailure(taskId, e);
        rethrowUnchecked(e);
      }
    }
  }

  private static void rethrowUnchecked(Throwable failure) {
    if (failure instanceof Error error) {
      throw error;
    }
    if (failure instanceof RuntimeException runtimeException) {
      throw runtimeException;
    }
    throw new IllegalStateException(failure);
  }
}
