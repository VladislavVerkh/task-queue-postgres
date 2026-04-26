package dev.verkhovskiy.taskqueue.runtime;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.exception.TaskOwnershipLostException;
import dev.verkhovskiy.taskqueue.handler.TaskHandlerRegistry;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.service.TaskDeadLetterService;
import dev.verkhovskiy.taskqueue.service.TaskExecutionService;
import dev.verkhovskiy.taskqueue.service.TaskQueueService;
import dev.verkhovskiy.taskqueue.service.TaskRetryService;
import dev.verkhovskiy.taskqueue.service.WorkerCoordinationService;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Тесты фонового runtime-координатора обработки задач. */
@ExtendWith(MockitoExtension.class)
class QueueWorkerRuntimeTest {

  @Mock private WorkerCoordinationService workerCoordinationService;
  @Mock private TaskQueueService queueService;
  @Mock private TaskRetryService retryService;
  @Mock private TaskDeadLetterService deadLetterService;
  @Mock private TaskHandlerRegistry handlerRegistry;
  @Mock private TaskExecutionService taskExecutionService;
  @Mock private TaskQueueMetrics metrics;
  @Mock private TaskQueueRuntimeShutdownStrategy shutdownStrategy;

  private TaskQueueProperties properties;

  @BeforeEach
  void setUp() {
    properties = new TaskQueueProperties();
    properties.setWorkerCount(2);
    properties.setPollBatchSize(1);
    properties.setPollInterval(Duration.ofMillis(25));
    properties.setTaskLeaseTimeout(Duration.ofMillis(60));
    properties.setHeartbeatInterval(Duration.ofSeconds(30));
    properties.setHeartbeatTaskTimeout(Duration.ofSeconds(1));
    properties.setProcessTimeout(Duration.ofSeconds(60));
    properties.setCleanupInterval(Duration.ofSeconds(30));
    properties.setQueueMetricsInterval(Duration.ofSeconds(30));
    properties.setHandoffReconcileInterval(Duration.ofSeconds(30));
    properties.setShutdownTimeout(Duration.ofMillis(500));
  }

  @Test
  void sharedLeaseSchedulerKeepsRenewingOtherTasksWhenOneTaskLosesOwnership() throws Exception {
    UUID lostTaskId = UUID.randomUUID();
    UUID healthyTaskId = UUID.randomUUID();
    QueuedTask lostTask = queuedTask(lostTaskId, "lost-key", 1);
    QueuedTask healthyTask = queuedTask(healthyTaskId, "healthy-key", 2);
    CountDownLatch handlersStarted = new CountDownLatch(2);
    CountDownLatch releaseHandlers = new CountDownLatch(1);
    AtomicInteger dequeues = new AtomicInteger();
    AtomicInteger lostRenewals = new AtomicInteger();
    AtomicInteger healthyRenewals = new AtomicInteger();

    when(queueService.dequeueForWorker(anyString(), anyInt()))
        .thenAnswer(
            invocation -> {
              int dequeue = dequeues.getAndIncrement();
              if (dequeue == 0) {
                return List.of(lostTask);
              }
              if (dequeue == 1) {
                return List.of(healthyTask);
              }
              return List.of();
            });
    doAnswer(
            invocation -> {
              UUID taskId = invocation.getArgument(0);
              String workerId = invocation.getArgument(1);
              if (lostTaskId.equals(taskId)) {
                lostRenewals.incrementAndGet();
                throw new TaskOwnershipLostException(taskId, workerId);
              }
              if (healthyTaskId.equals(taskId)) {
                healthyRenewals.incrementAndGet();
              }
              return null;
            })
        .when(queueService)
        .renewLease(any(UUID.class), anyString());
    doAnswer(
            invocation -> {
              handlersStarted.countDown();
              waitIgnoringInterrupts(releaseHandlers);
              return null;
            })
        .when(taskExecutionService)
        .handleAndAcknowledge(any(QueuedTask.class), anyString());

    QueueWorkerRuntime runtime =
        new QueueWorkerRuntime(
            properties,
            workerCoordinationService,
            queueService,
            retryService,
            deadLetterService,
            handlerRegistry,
            taskExecutionService,
            metrics,
            shutdownStrategy);

    runtime.start();
    try {
      assertTrue(handlersStarted.await(2, TimeUnit.SECONDS));
      assertTrue(
          waitUntil(
              () -> lostRenewals.get() >= 1 && healthyRenewals.get() >= 2, Duration.ofSeconds(2)));
    } finally {
      releaseHandlers.countDown();
      runtime.stop();
    }

    verify(shutdownStrategy, org.mockito.Mockito.never()).shutdown(anyInt(), anyString(), any());
    verify(metrics, atLeastOnce()).setActiveLeaseMonitors(2);
    verify(metrics, atLeastOnce()).setActiveLeaseMonitors(0);
    verify(metrics).leaseRenewalError();
    verify(retryService, org.mockito.Mockito.never())
        .retryOrFinalize(any(UUID.class), anyLong(), any(Throwable.class), anyString());
  }

  private static QueuedTask queuedTask(UUID taskId, String partitionKey, int partitionNum) {
    Instant now = Instant.now();
    return new QueuedTask(taskId, "runtime-test", "{}", partitionKey, partitionNum, now, 0, now);
  }

  private static boolean waitUntil(BooleanSupplier condition, Duration timeout)
      throws InterruptedException {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      if (condition.getAsBoolean()) {
        return true;
      }
      Thread.sleep(10);
    }
    return condition.getAsBoolean();
  }

  private static void waitIgnoringInterrupts(CountDownLatch latch) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          if (latch.await(50, TimeUnit.MILLISECONDS)) {
            return;
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
