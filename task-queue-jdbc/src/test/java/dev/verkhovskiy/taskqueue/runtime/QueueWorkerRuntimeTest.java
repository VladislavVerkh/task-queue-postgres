package dev.verkhovskiy.taskqueue.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.domain.QueuedTask;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
  void stopCancelsLeaseSchedulerBeforeWaitingForWorkerExecutor() throws Exception {
    UUID taskId = UUID.randomUUID();
    QueuedTask task = queuedTask(taskId, "key", 1);
    CountDownLatch handlerStarted = new CountDownLatch(1);
    CountDownLatch releaseHandler = new CountDownLatch(1);
    CountDownLatch activeMonitorsReset = new CountDownLatch(1);
    AtomicInteger dequeues = new AtomicInteger();
    AtomicInteger renewals = new AtomicInteger();
    AtomicInteger activeLeaseMonitors = new AtomicInteger(-1);
    AtomicBoolean handlerCompleted = new AtomicBoolean(false);

    properties.setWorkerCount(1);
    when(queueService.dequeueForWorker(anyString(), anyInt()))
        .thenAnswer(invocation -> dequeues.getAndIncrement() == 0 ? List.of(task) : List.of());
    doAnswer(
            invocation -> {
              renewals.incrementAndGet();
              return null;
            })
        .when(queueService)
        .renewLease(any(UUID.class), anyString());
    doAnswer(
            invocation -> {
              int count = invocation.getArgument(0);
              activeLeaseMonitors.set(count);
              if (count == 0) {
                activeMonitorsReset.countDown();
              }
              return null;
            })
        .when(metrics)
        .setActiveLeaseMonitors(anyInt());
    doAnswer(
            invocation -> {
              handlerStarted.countDown();
              releaseHandler.await();
              handlerCompleted.set(true);
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
    Thread stopThread = new Thread(runtime::stop, "queue-runtime-stop-test");

    runtime.start();
    try {
      assertTrue(handlerStarted.await(2, TimeUnit.SECONDS));
      assertTrue(waitUntil(() -> activeLeaseMonitors.get() == 1, Duration.ofSeconds(1)));
      assertTrue(waitUntil(() -> renewals.get() > 0, Duration.ofSeconds(1)));

      stopThread.start();

      assertTrue(activeMonitorsReset.await(250, TimeUnit.MILLISECONDS));
      assertFalse(handlerCompleted.get());
      int renewalsAfterSchedulerStop = renewals.get();
      Thread.sleep(150);
      assertEquals(renewalsAfterSchedulerStop, renewals.get());
    } finally {
      releaseHandler.countDown();
      assertTrue(waitUntil(() -> !stopThread.isAlive(), Duration.ofSeconds(2)));
      runtime.stop();
    }
    verify(shutdownStrategy, never()).shutdown(anyInt(), anyString(), any());
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
}
