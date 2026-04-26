package dev.verkhovskiy.taskqueue.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import dev.verkhovskiy.taskqueue.exception.TaskOwnershipLostException;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.service.TaskQueueService;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskLeaseRenewalSchedulerTest {

  @Mock private ScheduledExecutorService executor;
  @Mock private TaskQueueService queueService;
  @Mock private TaskQueueMetrics metrics;
  @Mock private TaskLeaseRenewalScheduler.LeaseRenewalFailureHandler failureHandler;

  private final AtomicBoolean running = new AtomicBoolean(true);
  private TaskLeaseRenewalScheduler scheduler;

  @BeforeEach
  void setUp() {
    scheduler =
        new TaskLeaseRenewalScheduler(
            executor, Duration.ofMillis(10), running::get, queueService, metrics, failureHandler);
  }

  @Test
  void renewsRemainingMonitorsWhenOneTaskLosesOwnership() {
    UUID lostTaskId = UUID.randomUUID();
    UUID healthyTaskId = UUID.randomUUID();
    Thread lostProcessingThread = new Thread();
    AtomicInteger healthyRenewals = new AtomicInteger();
    scheduler.register("worker-1", lostTaskId, lostProcessingThread);
    scheduler.register("worker-1", healthyTaskId, new Thread());
    doAnswer(
            invocation -> {
              UUID taskId = invocation.getArgument(0);
              String workerId = invocation.getArgument(1);
              if (lostTaskId.equals(taskId)) {
                throw new TaskOwnershipLostException(taskId, workerId);
              }
              if (healthyTaskId.equals(taskId)) {
                healthyRenewals.incrementAndGet();
              }
              return null;
            })
        .when(queueService)
        .renewLease(any(UUID.class), anyString());

    scheduler.renewActiveLeases();
    scheduler.renewActiveLeases();

    assertTrue(lostProcessingThread.isInterrupted());
    assertEquals(2, healthyRenewals.get());
    verify(metrics).leaseRenewalError();
    verify(failureHandler, never()).onFailure(any(UUID.class), any(Throwable.class));
  }

  @Test
  void renewalRuntimeFailureDelegatesFatalFailureAndStopsTick() {
    UUID taskId = UUID.randomUUID();
    RuntimeException failure = new IllegalStateException("db is down");
    scheduler.register("worker-1", taskId, new Thread());
    doThrow(failure).when(queueService).renewLease(taskId, "worker-1");

    RuntimeException actual = assertThrows(RuntimeException.class, scheduler::renewActiveLeases);

    assertSame(failure, actual);
    verify(metrics).leaseRenewalError();
    verify(metrics, atLeastOnce()).setActiveLeaseMonitors(0);
    verify(failureHandler).onFailure(taskId, failure);
  }

  @Test
  void stopCancelsMonitorsAndPreventsRenewal() {
    UUID taskId = UUID.randomUUID();
    scheduler.register("worker-1", taskId, new Thread());

    scheduler.stop();
    scheduler.renewActiveLeases();

    verify(metrics, atLeastOnce()).setActiveLeaseMonitors(0);
    verify(queueService, never()).renewLease(any(UUID.class), anyString());
  }
}
