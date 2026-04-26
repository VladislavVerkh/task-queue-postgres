package dev.verkhovskiy.taskqueue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.HandoffTimeoutAction;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository.QueueStateMetrics;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WorkerCoordinationServiceTest {

  private static final Instant NOW = Instant.parse("2026-03-01T16:00:00Z");

  @Mock private WorkerRegistryRepository workerRegistryRepository;
  @Mock private TaskQueueRepository queueRepository;
  @Mock private PartitionAssignmentPlanner assignmentPlanner;
  @Mock private TaskQueueMetrics metrics;

  private TaskQueueProperties properties;
  private WorkerCoordinationService service;

  @BeforeEach
  void setUp() throws Exception {
    properties = new TaskQueueProperties();
    properties.setPartitionCount(1);
    properties.setHandoffDrainTimeout(Duration.ofSeconds(30));
    properties.setHandoffTimeoutAction(HandoffTimeoutAction.EXTEND);

    lenient()
        .doAnswer(
            invocation -> {
              TaskQueueMetrics.RebalanceCallable<?> callable = invocation.getArgument(0);
              callable.call();
              return null;
            })
        .when(metrics)
        .recordRebalance(any());

    service =
        new WorkerCoordinationService(
            workerRegistryRepository, queueRepository, assignmentPlanner, properties, metrics);
  }

  @Test
  void startsDrainingWhenOwnerMustChangeAndInFlightExists() {
    when(workerRegistryRepository.findAllWorkerIdsOrdered()).thenReturn(List.of("w1", "w2"));
    when(workerRegistryRepository.findPartitionAssignments(1))
        .thenReturn(
            Map.of(
                1,
                new WorkerRegistryRepository.PartitionAssignment(
                    1,
                    "w1",
                    WorkerRegistryRepository.HandoffState.ACTIVE,
                    null,
                    null,
                    null,
                    true,
                    Duration.ZERO)));
    when(assignmentPlanner.plan(List.of("w1", "w2"), 1, Map.of(1, "w1")))
        .thenReturn(Map.of(1, "w2"));
    when(queueRepository.hasInFlightTasks(1, "w1")).thenReturn(true);
    when(workerRegistryRepository.countDrainingAssignments()).thenReturn(1);

    service.rebalance();

    verify(workerRegistryRepository).startDraining(1, "w2", properties.getHandoffDrainTimeout());
    verify(workerRegistryRepository, never()).upsertActiveAssignment(1, "w2");
    verify(workerRegistryRepository, never()).completeHandoff(1, "w2");
    verify(metrics).handoffStarted();
    verify(metrics).setDrainingAssignments(1);
  }

  @Test
  void completesDrainingWhenInFlightFinished() {
    Instant startedAt = NOW.minusSeconds(5);
    Instant deadlineAt = NOW.plusSeconds(30);
    Duration drainDuration = Duration.ofSeconds(5);
    when(workerRegistryRepository.findAllWorkerIdsOrdered()).thenReturn(List.of("w1", "w2"));
    when(workerRegistryRepository.findPartitionAssignments(1))
        .thenReturn(
            Map.of(
                1,
                new WorkerRegistryRepository.PartitionAssignment(
                    1,
                    "w1",
                    WorkerRegistryRepository.HandoffState.DRAINING,
                    "w2",
                    startedAt,
                    deadlineAt,
                    false,
                    drainDuration)));
    when(assignmentPlanner.plan(List.of("w1", "w2"), 1, Map.of(1, "w1")))
        .thenReturn(Map.of(1, "w2"));
    when(queueRepository.hasInFlightTasks(1, "w1")).thenReturn(false);

    service.rebalance();

    verify(workerRegistryRepository).completeHandoff(1, "w2");
    verify(metrics).handoffCompleted(drainDuration);
  }

  @Test
  void extendsDeadlineOnDrainTimeoutWhenActionIsExtend() {
    Instant startedAt = NOW.minusSeconds(60);
    Instant deadlineAt = NOW.minusSeconds(1);
    Duration drainDuration = Duration.ofSeconds(60);
    when(workerRegistryRepository.findAllWorkerIdsOrdered()).thenReturn(List.of("w1", "w2"));
    when(workerRegistryRepository.findPartitionAssignments(1))
        .thenReturn(
            Map.of(
                1,
                new WorkerRegistryRepository.PartitionAssignment(
                    1,
                    "w1",
                    WorkerRegistryRepository.HandoffState.DRAINING,
                    "w2",
                    startedAt,
                    deadlineAt,
                    true,
                    drainDuration)));
    when(assignmentPlanner.plan(List.of("w1", "w2"), 1, Map.of(1, "w1")))
        .thenReturn(Map.of(1, "w2"));
    when(queueRepository.hasInFlightTasks(1, "w1")).thenReturn(true);

    service.rebalance();

    verify(metrics).handoffTimeout();
    verify(metrics).handoffTimeoutExtended();
    verify(workerRegistryRepository).extendDrainDeadline(1, properties.getHandoffDrainTimeout());
    verify(workerRegistryRepository, never()).cancelDraining(1);
    verify(workerRegistryRepository, never()).completeHandoff(1, "w2");
  }

  @Test
  void cleansUpExpiredTaskLeases() {
    when(queueRepository.releaseExpiredTaskLeases(properties.getCleanupBatchSize())).thenReturn(3);

    int released = service.cleanUpExpiredTaskLeases();

    assertEquals(3, released);
    verify(metrics).expiredTaskLeasesReleased(3);
  }

  @Test
  void refreshesQueueStateMetrics() {
    QueueStateMetrics snapshot = new QueueStateMetrics(10, 2, 30, 5);
    when(queueRepository.loadQueueStateMetrics()).thenReturn(snapshot);

    service.refreshQueueStateMetrics();

    verify(metrics).setQueueState(snapshot);
    verify(queueRepository, never()).loadPartitionLagMetrics(properties.getPartitionCount());
    verify(metrics, never()).setPartitionLag(any());
  }

  @Test
  void refreshesPartitionLagMetricsWhenEnabled() {
    QueueStateMetrics snapshot = new QueueStateMetrics(10, 2, 30, 5);
    properties.setPartitionLagMetricsEnabled(true);
    when(queueRepository.loadQueueStateMetrics()).thenReturn(snapshot);
    when(queueRepository.loadPartitionLagMetrics(properties.getPartitionCount()))
        .thenReturn(List.of());

    service.refreshQueueStateMetrics();

    verify(metrics).setQueueState(snapshot);
    verify(metrics).setPartitionLag(List.of());
  }
}
