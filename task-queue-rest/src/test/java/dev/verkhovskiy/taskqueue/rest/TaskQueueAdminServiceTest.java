package dev.verkhovskiy.taskqueue.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.persistence.TaskDeadLetterRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetricsRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetricsRepository.PartitionLagMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetricsRepository.QueueStateMetrics;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository.HandoffState;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository.PartitionAssignment;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.RequeueDeadLetterRequest;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.SummaryResponse;
import dev.verkhovskiy.taskqueue.service.TaskDeadLetterService;
import dev.verkhovskiy.taskqueue.service.WorkerCoordinationService;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskQueueAdminServiceTest {

  private static final Instant NOW = Instant.parse("2026-04-26T12:00:00Z");

  @Mock private TaskQueueMetricsRepository metricsRepository;
  @Mock private TaskDeadLetterRepository deadLetterRepository;
  @Mock private WorkerRegistryRepository workerRegistryRepository;
  @Mock private WorkerCoordinationService workerCoordinationService;
  @Mock private TaskDeadLetterService deadLetterService;

  private TaskQueueProperties properties;
  private TaskQueueAdminService service;

  @BeforeEach
  void setUp() {
    properties = new TaskQueueProperties();
    properties.setPartitionCount(2);
    service =
        new TaskQueueAdminService(
            metricsRepository,
            deadLetterRepository,
            workerRegistryRepository,
            workerCoordinationService,
            deadLetterService,
            properties);
  }

  @Test
  void summaryCombinesQueueDeadLetterAndDrainingSnapshots() {
    when(metricsRepository.loadQueueStateMetrics()).thenReturn(new QueueStateMetrics(10, 2, 30, 5));
    when(deadLetterRepository.countDeadLetters()).thenReturn(4L);
    when(workerRegistryRepository.countDrainingAssignments()).thenReturn(1);

    SummaryResponse summary = service.summary();

    assertThat(summary).isEqualTo(new SummaryResponse(10, 2, 30, 5, 4, 1));
  }

  @Test
  void partitionsReturnAllConfiguredPartitionsWithSparseLagDefaults() {
    when(workerRegistryRepository.findPartitionAssignments(2))
        .thenReturn(
            Map.of(
                1,
                new PartitionAssignment(
                    1,
                    "worker-1",
                    HandoffState.DRAINING,
                    "worker-2",
                    NOW.minusSeconds(10),
                    NOW.plusSeconds(20),
                    false,
                    Duration.ofSeconds(10))));
    when(metricsRepository.loadPartitionLagMetrics(2))
        .thenReturn(List.of(new PartitionLagMetrics(1, 3, 30)));

    var partitions = service.partitions();

    assertThat(partitions).hasSize(2);
    assertThat(partitions.get(0).workerId()).isEqualTo("worker-1");
    assertThat(partitions.get(0).handoffState()).isEqualTo("DRAINING");
    assertThat(partitions.get(0).readyTasks()).isEqualTo(3);
    assertThat(partitions.get(1).workerId()).isNull();
    assertThat(partitions.get(1).readyTasks()).isZero();
    assertThat(partitions.get(1).oldestReadyAgeSeconds()).isZero();
  }

  @Test
  void requeueRejectsAmbiguousDelayRequestBeforeServiceCall() {
    UUID taskId = UUID.randomUUID();
    RequeueDeadLetterRequest request = new RequeueDeadLetterRequest(NOW, Duration.ofSeconds(10));

    assertThatThrownBy(() -> service.requeueDeadLetter(taskId, request))
        .isInstanceOf(TaskQueueRestException.class)
        .hasMessage("availableAt and availableAfter cannot both be set");

    verify(deadLetterService, never()).requeue(taskId, NOW);
    verify(deadLetterService, never()).requeueDelayed(taskId, Duration.ofSeconds(10));
  }

  @Test
  void requeueDelayedReturnsNotFoundWhenDeadLetterIsMissing() {
    UUID taskId = UUID.randomUUID();
    Duration delay = Duration.ofSeconds(10);
    when(deadLetterService.requeueDelayed(taskId, delay)).thenReturn(false);

    assertThatThrownBy(
            () -> service.requeueDeadLetter(taskId, new RequeueDeadLetterRequest(null, delay)))
        .isInstanceOf(TaskQueueRestException.class)
        .hasMessage("Dead-letter task not found: " + taskId);
  }
}
