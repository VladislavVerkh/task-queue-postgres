package dev.verkhovskiy.taskqueue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskDeadLetterServiceTest {

  @Mock private TaskQueueRepository queueRepository;
  @Mock private TaskQueueMetrics metrics;

  private TaskQueueProperties properties;
  private TaskDeadLetterService service;

  @BeforeEach
  void setUp() {
    properties = new TaskQueueProperties();
    service = new TaskDeadLetterService(queueRepository, properties, metrics);
  }

  @Test
  void requeueMovesDeadLetterToQueue() {
    UUID taskId = UUID.randomUUID();
    when(queueRepository.requeueDeadLetter(taskId, null)).thenReturn(true);

    assertTrue(service.requeue(taskId));

    verify(metrics).deadLetterRequeued();
  }

  @Test
  void requeueDoesNotIncrementMetricWhenTaskIsMissing() {
    UUID taskId = UUID.randomUUID();
    when(queueRepository.requeueDeadLetter(taskId, null)).thenReturn(false);

    assertFalse(service.requeue(taskId));

    verify(metrics, never()).deadLetterRequeued();
  }

  @Test
  void deleteExpiredUsesRetentionAndBatchSize() {
    properties.setDeadLetterRetention(Duration.ofDays(7));
    properties.setCleanupBatchSize(100);
    when(queueRepository.deleteDeadLettersOlderThan(Duration.ofDays(7), 100)).thenReturn(5);

    assertEquals(5, service.deleteExpired());

    verify(metrics).deadLetterDeleted(5);
  }

  @Test
  void deleteExpiredIsDisabledWhenRetentionIsZero() {
    assertEquals(0, service.deleteExpired());

    verify(queueRepository, never()).deleteDeadLettersOlderThan(Duration.ZERO, 32);
  }
}
