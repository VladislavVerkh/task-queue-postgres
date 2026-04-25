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
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskDeadLetterServiceTest {

  private static final Instant NOW = Instant.parse("2026-01-01T10:00:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

  @Mock private TaskQueueRepository queueRepository;
  @Mock private TaskQueueMetrics metrics;

  private TaskQueueProperties properties;
  private TaskDeadLetterService service;

  @BeforeEach
  void setUp() {
    properties = new TaskQueueProperties();
    service = new TaskDeadLetterService(queueRepository, properties, metrics, CLOCK);
  }

  @Test
  void requeueMovesDeadLetterToQueue() {
    UUID taskId = UUID.randomUUID();
    when(queueRepository.requeueDeadLetter(taskId, NOW)).thenReturn(true);

    assertTrue(service.requeue(taskId));

    verify(metrics).deadLetterRequeued();
  }

  @Test
  void requeueDoesNotIncrementMetricWhenTaskIsMissing() {
    UUID taskId = UUID.randomUUID();
    when(queueRepository.requeueDeadLetter(taskId, NOW)).thenReturn(false);

    assertFalse(service.requeue(taskId));

    verify(metrics, never()).deadLetterRequeued();
  }

  @Test
  void deleteExpiredUsesRetentionAndBatchSize() {
    properties.setDeadLetterRetention(Duration.ofDays(7));
    properties.setCleanupBatchSize(100);
    when(queueRepository.deleteDeadLettersOlderThan(NOW.minus(Duration.ofDays(7)), 100))
        .thenReturn(5);

    assertEquals(5, service.deleteExpired());

    verify(metrics).deadLetterDeleted(5);
  }

  @Test
  void deleteExpiredIsDisabledWhenRetentionIsZero() {
    assertEquals(0, service.deleteExpired());

    verify(queueRepository, never()).deleteDeadLettersOlderThan(NOW, 32);
  }
}
