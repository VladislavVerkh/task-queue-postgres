package dev.verkhovskiy.taskqueue.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskDeadLetterRepository;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskDeadLetterServiceTest {

  @Mock private TaskDeadLetterRepository deadLetterRepository;
  @Mock private TaskQueueMetrics metrics;

  private TaskQueueProperties properties;
  private TaskDeadLetterService service;

  @BeforeEach
  void setUp() {
    properties = new TaskQueueProperties();
    service = new TaskDeadLetterService(deadLetterRepository, properties, metrics);
  }

  @Test
  void requeueMovesDeadLetterToQueue() {
    UUID taskId = UUID.randomUUID();
    when(deadLetterRepository.requeueDeadLetter(taskId, null)).thenReturn(true);

    assertTrue(service.requeue(taskId));

    verify(metrics).deadLetterRequeued();
  }

  @Test
  void requeueDoesNotIncrementMetricWhenTaskIsMissing() {
    UUID taskId = UUID.randomUUID();
    when(deadLetterRepository.requeueDeadLetter(taskId, null)).thenReturn(false);

    assertFalse(service.requeue(taskId));

    verify(metrics, never()).deadLetterRequeued();
  }

  @Test
  void requeueDelayedMovesDeadLetterToQueueWithDatabaseRelativeDelay() {
    UUID taskId = UUID.randomUUID();
    Duration delay = Duration.ofSeconds(30);
    when(deadLetterRepository.requeueDeadLetter(taskId, null, delay)).thenReturn(true);

    assertTrue(service.requeueDelayed(taskId, delay));

    verify(deadLetterRepository).requeueDeadLetter(taskId, null, delay);
    verify(metrics).deadLetterRequeued();
  }

  @Test
  void requeueDelayedDoesNotIncrementMetricWhenTaskIsMissing() {
    UUID taskId = UUID.randomUUID();
    Duration delay = Duration.ofSeconds(30);
    when(deadLetterRepository.requeueDeadLetter(taskId, null, delay)).thenReturn(false);

    assertFalse(service.requeueDelayed(taskId, delay));

    verify(metrics, never()).deadLetterRequeued();
  }

  @Test
  void requeueDelayedRejectsNullDelayBeforeSql() {
    assertThatThrownBy(() -> service.requeueDelayed(UUID.randomUUID(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("delay must be set");

    verifyNoInteractions(deadLetterRepository, metrics);
  }

  @Test
  void requeueDelayedRejectsNegativeDelayBeforeSql() {
    assertThatThrownBy(() -> service.requeueDelayed(UUID.randomUUID(), Duration.ofMillis(-1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("delay must be greater than or equal to 0");

    verifyNoInteractions(deadLetterRepository, metrics);
  }

  @Test
  void deleteExpiredUsesRetentionAndBatchSize() {
    properties.setDeadLetterRetention(Duration.ofDays(7));
    properties.setCleanupBatchSize(100);
    when(deadLetterRepository.deleteDeadLettersOlderThan(Duration.ofDays(7), 100)).thenReturn(5);

    assertEquals(5, service.deleteExpired());

    verify(metrics).deadLetterDeleted(5);
  }

  @Test
  void deleteExpiredIsDisabledWhenRetentionIsZero() {
    assertEquals(0, service.deleteExpired());

    verify(deadLetterRepository, never()).deleteDeadLettersOlderThan(Duration.ZERO, 32);
  }
}
