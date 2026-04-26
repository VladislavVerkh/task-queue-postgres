package dev.verkhovskiy.taskqueue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskDeadLetterRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.retry.RetryBackoffDecision;
import dev.verkhovskiy.taskqueue.retry.RetryBackoffPolicy;
import dev.verkhovskiy.taskqueue.retry.RetryExceptionClassifier;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Тесты сценариев retry/finalize в сервисе обработки ошибок задач. */
@ExtendWith(MockitoExtension.class)
class TaskRetryServiceTest {

  @Mock private TaskQueueRepository queueRepository;
  @Mock private TaskDeadLetterRepository deadLetterRepository;
  @Mock private RetryBackoffPolicy retryBackoffPolicy;
  @Mock private RetryExceptionClassifier retryExceptionClassifier;
  @Mock private TaskQueueMetrics metrics;

  private TaskQueueProperties properties;
  private TaskRetryService service;

  @BeforeEach
  void setUp() {
    properties = new TaskQueueProperties();
    service =
        new TaskRetryService(
            queueRepository,
            deadLetterRepository,
            retryBackoffPolicy,
            retryExceptionClassifier,
            properties,
            metrics);
  }

  @Test
  void retryOrFinalizeWithWorkerIdDelaysOnlyOwnedTaskWhenRetryIsAllowed() {
    UUID taskId = UUID.randomUUID();
    RetryBackoffDecision decision = RetryBackoffDecision.retryAfter(2, 5_000);
    when(retryBackoffPolicy.nextRetry(1)).thenReturn(decision);
    when(retryExceptionClassifier.isRetryable(any())).thenReturn(true);

    RetryBackoffDecision actual =
        service.retryOrFinalize(taskId, 1, new RuntimeException("boom"), "worker-1");

    assertEquals(decision, actual);
    verify(queueRepository).delayOwnedBy(taskId, "worker-1", 5_000);
    verify(metrics).retryScheduled();
  }

  @Test
  void retryOrFinalizeWithWorkerIdRemovesOnlyOwnedTaskWhenErrorIsNonRetryable() {
    UUID taskId = UUID.randomUUID();
    when(retryExceptionClassifier.isRetryable(any())).thenReturn(false);

    RetryBackoffDecision actual =
        service.retryOrFinalize(taskId, 3, new IllegalArgumentException("bad"), "worker-1");

    assertEquals(RetryBackoffDecision.nonRetryable(4), actual);
    verify(queueRepository).removeOwnedBy(taskId, "worker-1");
    verify(metrics).nonRetryable();
    verify(retryBackoffPolicy, never()).nextRetry(anyLong());
  }

  @Test
  void retryOrFinalizeStoresNonRetryableTaskInDeadLetterWhenEnabled() {
    properties.setDeadLetterEnabled(true);
    UUID taskId = UUID.randomUUID();
    IllegalArgumentException failure = new IllegalArgumentException("bad payload");
    when(retryExceptionClassifier.isRetryable(any())).thenReturn(false);

    RetryBackoffDecision actual = service.retryOrFinalize(taskId, 0, failure, "worker-1");

    assertEquals(RetryBackoffDecision.nonRetryable(1), actual);
    verify(deadLetterRepository)
        .deadLetterOwnedBy(
            taskId,
            "worker-1",
            "NON_RETRYABLE",
            IllegalArgumentException.class.getName(),
            "bad payload");
    verify(queueRepository, never()).removeOwnedBy(taskId, "worker-1");
    verify(metrics).nonRetryable();
    verify(metrics).deadLettered();
  }

  @Test
  void retryOrFinalizeStoresRetryExhaustedTaskInDeadLetterWhenEnabled() {
    properties.setDeadLetterEnabled(true);
    UUID taskId = UUID.randomUUID();
    RetryBackoffDecision decision = RetryBackoffDecision.noRetry(4);
    when(retryBackoffPolicy.nextRetry(3)).thenReturn(decision);

    RetryBackoffDecision actual = service.retryOrFinalize(taskId, 3, "worker-1");

    assertEquals(decision, actual);
    verify(deadLetterRepository)
        .deadLetterOwnedBy(taskId, "worker-1", "RETRY_EXHAUSTED", null, null);
    verify(queueRepository, never()).removeOwnedBy(taskId, "worker-1");
    verify(metrics).retryExhausted();
    verify(metrics).deadLettered();
  }
}
