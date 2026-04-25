package dev.verkhovskiy.taskqueue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.retry.RetryBackoffDecision;
import dev.verkhovskiy.taskqueue.retry.RetryBackoffPolicy;
import dev.verkhovskiy.taskqueue.retry.RetryExceptionClassifier;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
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
  @Mock private RetryBackoffPolicy retryBackoffPolicy;
  @Mock private RetryExceptionClassifier retryExceptionClassifier;

  private TaskQueueProperties properties;
  private Clock clock;
  private TaskRetryService service;

  @BeforeEach
  void setUp() {
    properties = new TaskQueueProperties();
    clock = Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);
    service =
        new TaskRetryService(
            queueRepository, retryBackoffPolicy, retryExceptionClassifier, properties, clock);
  }

  @Test
  void retryOrFinalizeWithWorkerIdDelaysOnlyOwnedTaskWhenRetryIsAllowed() {
    UUID taskId = UUID.randomUUID();
    RetryBackoffDecision decision = RetryBackoffDecision.retryAfter(2, 5_000);
    when(retryBackoffPolicy.nextRetry(1)).thenReturn(decision);
    when(retryExceptionClassifier.isRetryable(org.mockito.ArgumentMatchers.any())).thenReturn(true);

    RetryBackoffDecision actual =
        service.retryOrFinalize(taskId, 1, new RuntimeException("boom"), "worker-1");

    assertEquals(decision, actual);
    verify(queueRepository).delayOwnedBy(taskId, "worker-1", Instant.parse("2026-01-01T10:00:05Z"));
  }

  @Test
  void retryOrFinalizeWithWorkerIdRemovesOnlyOwnedTaskWhenErrorIsNonRetryable() {
    UUID taskId = UUID.randomUUID();
    when(retryExceptionClassifier.isRetryable(org.mockito.ArgumentMatchers.any()))
        .thenReturn(false);

    RetryBackoffDecision actual =
        service.retryOrFinalize(taskId, 3, new IllegalArgumentException("bad"), "worker-1");

    assertEquals(RetryBackoffDecision.nonRetryable(4), actual);
    verify(queueRepository).removeOwnedBy(taskId, "worker-1");
    verify(retryBackoffPolicy, never()).nextRetry(anyLong());
  }

  @Test
  void retryOrFinalizeStoresNonRetryableTaskInDeadLetterWhenEnabled() {
    properties.setDeadLetterEnabled(true);
    UUID taskId = UUID.randomUUID();
    IllegalArgumentException failure = new IllegalArgumentException("bad payload");
    when(retryExceptionClassifier.isRetryable(org.mockito.ArgumentMatchers.any()))
        .thenReturn(false);

    RetryBackoffDecision actual = service.retryOrFinalize(taskId, 0, failure, "worker-1");

    assertEquals(RetryBackoffDecision.nonRetryable(1), actual);
    verify(queueRepository)
        .deadLetterOwnedBy(
            taskId,
            "worker-1",
            "NON_RETRYABLE",
            IllegalArgumentException.class.getName(),
            "bad payload",
            clock.instant());
    verify(queueRepository, never()).removeOwnedBy(taskId, "worker-1");
  }

  @Test
  void retryOrFinalizeStoresRetryExhaustedTaskInDeadLetterWhenEnabled() {
    properties.setDeadLetterEnabled(true);
    UUID taskId = UUID.randomUUID();
    RetryBackoffDecision decision = RetryBackoffDecision.noRetry(4);
    when(retryBackoffPolicy.nextRetry(3)).thenReturn(decision);

    RetryBackoffDecision actual = service.retryOrFinalize(taskId, 3, "worker-1");

    assertEquals(decision, actual);
    verify(queueRepository)
        .deadLetterOwnedBy(taskId, "worker-1", "RETRY_EXHAUSTED", null, null, clock.instant());
    verify(queueRepository, never()).removeOwnedBy(taskId, "worker-1");
  }
}
