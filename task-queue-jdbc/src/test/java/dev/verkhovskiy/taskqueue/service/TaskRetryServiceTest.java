package dev.verkhovskiy.taskqueue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

  private TaskRetryService service;

  @BeforeEach
  void setUp() {
    Clock clock = Clock.fixed(Instant.parse("2026-01-01T10:00:00Z"), ZoneOffset.UTC);
    service =
        new TaskRetryService(queueRepository, retryBackoffPolicy, retryExceptionClassifier, clock);
  }

  @Test
  void retryOrFinalizeDelaysTaskWhenRetryIsAllowed() {
    UUID taskId = UUID.randomUUID();
    RetryBackoffDecision decision = RetryBackoffDecision.retryAfter(2, 5_000);
    when(retryBackoffPolicy.nextRetry(1)).thenReturn(decision);
    when(retryExceptionClassifier.isRetryable(org.mockito.ArgumentMatchers.any())).thenReturn(true);

    RetryBackoffDecision actual = service.retryOrFinalize(taskId, 1, new RuntimeException("boom"));

    assertEquals(decision, actual);
    verify(queueRepository).delay(taskId, Instant.parse("2026-01-01T10:00:05Z"));
    verify(queueRepository, never()).remove(taskId);
  }

  @Test
  void retryOrFinalizeRemovesTaskWhenRetryLimitReached() {
    UUID taskId = UUID.randomUUID();
    RetryBackoffDecision decision = RetryBackoffDecision.noRetry(4);
    when(retryBackoffPolicy.nextRetry(3)).thenReturn(decision);
    when(retryExceptionClassifier.isRetryable(org.mockito.ArgumentMatchers.any())).thenReturn(true);

    RetryBackoffDecision actual = service.retryOrFinalize(taskId, 3, new RuntimeException("boom"));

    assertEquals(decision, actual);
    verify(queueRepository).remove(taskId);
    verify(queueRepository, never()).delay(eq(taskId), any(Instant.class));
  }

  @Test
  void retryOrFinalizeRemovesTaskWithoutPolicyWhenErrorIsNonRetryable() {
    UUID taskId = UUID.randomUUID();
    when(retryExceptionClassifier.isRetryable(org.mockito.ArgumentMatchers.any()))
        .thenReturn(false);

    RetryBackoffDecision actual =
        service.retryOrFinalize(taskId, 3, new IllegalArgumentException("bad"));

    assertEquals(RetryBackoffDecision.nonRetryable(4), actual);
    verify(queueRepository).remove(taskId);
    verify(queueRepository, never()).delay(eq(taskId), any(Instant.class));
    verify(retryBackoffPolicy, never()).nextRetry(anyLong());
  }
}
