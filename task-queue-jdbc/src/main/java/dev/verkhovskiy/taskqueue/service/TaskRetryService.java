package dev.verkhovskiy.taskqueue.service;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.retry.RetryBackoffDecision;
import dev.verkhovskiy.taskqueue.retry.RetryBackoffPolicy;
import dev.verkhovskiy.taskqueue.retry.RetryExceptionClassifier;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Сервис retry-логики задач: классификация ошибок, backoff и финализация. */
@Service
@RequiredArgsConstructor
public class TaskRetryService {

  private final TaskQueueRepository queueRepository;
  private final RetryBackoffPolicy retryBackoffPolicy;
  private final RetryExceptionClassifier retryExceptionClassifier;
  private final Clock clock;

  /**
   * Выполняет retry-or-finalize логику на основе фактической ошибки обработки.
   *
   * @param taskId идентификатор задачи
   * @param alreadyRetriedCount количество уже выполненных retry
   * @param failure ошибка обработки
   * @return решение о следующем шаге обработки
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public RetryBackoffDecision retryOrFinalize(
      UUID taskId, long alreadyRetriedCount, Throwable failure) {
    if (!retryExceptionClassifier.isRetryable(failure)) {
      queueRepository.remove(taskId);
      return RetryBackoffDecision.nonRetryable(nextAttempt(alreadyRetriedCount));
    }
    return retryRetryableTask(taskId, alreadyRetriedCount);
  }

  /**
   * Выполняет retry-or-finalize без классификации ошибки.
   *
   * <p>Метод полезен для вызовов, где retry заведомо разрешен.
   *
   * @param taskId идентификатор задачи
   * @param alreadyRetriedCount количество уже выполненных retry
   * @return решение о следующем шаге обработки
   */
  @SuppressWarnings("unused")
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public RetryBackoffDecision retryOrFinalize(UUID taskId, long alreadyRetriedCount) {
    return retryRetryableTask(taskId, alreadyRetriedCount);
  }

  private RetryBackoffDecision retryRetryableTask(UUID taskId, long alreadyRetriedCount) {
    RetryBackoffDecision decision = retryBackoffPolicy.nextRetry(alreadyRetriedCount);
    if (decision.shouldRetry()) {
      Instant availableAt = clock.instant().plusMillis(decision.delayMillis());
      queueRepository.delay(taskId, availableAt);
      return decision;
    }

    queueRepository.remove(taskId);
    return decision;
  }

  /**
   * Вычисляет номер следующей попытки с насыщением к диапазону {@code int}.
   *
   * @param alreadyRetriedCount количество уже выполненных retry
   * @return номер следующей попытки
   */
  private static int nextAttempt(long alreadyRetriedCount) {
    long value = alreadyRetriedCount == Long.MAX_VALUE ? Long.MAX_VALUE : alreadyRetriedCount + 1;
    return value > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
  }
}
