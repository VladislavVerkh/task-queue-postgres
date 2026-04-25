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
   * Выполняет retry-or-finalize, проверяя что задача все еще закреплена за ожидаемым воркером.
   *
   * @param taskId идентификатор задачи
   * @param alreadyRetriedCount количество уже выполненных retry
   * @param failure ошибка обработки
   * @param workerId идентификатор воркера-владельца
   * @return решение о следующем шаге обработки
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public RetryBackoffDecision retryOrFinalize(
      UUID taskId, long alreadyRetriedCount, Throwable failure, String workerId) {
    requireWorkerId(workerId);
    if (!retryExceptionClassifier.isRetryable(failure)) {
      queueRepository.removeOwnedBy(taskId, workerId);
      return RetryBackoffDecision.nonRetryable(nextAttempt(alreadyRetriedCount));
    }
    return retryRetryableTask(taskId, alreadyRetriedCount, workerId);
  }

  /**
   * Выполняет retry-or-finalize без классификации ошибки, проверяя текущего владельца задачи.
   *
   * <p>Метод полезен для вызовов, где retry заведомо разрешен.
   *
   * @param taskId идентификатор задачи
   * @param alreadyRetriedCount количество уже выполненных retry
   * @param workerId идентификатор воркера-владельца
   * @return решение о следующем шаге обработки
   */
  @SuppressWarnings("unused")
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public RetryBackoffDecision retryOrFinalize(
      UUID taskId, long alreadyRetriedCount, String workerId) {
    requireWorkerId(workerId);
    return retryRetryableTask(taskId, alreadyRetriedCount, workerId);
  }

  private RetryBackoffDecision retryRetryableTask(
      UUID taskId, long alreadyRetriedCount, String workerId) {
    RetryBackoffDecision decision = retryBackoffPolicy.nextRetry(alreadyRetriedCount);
    if (decision.shouldRetry()) {
      Instant availableAt = clock.instant().plusMillis(decision.delayMillis());
      queueRepository.delayOwnedBy(taskId, workerId, availableAt);
      return decision;
    }

    queueRepository.removeOwnedBy(taskId, workerId);
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

  private static void requireWorkerId(String workerId) {
    if (workerId == null || workerId.isBlank()) {
      throw new IllegalArgumentException("workerId must be set");
    }
  }
}
