package dev.verkhovskiy.taskqueue.service;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Сервис обслуживания dead-letter задач. */
@Service
@RequiredArgsConstructor
public class TaskDeadLetterService {

  private final TaskQueueRepository queueRepository;
  private final TaskQueueProperties properties;
  private final TaskQueueMetrics metrics;
  private final Clock clock;

  /**
   * Возвращает dead-letter задачу в основную очередь с доступностью "сейчас".
   *
   * @param taskId идентификатор dead-letter задачи
   * @return {@code true}, если задача перенесена
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public boolean requeue(UUID taskId) {
    return requeue(taskId, clock.instant());
  }

  /**
   * Возвращает dead-letter задачу в основную очередь с заданным временем доступности.
   *
   * @param taskId идентификатор dead-letter задачи
   * @param availableAt время, когда задача станет доступна для обработки
   * @return {@code true}, если задача перенесена
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public boolean requeue(UUID taskId, Instant availableAt) {
    boolean requeued = queueRepository.requeueDeadLetter(taskId, availableAt);
    if (requeued) {
      metrics.deadLetterRequeued();
    }
    return requeued;
  }

  /**
   * Удаляет dead-letter записи старше настроенного retention.
   *
   * @return количество удаленных записей
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public int deleteExpired() {
    Duration retention = properties.getDeadLetterRetention();
    if (retention == null || retention.isZero() || retention.isNegative()) {
      return 0;
    }
    return deleteOlderThan(clock.instant().minus(retention), properties.getCleanupBatchSize());
  }

  /**
   * Удаляет dead-letter записи старше указанного момента.
   *
   * @param cutoff удаляются записи старше этого момента
   * @param limit максимальное количество удаляемых записей
   * @return количество удаленных записей
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public int deleteOlderThan(Instant cutoff, int limit) {
    int deleted = queueRepository.deleteDeadLettersOlderThan(cutoff, limit);
    metrics.deadLetterDeleted(deleted);
    return deleted;
  }
}
