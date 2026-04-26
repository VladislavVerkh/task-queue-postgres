package dev.verkhovskiy.taskqueue.service;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/** Публичный API продюсера для постановки задач в очередь. */
public interface TaskProducer {

  /**
   * Добавляет задачу в очередь с моментом доступности "сейчас".
   *
   * @param taskType тип задачи
   * @param partitionKey ключ партиционирования
   * @param payload полезная нагрузка
   * @return идентификатор созданной задачи
   */
  UUID enqueue(String taskType, String partitionKey, String payload);

  /**
   * Добавляет задачу в очередь с задержкой относительно времени PostgreSQL.
   *
   * @param taskType тип задачи
   * @param partitionKey ключ партиционирования
   * @param payload полезная нагрузка
   * @param delay задержка до доступности задачи
   * @return идентификатор созданной задачи
   */
  default UUID enqueueDelayed(
      String taskType, String partitionKey, String payload, Duration delay) {
    throw new UnsupportedOperationException("Delayed enqueue is not supported");
  }

  /**
   * Добавляет задачу в очередь с указанным временем доступности.
   *
   * @param taskType тип задачи
   * @param partitionKey ключ партиционирования
   * @param payload полезная нагрузка
   * @param availableAt время, когда задача станет доступна для обработки
   * @return идентификатор созданной задачи
   */
  UUID enqueue(String taskType, String partitionKey, String payload, Instant availableAt);
}
