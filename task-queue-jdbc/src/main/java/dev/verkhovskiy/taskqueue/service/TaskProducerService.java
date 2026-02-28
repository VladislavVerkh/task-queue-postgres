package dev.verkhovskiy.taskqueue.service;

import dev.verkhovskiy.taskqueue.domain.TaskEnqueueRequest;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/** Упрощенный API продюсера для постановки задач в очередь. */
@Service
@RequiredArgsConstructor
public class TaskProducerService implements TaskProducer {

  private final TaskQueueService queueService;
  private final Clock clock;

  /**
   * Добавляет задачу в очередь с моментом доступности "сейчас".
   *
   * @param taskType тип задачи
   * @param partitionKey ключ партиционирования
   * @param payload полезная нагрузка
   * @return идентификатор созданной задачи
   */
  @SuppressWarnings("unused")
  @Override
  public UUID enqueue(String taskType, String partitionKey, String payload) {
    return queueService.enqueue(
        new TaskEnqueueRequest(taskType, partitionKey, payload, clock.instant()));
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
  @Override
  public UUID enqueue(String taskType, String partitionKey, String payload, Instant availableAt) {
    return queueService.enqueue(
        new TaskEnqueueRequest(taskType, partitionKey, payload, availableAt));
  }
}
