package dev.verkhovskiy.taskqueue.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Представление задачи, извлеченной из очереди для обработки.
 *
 * @param taskId уникальный идентификатор задачи
 * @param taskType тип задачи
 * @param payload полезная нагрузка задачи
 * @param partitionKey ключ партиционирования
 * @param partitionNum номер партиции задачи
 * @param availableAt время, начиная с которого задача доступна к обработке
 * @param delayCount количество выполненных задержек/retry
 * @param createdAt время создания задачи
 */
public record QueuedTask(
    UUID taskId,
    String taskType,
    String payload,
    String partitionKey,
    int partitionNum,
    Instant availableAt,
    long delayCount,
    Instant createdAt) {}
