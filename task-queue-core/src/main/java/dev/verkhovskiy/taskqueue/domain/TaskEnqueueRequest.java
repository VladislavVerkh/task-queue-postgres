package dev.verkhovskiy.taskqueue.domain;

import java.time.Instant;

/**
 * Запрос на постановку задачи в очередь.
 *
 * @param taskType тип задачи
 * @param partitionKey ключ партиционирования для последовательной обработки связанных задач
 * @param payload полезная нагрузка задачи
 * @param availableAt время, начиная с которого задача доступна к обработке
 */
public record TaskEnqueueRequest(
    String taskType, String partitionKey, String payload, Instant availableAt) {}
