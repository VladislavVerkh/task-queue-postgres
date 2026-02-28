package dev.verkhovskiy.taskqueue.sample.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Instant;

/**
 * DTO запроса постановки демонстрационной задачи в очередь.
 *
 * @param partitionKey ключ партиционирования
 * @param message сообщение для обработки
 * @param sleepMs длительность имитации обработки в миллисекундах
 * @param availableAt момент, когда задача станет доступна в очереди
 */
public record EnqueueLogSleepTaskRequest(
    @NotBlank String partitionKey,
    @NotBlank String message,
    @PositiveOrZero Long sleepMs,
    Instant availableAt) {}
