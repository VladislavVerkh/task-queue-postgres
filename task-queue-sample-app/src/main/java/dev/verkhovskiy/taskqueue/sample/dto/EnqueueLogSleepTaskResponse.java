package dev.verkhovskiy.taskqueue.sample.dto;

import java.util.UUID;

/**
 * DTO ответа на постановку демонстрационной задачи.
 *
 * @param taskId идентификатор созданной задачи
 */
public record EnqueueLogSleepTaskResponse(UUID taskId) {}
