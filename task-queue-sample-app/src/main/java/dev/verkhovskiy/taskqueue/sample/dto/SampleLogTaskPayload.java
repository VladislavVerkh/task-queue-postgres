package dev.verkhovskiy.taskqueue.sample.dto;

/**
 * DTO полезной нагрузки демонстрационной задачи.
 *
 * @param message текст сообщения для логирования
 * @param sleepMs задержка обработки в миллисекундах
 */
public record SampleLogTaskPayload(String message, long sleepMs) {}
