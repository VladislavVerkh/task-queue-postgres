package dev.verkhovskiy.taskqueue.exception;

import java.util.UUID;

/** Исключение выбрасывается, когда ожидаемая задача отсутствует в очереди. */
public class TaskNotFoundException extends RuntimeException {

  /**
   * Создает исключение для конкретного идентификатора задачи.
   *
   * @param taskId идентификатор отсутствующей задачи
   */
  public TaskNotFoundException(UUID taskId) {
    super("Task not found in queue: " + taskId);
  }
}
