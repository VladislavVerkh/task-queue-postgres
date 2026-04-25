package dev.verkhovskiy.taskqueue.exception;

import java.util.UUID;

/** Исключение выбрасывается, когда задача больше не закреплена за ожидаемым воркером. */
public class TaskOwnershipLostException extends RuntimeException {

  /**
   * Создает исключение для задачи и воркера, который потерял право владения задачей.
   *
   * @param taskId идентификатор задачи
   * @param workerId идентификатор воркера
   */
  public TaskOwnershipLostException(UUID taskId, String workerId) {
    super("Task " + taskId + " is not owned by worker " + workerId);
  }
}
