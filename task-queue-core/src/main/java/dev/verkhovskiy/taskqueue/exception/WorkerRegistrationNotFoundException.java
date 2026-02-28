package dev.verkhovskiy.taskqueue.exception;

/** Исключение выбрасывается, когда операция выполняется для несуществующего воркера. */
public class WorkerRegistrationNotFoundException extends RuntimeException {

  /**
   * Создает исключение для отсутствующего воркера.
   *
   * @param workerId идентификатор воркера, который не найден
   */
  public WorkerRegistrationNotFoundException(String workerId) {
    super("Queue worker registration not found: " + workerId);
  }
}
