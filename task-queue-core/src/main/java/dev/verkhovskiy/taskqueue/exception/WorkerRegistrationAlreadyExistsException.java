package dev.verkhovskiy.taskqueue.exception;

/** Исключение выбрасывается при попытке зарегистрировать уже существующий воркер. */
public class WorkerRegistrationAlreadyExistsException extends RuntimeException {

  /**
   * Создает исключение для конфликтующего идентификатора воркера.
   *
   * @param workerId идентификатор уже зарегистрированного воркера
   */
  public WorkerRegistrationAlreadyExistsException(String workerId) {
    super("Queue worker registration already exists: " + workerId);
  }
}
