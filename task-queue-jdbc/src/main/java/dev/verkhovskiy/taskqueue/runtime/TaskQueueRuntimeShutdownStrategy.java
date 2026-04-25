package dev.verkhovskiy.taskqueue.runtime;

/**
 * Стратегия остановки приложения при фатальной ошибке runtime очереди.
 *
 * <p>Позволяет приложению заменить дефолтное поведение библиотеки и выполнить собственный graceful
 * shutdown, отправку события в orchestrator или тестовую фиксацию аварийного сценария.
 */
@FunctionalInterface
public interface TaskQueueRuntimeShutdownStrategy {

  /**
   * Запрашивает остановку приложения.
   *
   * @param exitCode рекомендуемый код завершения процесса
   * @param message причина остановки
   * @param cause исходная ошибка
   */
  void shutdown(int exitCode, String message, Throwable cause);
}
