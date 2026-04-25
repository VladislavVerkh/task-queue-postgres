package dev.verkhovskiy.taskqueue.exception;

/** Ошибка несовместимого изменения количества партиций очереди. */
public class PartitionCountMismatchException extends IllegalStateException {

  /**
   * Создает ошибку несовпадения сохраненного и настроенного partition-count.
   *
   * @param storedPartitionCount значение, сохраненное в metadata очереди
   * @param configuredPartitionCount значение из текущей конфигурации
   */
  public PartitionCountMismatchException(
      String storedPartitionCount, String configuredPartitionCount) {
    super(
        "task.queue.partition-count cannot be changed without a migration: stored="
            + storedPartitionCount
            + ", configured="
            + configuredPartitionCount);
  }
}
