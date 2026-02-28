package dev.verkhovskiy.taskqueue.handler;

import dev.verkhovskiy.taskqueue.domain.QueuedTask;

/** Контракт обработчика задач конкретного типа. */
public interface TaskHandler {

  /**
   * Возвращает тип задачи, который поддерживает обработчик.
   *
   * @return тип задачи
   */
  String taskType();

  /**
   * Выполняет обработку задачи.
   *
   * @param task задача из очереди
   * @throws Exception исключение в случае ошибки обработки
   */
  void handle(QueuedTask task) throws Exception;
}
