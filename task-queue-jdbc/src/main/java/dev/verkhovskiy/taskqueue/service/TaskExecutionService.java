package dev.verkhovskiy.taskqueue.service;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.handler.TaskHandler;
import dev.verkhovskiy.taskqueue.handler.TaskHandlerRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Транзакционный фасад обработки задачи.
 *
 * <p>Гарантирует, что бизнес-обработка задачи и подтверждение {@code ack} выполняются в одной
 * транзакции.
 */
@Service
@RequiredArgsConstructor
public class TaskExecutionService {

  private final TaskHandlerRegistry handlerRegistry;
  private final TaskQueueService taskQueueService;

  /**
   * Выполняет обработчик задачи и подтверждает выполнение, проверяя текущего владельца задачи.
   *
   * <p>При любой ошибке (включая checked-исключения) транзакция откатывается.
   *
   * @param task задача из очереди
   * @param workerId идентификатор воркера-владельца
   * @throws Exception ошибка бизнес-обработки
   */
  @Transactional(
      transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER,
      rollbackFor = Exception.class)
  public void handleAndAcknowledge(QueuedTask task, String workerId) throws Exception {
    handle(task);
    taskQueueService.acknowledge(task.taskId(), workerId);
  }

  private void handle(QueuedTask task) throws Exception {
    TaskHandler handler =
        handlerRegistry
            .findByType(task.taskType())
            .orElseThrow(
                () -> new IllegalStateException("No task handler for type " + task.taskType()));
    handler.handle(task);
  }
}
