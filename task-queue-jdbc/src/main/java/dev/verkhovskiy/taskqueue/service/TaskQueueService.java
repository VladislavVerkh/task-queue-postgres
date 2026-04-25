package dev.verkhovskiy.taskqueue.service;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.domain.TaskEnqueueRequest;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Сервис базовых операций очереди: постановка, выборка и подтверждение задач. */
@Service
@RequiredArgsConstructor
public class TaskQueueService {

  private final TaskQueueRepository queueRepository;
  private final WorkerRegistryRepository workerRegistryRepository;
  private final TaskPartitioner partitioner;
  private final TaskQueueProperties properties;
  private final Clock clock;
  private final TaskIdGenerator taskIdGenerator;

  /**
   * Ставит задачу в очередь.
   *
   * @param request данные задачи
   * @return идентификатор созданной задачи
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public UUID enqueue(TaskEnqueueRequest request) {
    if (request.taskType() == null || request.taskType().isBlank()) {
      throw new IllegalArgumentException("taskType must be set");
    }
    if (request.payload() == null) {
      throw new IllegalArgumentException("payload must be set");
    }

    Instant now = clock.instant();
    UUID taskId = taskIdGenerator.next();
    int partitionNum =
        partitioner.partition(request.partitionKey(), properties.getPartitionCount());
    Instant availableAt = request.availableAt() == null ? now : request.availableAt();
    queueRepository.enqueue(
        taskId,
        request.taskType(),
        request.payload(),
        request.partitionKey(),
        partitionNum,
        availableAt,
        now);
    return taskId;
  }

  /**
   * Выбирает и закрепляет за воркером следующую пачку задач.
   *
   * @param workerId идентификатор воркера
   * @param maxCount максимальный размер пачки
   * @return список задач для обработки
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public List<QueuedTask> dequeueForWorker(String workerId, int maxCount) {
    workerRegistryRepository.lockShared(properties.getRebalanceLockKey());
    return queueRepository.lockNextTasksForWorker(workerId, maxCount, clock.instant());
  }

  /**
   * Подтверждает успешную обработку задачи только если задача все еще закреплена за воркером.
   *
   * @param taskId идентификатор задачи
   * @param workerId идентификатор воркера-владельца
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public void acknowledge(UUID taskId, String workerId) {
    requireWorkerId(workerId);
    queueRepository.removeOwnedBy(taskId, workerId);
  }

  private static void requireWorkerId(String workerId) {
    if (workerId == null || workerId.isBlank()) {
      throw new IllegalArgumentException("workerId must be set");
    }
  }
}
