package dev.verkhovskiy.taskqueue.service;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.domain.TaskEnqueueRequest;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Сервис базовых операций очереди: постановка, выборка и подтверждение задач. */
@Service
@RequiredArgsConstructor
public class TaskQueueService {

  private static final int MAX_TASK_TYPE_LENGTH = 128;
  private static final int MAX_PARTITION_KEY_LENGTH = 512;

  private final TaskQueueRepository queueRepository;
  private final WorkerRegistryRepository workerRegistryRepository;
  private final TaskPartitioner partitioner;
  private final TaskQueueProperties properties;
  private final TaskIdGenerator taskIdGenerator;

  /**
   * Ставит задачу в очередь.
   *
   * @param request данные задачи
   * @return идентификатор созданной задачи
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public UUID enqueue(TaskEnqueueRequest request) {
    return enqueue(request, null);
  }

  /**
   * Ставит задачу в очередь с задержкой относительно времени PostgreSQL.
   *
   * @param request данные задачи
   * @param availableAfter задержка до доступности задачи
   * @return идентификатор созданной задачи
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public UUID enqueue(TaskEnqueueRequest request, Duration availableAfter) {
    validateEnqueueRequest(request, availableAfter);
    UUID taskId = taskIdGenerator.next();
    int partitionNum =
        partitioner.partition(request.partitionKey(), properties.getPartitionCount());
    queueRepository.enqueue(
        taskId,
        request.taskType(),
        request.payload(),
        request.partitionKey(),
        partitionNum,
        request.availableAt(),
        availableAfter);
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
    requireWorkerId(workerId);
    if (maxCount <= 0) {
      throw new IllegalArgumentException("maxCount must be greater than 0");
    }
    workerRegistryRepository.lockShared(properties.getRebalanceLockKey());
    return queueRepository.lockNextTasksForWorker(
        workerId,
        maxCount,
        properties.getPollMaxTasksPerPartition(),
        properties.getTaskLeaseTimeout());
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

  /**
   * Продлевает lease задачи только если задача все еще закреплена за воркером.
   *
   * @param taskId идентификатор задачи
   * @param workerId идентификатор воркера-владельца
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public void renewLease(UUID taskId, String workerId) {
    requireWorkerId(workerId);
    queueRepository.renewLeaseOwnedBy(taskId, workerId, properties.getTaskLeaseTimeout());
  }

  private static void validateEnqueueRequest(TaskEnqueueRequest request, Duration availableAfter) {
    if (request == null) {
      throw new IllegalArgumentException("request must be set");
    }
    if (request.taskType() == null || request.taskType().isBlank()) {
      throw new IllegalArgumentException("taskType must be set");
    }
    if (request.taskType().length() > MAX_TASK_TYPE_LENGTH) {
      throw new IllegalArgumentException("taskType length must be <= " + MAX_TASK_TYPE_LENGTH);
    }
    if (request.partitionKey() != null
        && request.partitionKey().length() > MAX_PARTITION_KEY_LENGTH) {
      throw new IllegalArgumentException(
          "partitionKey length must be <= " + MAX_PARTITION_KEY_LENGTH);
    }
    if (request.payload() == null) {
      throw new IllegalArgumentException("payload must be set");
    }
    if (request.availableAt() != null && availableAfter != null) {
      throw new IllegalArgumentException("availableAt and availableAfter cannot both be set");
    }
    if (availableAfter != null && availableAfter.isNegative()) {
      throw new IllegalArgumentException("availableAfter must be greater than or equal to 0");
    }
  }

  private static void requireWorkerId(String workerId) {
    if (workerId == null || workerId.isBlank()) {
      throw new IllegalArgumentException("workerId must be set");
    }
  }
}
