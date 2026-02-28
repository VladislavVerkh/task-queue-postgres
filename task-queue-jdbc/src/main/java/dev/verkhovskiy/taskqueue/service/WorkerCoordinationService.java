package dev.verkhovskiy.taskqueue.service;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Координирует жизненный цикл воркеров и процесс ребаланса партиций. */
@Service
@RequiredArgsConstructor
public class WorkerCoordinationService {

  private final WorkerRegistryRepository workerRegistryRepository;
  private final TaskQueueRepository queueRepository;
  private final PartitionAssignmentPlanner assignmentPlanner;
  private final TaskQueueProperties properties;
  private final TaskQueueMetrics metrics;
  private final Clock clock;

  /**
   * Регистрирует воркер и инициирует ребаланс партиций.
   *
   * @param workerId идентификатор воркера
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public void registerWorker(String workerId) {
    try {
      Instant now = clock.instant();
      workerRegistryRepository.registerWorker(
          workerId, properties.getProcessTimeout().toSeconds(), now);
      metrics.registerSuccess();
      rebalanceInternal();
    } catch (RuntimeException e) {
      metrics.registerFailure();
      throw e;
    }
  }

  /**
   * Фиксирует heartbeat воркера.
   *
   * @param workerId идентификатор воркера
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public void heartbeatWorker(String workerId) {
    workerRegistryRepository.heartbeatWorker(workerId, clock.instant());
  }

  /**
   * Разрегистрирует воркера, освобождает его задачи и инициирует ребаланс.
   *
   * @param workerId идентификатор воркера
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public void unregisterWorker(String workerId) {
    workerRegistryRepository.lockExclusive(properties.getRebalanceLockKey());
    queueRepository.releaseLockedByWorker(workerId);
    workerRegistryRepository.removeWorker(workerId);
    rebalanceInternal();
  }

  /**
   * Удаляет просроченные воркеры и пересчитывает закрепление партиций.
   *
   * @return количество удаленных воркеров
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public int cleanUpDeadWorkers() {
    workerRegistryRepository.lockExclusive(properties.getRebalanceLockKey());
    Instant now = clock.instant();
    List<String> deadWorkerIds =
        workerRegistryRepository.findExpiredWorkerIdsForUpdate(
            now,
            properties.getHeartbeatDeviation().toSeconds(),
            properties.getDeadProcessTimeoutMultiplier(),
            properties.getCleanupBatchSize());
    if (deadWorkerIds.isEmpty()) {
      metrics.cleanupRun(0);
      return 0;
    }

    deadWorkerIds.forEach(
        workerId -> {
          queueRepository.releaseLockedByWorker(workerId);
          workerRegistryRepository.removeWorker(workerId);
        });

    rebalanceInternal();
    metrics.cleanupRun(deadWorkerIds.size());
    return deadWorkerIds.size();
  }

  /** Принудительно запускает ребаланс партиций. */
  @SuppressWarnings("unused")
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public void rebalance() {
    rebalanceInternal();
  }

  /**
   * Выполняет ребаланс в защищенной секции под advisory-блокировкой.
   *
   * <p>Сначала очищаются некорректные закрепления, затем строится план с минимальным переносом
   * партиций и применяются только необходимые изменения.
   */
  private void rebalanceInternal() {
    try {
      metrics.recordRebalance(
          () -> {
            workerRegistryRepository.lockExclusive(properties.getRebalanceLockKey());
            workerRegistryRepository.removeAssignmentsAbovePartition(
                properties.getPartitionCount());

            List<String> workerIds = workerRegistryRepository.findAllWorkerIdsOrdered();
            if (workerIds.isEmpty()) {
              workerRegistryRepository.clearAssignments();
              return null;
            }

            Map<Integer, String> currentAssignments =
                workerRegistryRepository.findCurrentPartitionAssignments(
                    properties.getPartitionCount());
            Map<Integer, String> assignmentPlan =
                assignmentPlanner.plan(
                    workerIds, properties.getPartitionCount(), currentAssignments);
            Instant ownerChangedAt = clock.instant();
            assignmentPlan.forEach(
                (partitionNum, workerId) ->
                    workerRegistryRepository.upsertPartitionAssignment(
                        partitionNum, workerId, ownerChangedAt));
            return null;
          });
    } catch (RuntimeException e) {
      metrics.rebalanceFailure();
      throw e;
    } catch (Exception e) {
      metrics.rebalanceFailure();
      throw new IllegalStateException("Unexpected rebalance failure", e);
    }
  }
}
