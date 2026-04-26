package dev.verkhovskiy.taskqueue.service;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import java.util.LinkedHashMap;
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

  /**
   * Регистрирует воркер и инициирует ребаланс партиций.
   *
   * @param workerId идентификатор воркера
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public void registerWorker(String workerId) {
    try {
      workerRegistryRepository.registerWorker(workerId, properties.getProcessTimeout().toSeconds());
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
    workerRegistryRepository.heartbeatWorker(workerId);
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
    List<String> deadWorkerIds =
        workerRegistryRepository.findExpiredWorkerIdsForUpdate(
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

  /**
   * Освобождает задачи, lease которых истек, даже если heartbeat воркера еще жив.
   *
   * @return количество освобожденных задач
   */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public int cleanUpExpiredTaskLeases() {
    int released = queueRepository.releaseExpiredTaskLeases(properties.getCleanupBatchSize());
    metrics.expiredTaskLeasesReleased(released);
    return released;
  }

  /** Обновляет aggregate-метрики состояния очереди. */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER, readOnly = true)
  public void refreshQueueStateMetrics() {
    metrics.setQueueState(queueRepository.loadQueueStateMetrics());
    if (properties.isPartitionLagMetricsEnabled()) {
      metrics.setPartitionLag(
          queueRepository.loadPartitionLagMetrics(properties.getPartitionCount()));
    }
  }

  /** Принудительно запускает ребаланс партиций. */
  @SuppressWarnings("unused")
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public void rebalance() {
    rebalanceInternal();
  }

  /** Синхронизирует застрявшие DRAINING-assignments без изменения состава воркеров. */
  @Transactional(transactionManager = TaskQueueBeanNames.TRANSACTION_MANAGER)
  public void reconcileHandoffs() {
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
              metrics.setDrainingAssignments(0);
              return null;
            }

            Map<Integer, WorkerRegistryRepository.PartitionAssignment> currentAssignments =
                workerRegistryRepository.findPartitionAssignments(properties.getPartitionCount());
            Map<Integer, String> currentOwners = currentOwners(currentAssignments);
            Map<Integer, String> assignmentPlan =
                assignmentPlanner.plan(workerIds, properties.getPartitionCount(), currentOwners);
            applyAssignmentPlan(assignmentPlan, currentAssignments);
            metrics.setDrainingAssignments(workerRegistryRepository.countDrainingAssignments());
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

  private void applyAssignmentPlan(
      Map<Integer, String> assignmentPlan,
      Map<Integer, WorkerRegistryRepository.PartitionAssignment> currentAssignments) {
    assignmentPlan.forEach(
        (partitionNum, targetWorkerId) -> {
          WorkerRegistryRepository.PartitionAssignment current =
              currentAssignments.get(partitionNum);
          if (current == null) {
            workerRegistryRepository.upsertActiveAssignment(partitionNum, targetWorkerId);
            return;
          }

          if (!current.draining()) {
            handleActiveAssignment(partitionNum, targetWorkerId, current);
            return;
          }

          handleDrainingAssignment(partitionNum, targetWorkerId, current);
        });
  }

  private void handleActiveAssignment(
      int partitionNum,
      String targetWorkerId,
      WorkerRegistryRepository.PartitionAssignment current) {
    if (current.workerId().equals(targetWorkerId)) {
      return;
    }

    if (!queueRepository.hasInFlightTasks(partitionNum, current.workerId())) {
      workerRegistryRepository.upsertActiveAssignment(partitionNum, targetWorkerId);
      return;
    }

    workerRegistryRepository.startDraining(
        partitionNum, targetWorkerId, properties.getHandoffDrainTimeout());
    metrics.handoffStarted();
  }

  private void handleDrainingAssignment(
      int partitionNum,
      String targetWorkerId,
      WorkerRegistryRepository.PartitionAssignment current) {
    if (current.workerId().equals(targetWorkerId)) {
      workerRegistryRepository.cancelDraining(partitionNum);
      metrics.handoffCancelled(current.drainDuration());
      return;
    }

    if (!targetWorkerId.equals(current.pendingWorkerId())) {
      workerRegistryRepository.updatePendingWorker(partitionNum, targetWorkerId);
    }

    if (!queueRepository.hasInFlightTasks(partitionNum, current.workerId())) {
      workerRegistryRepository.completeHandoff(partitionNum, targetWorkerId);
      metrics.handoffCompleted(current.drainDuration());
      return;
    }

    if (!current.drainDeadlineReached()) {
      return;
    }

    metrics.handoffTimeout();
    switch (properties.getHandoffTimeoutAction()) {
      case EXTEND -> {
        workerRegistryRepository.extendDrainDeadline(
            partitionNum, properties.getHandoffDrainTimeout());
        metrics.handoffTimeoutExtended();
      }
      case ABORT -> {
        workerRegistryRepository.cancelDraining(partitionNum);
        metrics.handoffTimeoutAborted(current.drainDuration());
      }
      case FORCE -> {
        workerRegistryRepository.completeHandoff(partitionNum, targetWorkerId);
        metrics.handoffTimeoutForced(current.drainDuration());
      }
    }
  }

  private static Map<Integer, String> currentOwners(
      Map<Integer, WorkerRegistryRepository.PartitionAssignment> currentAssignments) {
    Map<Integer, String> owners = new LinkedHashMap<>(currentAssignments.size());
    currentAssignments.forEach(
        (partitionNum, assignment) -> owners.put(partitionNum, assignment.workerId()));
    return owners;
  }
}
