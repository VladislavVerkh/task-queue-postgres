package dev.verkhovskiy.taskqueue.rest;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.persistence.TaskDeadLetterRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetricsRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetricsRepository.PartitionLagMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetricsRepository.QueueStateMetrics;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository.PartitionAssignment;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.CleanupDeadWorkersResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.CleanupExpiredLeasesResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.OperationResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.PartitionResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.RequeueDeadLetterRequest;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.RequeueDeadLetterResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.SummaryResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.WorkerResponse;
import dev.verkhovskiy.taskqueue.service.TaskDeadLetterService;
import dev.verkhovskiy.taskqueue.service.WorkerCoordinationService;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TaskQueueAdminService {

  private static final OperationResponse COMPLETED = new OperationResponse("completed");

  private final TaskQueueMetricsRepository metricsRepository;
  private final TaskDeadLetterRepository deadLetterRepository;
  private final WorkerRegistryRepository workerRegistryRepository;
  private final WorkerCoordinationService workerCoordinationService;
  private final TaskDeadLetterService deadLetterService;
  private final TaskQueueProperties properties;

  public SummaryResponse summary() {
    QueueStateMetrics queueState = metricsRepository.loadQueueStateMetrics();
    return new SummaryResponse(
        queueState.readyTasks(),
        queueState.inFlightTasks(),
        queueState.oldestReadyAgeSeconds(),
        queueState.oldestInFlightAgeSeconds(),
        deadLetterRepository.countDeadLetters(),
        workerRegistryRepository.countDrainingAssignments());
  }

  public List<PartitionResponse> partitions() {
    int partitionCount = properties.getPartitionCount();
    Map<Integer, PartitionAssignment> assignments =
        workerRegistryRepository.findPartitionAssignments(partitionCount);
    Map<Integer, PartitionLagMetrics> lagByPartition =
        metricsRepository.loadPartitionLagMetrics(partitionCount).stream()
            .collect(Collectors.toMap(PartitionLagMetrics::partitionNum, Function.identity()));

    return IntStream.rangeClosed(1, partitionCount)
        .mapToObj(
            partitionNum ->
                partitionResponse(
                    partitionNum, assignments.get(partitionNum), lagByPartition.get(partitionNum)))
        .toList();
  }

  public List<WorkerResponse> workers() {
    long heartbeatDeviationSeconds = properties.getHeartbeatDeviation().toSeconds();
    return workerRegistryRepository
        .loadWorkerSnapshots(
            heartbeatDeviationSeconds, properties.getDeadProcessTimeoutMultiplier())
        .stream()
        .map(
            worker ->
                new WorkerResponse(
                    worker.workerId(),
                    worker.heartbeatLast(),
                    worker.timeoutSeconds(),
                    worker.createdAt(),
                    worker.expired(),
                    worker.assignedPartitions(),
                    worker.drainingPartitions()))
        .sorted(Comparator.comparing(WorkerResponse::createdAt))
        .toList();
  }

  public OperationResponse rebalance() {
    workerCoordinationService.rebalance();
    return COMPLETED;
  }

  public OperationResponse reconcileHandoffs() {
    workerCoordinationService.reconcileHandoffs();
    return COMPLETED;
  }

  public CleanupExpiredLeasesResponse cleanupExpiredLeases() {
    return new CleanupExpiredLeasesResponse(workerCoordinationService.cleanUpExpiredTaskLeases());
  }

  public CleanupDeadWorkersResponse cleanupDeadWorkers() {
    return new CleanupDeadWorkersResponse(workerCoordinationService.cleanUpDeadWorkers());
  }

  public RequeueDeadLetterResponse requeueDeadLetter(
      UUID taskId, RequeueDeadLetterRequest request) {
    Instant availableAt = request == null ? null : request.availableAt();
    Duration availableAfter = request == null ? null : request.availableAfter();
    validateRequeueRequest(availableAt, availableAfter);

    boolean requeued =
        availableAfter == null
            ? deadLetterService.requeue(taskId, availableAt)
            : deadLetterService.requeueDelayed(taskId, availableAfter);
    if (!requeued) {
      throw TaskQueueRestException.notFound("Dead-letter task not found: " + taskId);
    }
    return new RequeueDeadLetterResponse(taskId, true);
  }

  private static PartitionResponse partitionResponse(
      int partitionNum, PartitionAssignment assignment, PartitionLagMetrics lag) {
    return new PartitionResponse(
        partitionNum,
        assignment == null ? null : assignment.workerId(),
        assignment == null ? null : assignment.handoffState().name(),
        assignment == null ? null : assignment.pendingWorkerId(),
        assignment == null ? null : assignment.drainStartedAt(),
        assignment == null ? null : assignment.drainDeadlineAt(),
        assignment == null ? null : assignment.drainDeadlineReached(),
        assignment == null ? null : assignment.drainDuration().toMillis(),
        lag == null ? 0L : lag.readyTasks(),
        lag == null ? 0L : lag.oldestReadyAgeSeconds());
  }

  private static void validateRequeueRequest(Instant availableAt, Duration availableAfter) {
    if (availableAt != null && availableAfter != null) {
      throw TaskQueueRestException.badRequest("availableAt and availableAfter cannot both be set");
    }
    if (availableAfter != null && availableAfter.isNegative()) {
      throw TaskQueueRestException.badRequest("availableAfter must be greater than or equal to 0");
    }
  }
}
