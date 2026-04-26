package dev.verkhovskiy.taskqueue.rest.dto;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public final class TaskQueueAdminDtos {

  private TaskQueueAdminDtos() {}

  public record SummaryResponse(
      long readyTasks,
      long inFlightTasks,
      long oldestReadyAgeSeconds,
      long oldestInFlightAgeSeconds,
      long deadLetters,
      int drainingPartitions) {}

  public record PartitionResponse(
      int partitionNum,
      String workerId,
      String handoffState,
      String pendingWorkerId,
      Instant drainStartedAt,
      Instant drainDeadlineAt,
      Boolean drainDeadlineReached,
      Long drainDurationMillis,
      long readyTasks,
      long oldestReadyAgeSeconds) {}

  public record WorkerResponse(
      String workerId,
      Instant heartbeatLast,
      long timeoutSeconds,
      Instant createdAt,
      boolean expired,
      int assignedPartitions,
      int drainingPartitions) {}

  public record OperationResponse(String status) {}

  public record CleanupExpiredLeasesResponse(int releasedTasks) {}

  public record CleanupDeadWorkersResponse(int removedWorkers) {}

  public record RequeueDeadLetterRequest(Instant availableAt, Duration availableAfter) {}

  public record RequeueDeadLetterResponse(UUID taskId, boolean requeued) {}
}
