package dev.verkhovskiy.taskqueue.rest;

import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.CleanupDeadWorkersResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.CleanupExpiredLeasesResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.OperationResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.PartitionResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.RequeueDeadLetterRequest;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.RequeueDeadLetterResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.SummaryResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.WorkerResponse;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping(
    path = "${task.queue.rest.base-path:/task-queue/admin/v1}",
    produces = MediaType.APPLICATION_JSON_VALUE)
public class TaskQueueAdminController {

  private final TaskQueueAdminService adminService;

  @GetMapping("/summary")
  public SummaryResponse summary() {
    return adminService.summary();
  }

  @GetMapping("/partitions")
  public List<PartitionResponse> partitions() {
    return adminService.partitions();
  }

  @GetMapping("/workers")
  public List<WorkerResponse> workers() {
    return adminService.workers();
  }

  @PostMapping("/rebalance")
  public OperationResponse rebalance() {
    return adminService.rebalance();
  }

  @PostMapping("/handoffs/reconcile")
  public OperationResponse reconcileHandoffs() {
    return adminService.reconcileHandoffs();
  }

  @PostMapping("/cleanup/expired-leases")
  public CleanupExpiredLeasesResponse cleanupExpiredLeases() {
    return adminService.cleanupExpiredLeases();
  }

  @PostMapping("/cleanup/dead-workers")
  public CleanupDeadWorkersResponse cleanupDeadWorkers() {
    return adminService.cleanupDeadWorkers();
  }

  @PostMapping("/dead-letters/{taskId}/requeue")
  public RequeueDeadLetterResponse requeueDeadLetter(
      @PathVariable("taskId") UUID taskId,
      @RequestBody(required = false) RequeueDeadLetterRequest request) {
    return adminService.requeueDeadLetter(taskId, request);
  }
}
