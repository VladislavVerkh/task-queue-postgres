package dev.verkhovskiy.taskqueue.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.exception.TaskOwnershipLostException;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.sample.TaskQueueSampleApplication;
import dev.verkhovskiy.taskqueue.service.TaskQueueService;
import dev.verkhovskiy.taskqueue.service.TaskRetryService;
import dev.verkhovskiy.taskqueue.service.WorkerCoordinationService;
import dev.verkhovskiy.taskqueue.testkit.TaskQueuePostgresContainerSupport;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Интеграционные тесты регистрации воркеров, failover и ребаланса партиций. */
@SpringBootTest(
    classes = TaskQueueSampleApplication.class,
    properties = {
      "task.queue.runtime-enabled=false",
      "task.queue.partition-count=20",
      "task.queue.process-timeout=10s",
      "task.queue.heartbeat-deviation=0s",
      "task.queue.cleanup-batch-size=100",
      "task.queue.dead-letter-enabled=true",
      "task.queue.not-retryable-exceptions=java.lang.IllegalArgumentException"
    })
@Testcontainers(disabledWithoutDocker = true)
class TaskQueueIntegrationTest {

  private static final String P1W1 = "pod-1-thread-1";
  private static final String P1W2 = "pod-1-thread-2";
  private static final String P2W1 = "pod-2-thread-1";
  private static final String P2W2 = "pod-2-thread-2";

  @Container
  static final PostgreSQLContainer<?> POSTGRES =
      TaskQueuePostgresContainerSupport.createContainer();

  @Autowired private WorkerCoordinationService workerCoordinationService;

  @Autowired private TaskQueueService queueService;

  @Autowired private TaskQueueRepository queueRepository;

  @Autowired private TaskRetryService retryService;

  @Autowired private NamedParameterJdbcTemplate jdbc;

  @DynamicPropertySource
  static void configureDatasource(DynamicPropertyRegistry registry) {
    TaskQueuePostgresContainerSupport.registerDatasource(registry, POSTGRES);
  }

  @BeforeEach
  void cleanTables() {
    jdbc.update("delete from task_queue_dead_letter", new MapSqlParameterSource());
    jdbc.update("delete from task_queue", new MapSqlParameterSource());
    jdbc.update("delete from task_worker_registry", new MapSqlParameterSource());
  }

  @Test
  void rebalancesAcrossTwoPodsWithTwoThreadsEach() {
    workerCoordinationService.registerWorker(P1W1);
    workerCoordinationService.registerWorker(P1W2);

    Map<String, Integer> firstPodDistribution = loadAssignmentCounts();
    assertEquals(2, firstPodDistribution.size());
    assertEquals(10, firstPodDistribution.get(P1W1));
    assertEquals(10, firstPodDistribution.get(P1W2));

    workerCoordinationService.registerWorker(P2W1);
    workerCoordinationService.registerWorker(P2W2);

    Map<String, Integer> secondPodDistribution = loadAssignmentCounts();
    assertEquals(4, secondPodDistribution.size());
    assertEquals(5, secondPodDistribution.get(P1W1));
    assertEquals(5, secondPodDistribution.get(P1W2));
    assertEquals(5, secondPodDistribution.get(P2W1));
    assertEquals(5, secondPodDistribution.get(P2W2));
  }

  @Test
  void failoverReleasesTaskLocksAndRebalances() {
    workerCoordinationService.registerWorker(P1W1);
    workerCoordinationService.registerWorker(P1W2);
    workerCoordinationService.registerWorker(P2W1);
    workerCoordinationService.registerWorker(P2W2);

    int partitionOwnedByDeadWorker = loadAssignedPartitionsOfSecondPodFirstWorker().getFirst();
    UUID taskId = UUID.randomUUID();
    Instant now = Instant.now();
    queueRepository.enqueue(
        taskId, "integration-test", "{}", "k", partitionOwnedByDeadWorker, now, now);

    List<QueuedTask> lockedTasks = queueService.dequeueForWorker(P2W1, 1);
    assertEquals(1, lockedTasks.size());
    assertEquals(taskId, lockedTasks.getFirst().taskId());

    markWorkerAsDead(P2W1);
    int cleaned = workerCoordinationService.cleanUpDeadWorkers();
    assertEquals(1, cleaned);

    String taskOwner =
        jdbc.queryForObject(
            "select worker_id from task_queue where task_id = :taskId",
            new MapSqlParameterSource("taskId", taskId),
            String.class);
    assertNull(taskOwner);

    Map<String, Integer> assignmentCounts = loadAssignmentCounts();
    assertFalse(assignmentCounts.containsKey(P2W1));
    assertEquals(3, assignmentCounts.size());

    List<Integer> sortedCounts = new ArrayList<>(assignmentCounts.values());
    sortedCounts.sort(Comparator.naturalOrder());
    assertEquals(List.of(6, 7, 7), sortedCounts);
  }

  @Test
  void staleWorkerCannotAcknowledgeTaskAfterCleanupReleasedOwnership() {
    workerCoordinationService.registerWorker(P1W1);

    UUID taskId = UUID.randomUUID();
    Instant now = Instant.now();
    queueRepository.enqueue(taskId, "integration-test", "{}", "k", 1, now, now);

    List<QueuedTask> lockedTasks = queueService.dequeueForWorker(P1W1, 1);
    assertEquals(1, lockedTasks.size());
    assertEquals(taskId, lockedTasks.getFirst().taskId());

    markWorkerAsDead(P1W1);
    int cleaned = workerCoordinationService.cleanUpDeadWorkers();
    assertEquals(1, cleaned);

    assertThrows(TaskOwnershipLostException.class, () -> queueService.acknowledge(taskId, P1W1));

    Integer remaining =
        jdbc.queryForObject(
            "select count(*) from task_queue where task_id = :taskId",
            new MapSqlParameterSource("taskId", taskId),
            Integer.class);
    assertEquals(1, remaining);
  }

  @Test
  void staleWorkerCannotDelayTaskAlreadyOwnedByAnotherWorker() {
    workerCoordinationService.registerWorker(P1W1);
    workerCoordinationService.registerWorker(P2W1);

    UUID taskId = UUID.randomUUID();
    Instant now = Instant.now();
    queueRepository.enqueue(taskId, "integration-test", "{}", "k", 1, now, now);
    setTaskOwner(taskId, P2W1);
    RuntimeException failure = new RuntimeException("boom");

    assertThrows(
        TaskOwnershipLostException.class,
        () -> retryService.retryOrFinalize(taskId, 0, failure, P1W1));

    Map<String, Object> row =
        jdbc.queryForMap(
            "select worker_id, delay_count from task_queue where task_id = :taskId",
            new MapSqlParameterSource("taskId", taskId));
    assertEquals(P2W1, row.get("worker_id"));
    assertEquals(0L, row.get("delay_count"));
  }

  @Test
  void nonRetryableTaskIsMovedToDeadLetterWhenEnabled() {
    workerCoordinationService.registerWorker(P1W1);

    UUID taskId = UUID.randomUUID();
    Instant now = Instant.now();
    queueRepository.enqueue(taskId, "integration-test", "{}", "k", 1, now, now);

    List<QueuedTask> lockedTasks = queueService.dequeueForWorker(P1W1, 1);
    assertEquals(1, lockedTasks.size());
    IllegalArgumentException failure = new IllegalArgumentException("bad payload");

    retryService.retryOrFinalize(taskId, lockedTasks.getFirst().delayCount(), failure, P1W1);

    Integer queueRows =
        jdbc.queryForObject(
            "select count(*) from task_queue where task_id = :taskId",
            new MapSqlParameterSource("taskId", taskId),
            Integer.class);
    assertEquals(0, queueRows);

    Map<String, Object> deadLetterRow =
        jdbc.queryForMap(
            """
            select task_type,
                   payload,
                   partition_key,
                   partition_num,
                   delay_count,
                   attempt_count,
                   failed_worker_id,
                   reason,
                   error_class,
                   error_message
              from task_queue_dead_letter
             where task_id = :taskId
            """,
            new MapSqlParameterSource("taskId", taskId));
    assertEquals("integration-test", deadLetterRow.get("task_type"));
    assertEquals("{}", deadLetterRow.get("payload"));
    assertEquals("k", deadLetterRow.get("partition_key"));
    assertEquals(1, deadLetterRow.get("partition_num"));
    assertEquals(0L, deadLetterRow.get("delay_count"));
    assertEquals(1L, deadLetterRow.get("attempt_count"));
    assertEquals(P1W1, deadLetterRow.get("failed_worker_id"));
    assertEquals("NON_RETRYABLE", deadLetterRow.get("reason"));
    assertEquals(IllegalArgumentException.class.getName(), deadLetterRow.get("error_class"));
    assertEquals("bad payload", deadLetterRow.get("error_message"));
  }

  @Test
  void failoverOfWholeSecondPodRebalancesToFirstPodThreads() {
    workerCoordinationService.registerWorker(P1W1);
    workerCoordinationService.registerWorker(P1W2);
    workerCoordinationService.registerWorker(P2W1);
    workerCoordinationService.registerWorker(P2W2);

    markWorkerAsDead(P2W1);
    markWorkerAsDead(P2W2);
    int cleaned = workerCoordinationService.cleanUpDeadWorkers();
    assertEquals(2, cleaned);

    Map<String, Integer> assignmentCounts = loadAssignmentCounts();
    assertEquals(2, assignmentCounts.size());
    assertEquals(10, assignmentCounts.get(P1W1));
    assertEquals(10, assignmentCounts.get(P1W2));
    assertTrue(!assignmentCounts.containsKey(P2W1) && !assignmentCounts.containsKey(P2W2));
  }

  private void markWorkerAsDead(String workerId) {
    jdbc.update(
        """
            update task_worker_registry
               set heartbeat_last = now() - interval '5 minutes'
             where worker_id = :workerId
            """,
        new MapSqlParameterSource("workerId", workerId));
  }

  private void setTaskOwner(UUID taskId, String workerId) {
    jdbc.update(
        """
            update task_queue
               set worker_id = :workerId
             where task_id = :taskId
            """,
        new MapSqlParameterSource().addValue("taskId", taskId).addValue("workerId", workerId));
  }

  private List<Integer> loadAssignedPartitionsOfSecondPodFirstWorker() {
    return jdbc.queryForList(
        """
            select partition_num
              from task_worker_partition_assignment
             where worker_id = :workerId
             order by partition_num
            """,
        new MapSqlParameterSource("workerId", P2W1),
        Integer.class);
  }

  private Map<String, Integer> loadAssignmentCounts() {
    return jdbc.query(
        """
            select worker_id, count(*) as cnt
              from task_worker_partition_assignment
             group by worker_id
            """,
        rs -> {
          var result = new java.util.HashMap<String, Integer>();
          while (rs.next()) {
            result.put(rs.getString("worker_id"), rs.getInt("cnt"));
          }
          return result;
        });
  }
}
