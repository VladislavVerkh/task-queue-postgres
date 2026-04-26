package dev.verkhovskiy.taskqueue.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.verkhovskiy.taskqueue.config.MetricsConfiguration;
import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.handler.TaskHandlerRegistry;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetadataRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository.PartitionLagMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository.QueueStateMetrics;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import dev.verkhovskiy.taskqueue.retry.RetryBackoffPolicy;
import dev.verkhovskiy.taskqueue.retry.RetryExceptionClassifier;
import dev.verkhovskiy.taskqueue.service.PartitionAssignmentPlanner;
import dev.verkhovskiy.taskqueue.service.TaskDeadLetterService;
import dev.verkhovskiy.taskqueue.service.TaskExecutionService;
import dev.verkhovskiy.taskqueue.service.TaskIdGenerator;
import dev.verkhovskiy.taskqueue.service.TaskPartitioner;
import dev.verkhovskiy.taskqueue.service.TaskQueuePartitionGuard;
import dev.verkhovskiy.taskqueue.service.TaskQueueService;
import dev.verkhovskiy.taskqueue.service.TaskRetryService;
import dev.verkhovskiy.taskqueue.service.UuidV7TaskIdGenerator;
import dev.verkhovskiy.taskqueue.service.WorkerCoordinationService;
import dev.verkhovskiy.taskqueue.testkit.TaskQueuePostgresContainerSupport;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** PostgreSQL-backed тесты SQL-запросов JDBC-реализации. */
@SpringBootTest(
    classes = TaskQueueJdbcPostgresIntegrationTest.TestApplication.class,
    properties = {
      "spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.yaml",
      "task.queue.runtime-enabled=false",
      "task.queue.partition-count=2",
      "task.queue.poll-batch-size=10",
      "task.queue.poll-max-tasks-per-partition=2",
      "task.queue.cleanup-batch-size=100",
      "task.queue.dead-letter-enabled=true",
      "task.queue.not-retryable-exceptions=java.lang.IllegalArgumentException"
    })
@Testcontainers(disabledWithoutDocker = true)
class TaskQueueJdbcPostgresIntegrationTest {

  private static final String WORKER_ID = "worker-1";

  @Container
  static final PostgreSQLContainer<?> POSTGRES =
      TaskQueuePostgresContainerSupport.createContainer();

  @DynamicPropertySource
  static void configureDatasource(DynamicPropertyRegistry registry) {
    TaskQueuePostgresContainerSupport.registerDatasource(registry, POSTGRES);
  }

  @Autowired private WorkerCoordinationService workerCoordinationService;
  @Autowired private TaskQueueService queueService;
  @Autowired private TaskRetryService retryService;
  @Autowired private TaskDeadLetterService deadLetterService;
  @Autowired private TaskQueueRepository queueRepository;
  @Autowired private TaskQueueMetadataRepository metadataRepository;
  @Autowired private NamedParameterJdbcTemplate jdbc;

  @BeforeEach
  void cleanTables() {
    jdbc.update("delete from task_queue_dead_letter", new MapSqlParameterSource());
    jdbc.update("delete from task_queue", new MapSqlParameterSource());
    jdbc.update("delete from task_worker_partition_assignment", new MapSqlParameterSource());
    jdbc.update("delete from task_worker_registry", new MapSqlParameterSource());
  }

  @Test
  void dequeueRespectsPerPartitionLimitAndQueueMetricsQueriesWorkOnPostgres() {
    workerCoordinationService.registerWorker(WORKER_ID);
    Instant now = Instant.now();
    enqueueTask("p1-a", 1, now);
    enqueueTask("p1-b", 1, now);
    enqueueTask("p1-c", 1, now);
    enqueueTask("p2-a", 2, now);
    enqueueTask("p2-b", 2, now);

    List<QueuedTask> tasks = queueService.dequeueForWorker(WORKER_ID, 10);

    assertEquals(4, tasks.size());
    Map<Integer, Long> tasksByPartition =
        tasks.stream()
            .collect(Collectors.groupingBy(QueuedTask::partitionNum, Collectors.counting()));
    assertEquals(2L, tasksByPartition.get(1));
    assertEquals(2L, tasksByPartition.get(2));

    QueueStateMetrics queueStateMetrics = queueRepository.loadQueueStateMetrics();
    assertEquals(1, queueStateMetrics.readyTasks());
    assertEquals(4, queueStateMetrics.inFlightTasks());
    assertTrue(queueStateMetrics.oldestReadyAgeSeconds() >= 0);
    assertTrue(queueStateMetrics.oldestInFlightAgeSeconds() >= 0);

    List<PartitionLagMetrics> partitionLagMetrics = queueRepository.loadPartitionLagMetrics(2);
    assertEquals(1, partitionLagMetrics.size());
    assertEquals(1, partitionLagMetrics.getFirst().partitionNum());
    assertEquals(1, partitionLagMetrics.get(0).readyTasks());
  }

  @Test
  void deadLetterRequeueAndRetentionQueriesWorkOnPostgres() {
    workerCoordinationService.registerWorker(WORKER_ID);
    UUID taskId = enqueueTask("non-retryable", 1, Instant.now());
    List<QueuedTask> tasks = queueService.dequeueForWorker(WORKER_ID, 1);
    assertEquals(1, tasks.size());

    retryService.retryOrFinalize(
        taskId, tasks.getFirst().delayCount(), new IllegalArgumentException("bad"), WORKER_ID);

    assertEquals(0, countRows("task_queue", taskId));
    assertEquals(1, countRows("task_queue_dead_letter", taskId));

    assertTrue(deadLetterService.requeue(taskId));
    assertEquals(1, countRows("task_queue", taskId));
    assertEquals(0, countRows("task_queue_dead_letter", taskId));

    UUID oldDeadLetterTaskId = UUID.randomUUID();
    insertOldDeadLetter(oldDeadLetterTaskId);

    assertEquals(1, deadLetterService.deleteOlderThan(Instant.now().minusSeconds(86_400), 100));
    assertEquals(0, countRows("task_queue_dead_letter", oldDeadLetterTaskId));
  }

  @Test
  void leaseRenewalAndExpiredLeaseCleanupQueriesWorkOnPostgres() {
    workerCoordinationService.registerWorker(WORKER_ID);
    UUID taskId = enqueueTask("leased", 1, Instant.now());
    assertEquals(1, queueService.dequeueForWorker(WORKER_ID, 1).size());

    expireTaskLease(taskId);
    queueService.renewLease(taskId, WORKER_ID);
    assertTrue(isLeaseInFuture(taskId));

    expireTaskLease(taskId);
    assertEquals(1, workerCoordinationService.cleanUpExpiredTaskLeases());
    assertFalse(isTaskOwned(taskId));
  }

  @Test
  void partitionCountGuardPersistsMetadataInPostgres() {
    assertEquals("2", metadataRepository.findValue("partition-count").orElseThrow());
  }

  private UUID enqueueTask(String partitionKey, int partitionNum, Instant now) {
    UUID taskId = UUID.randomUUID();
    queueRepository.enqueue(taskId, "integration-test", "{}", partitionKey, partitionNum, now);
    return taskId;
  }

  private int countRows(String tableName, UUID taskId) {
    return jdbc.queryForObject(
        "select count(*) from " + tableName + " where task_id = :taskId",
        new MapSqlParameterSource("taskId", taskId),
        Integer.class);
  }

  private void expireTaskLease(UUID taskId) {
    jdbc.update(
        """
            update task_queue
               set lease_until = now() - interval '1 second'
             where task_id = :taskId
            """,
        new MapSqlParameterSource("taskId", taskId));
  }

  private boolean isLeaseInFuture(UUID taskId) {
    Boolean result =
        jdbc.queryForObject(
            "select lease_until > now() from task_queue where task_id = :taskId",
            new MapSqlParameterSource("taskId", taskId),
            Boolean.class);
    return Boolean.TRUE.equals(result);
  }

  private boolean isTaskOwned(UUID taskId) {
    Boolean result =
        jdbc.queryForObject(
            "select worker_id is not null from task_queue where task_id = :taskId",
            new MapSqlParameterSource("taskId", taskId),
            Boolean.class);
    return Boolean.TRUE.equals(result);
  }

  private void insertOldDeadLetter(UUID taskId) {
    jdbc.update(
        """
            insert into task_queue_dead_letter(
                task_id,
                task_type,
                payload,
                partition_key,
                partition_num,
                delay_count,
                attempt_count,
                failed_worker_id,
                reason,
                original_created_at,
                dead_lettered_at
            )
            values(
                :taskId,
                'integration-test',
                '{}',
                'old',
                1,
                0,
                1,
                :workerId,
                'NON_RETRYABLE',
                now() - interval '10 days',
                now() - interval '10 days'
            )
            """,
        new MapSqlParameterSource().addValue("taskId", taskId).addValue("workerId", WORKER_ID));
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  @EnableConfigurationProperties(TaskQueueProperties.class)
  @Import({
    MetricsConfiguration.class,
    TaskPartitioner.class,
    PartitionAssignmentPlanner.class,
    RetryBackoffPolicy.class,
    RetryExceptionClassifier.class,
    TaskHandlerRegistry.class,
    TaskQueueMetrics.class,
    TaskQueueMetadataRepository.class,
    TaskQueueRepository.class,
    WorkerRegistryRepository.class,
    TaskQueuePartitionGuard.class,
    TaskQueueService.class,
    TaskExecutionService.class,
    TaskRetryService.class,
    TaskDeadLetterService.class,
    WorkerCoordinationService.class
  })
  static class TestApplication {

    @Bean(name = TaskQueueBeanNames.JDBC_TEMPLATE)
    JdbcTemplate taskQueueJdbcTemplate(DataSource dataSource) {
      return new JdbcTemplate(dataSource);
    }

    @Bean(name = TaskQueueBeanNames.NAMED_PARAMETER_JDBC_TEMPLATE)
    NamedParameterJdbcTemplate taskQueueNamedParameterJdbcTemplate(
        @Qualifier(TaskQueueBeanNames.JDBC_TEMPLATE) JdbcTemplate jdbcTemplate) {
      return new NamedParameterJdbcTemplate(jdbcTemplate);
    }

    @Bean(name = TaskQueueBeanNames.TRANSACTION_MANAGER)
    PlatformTransactionManager taskQueueTransactionManager(DataSource dataSource) {
      return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    TaskIdGenerator taskIdGenerator() {
      return new UuidV7TaskIdGenerator();
    }
  }
}
