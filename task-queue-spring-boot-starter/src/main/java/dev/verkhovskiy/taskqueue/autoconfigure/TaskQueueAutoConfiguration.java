package dev.verkhovskiy.taskqueue.autoconfigure;

import dev.verkhovskiy.taskqueue.config.MetricsConfiguration;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.handler.TaskHandlerRegistry;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.persistence.TaskDeadLetterRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetadataRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetricsRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import dev.verkhovskiy.taskqueue.retry.RetryBackoffPolicy;
import dev.verkhovskiy.taskqueue.retry.RetryExceptionClassifier;
import dev.verkhovskiy.taskqueue.runtime.QueueWorkerRuntime;
import dev.verkhovskiy.taskqueue.service.PartitionAssignmentPlanner;
import dev.verkhovskiy.taskqueue.service.TaskDeadLetterService;
import dev.verkhovskiy.taskqueue.service.TaskExecutionService;
import dev.verkhovskiy.taskqueue.service.TaskPartitioner;
import dev.verkhovskiy.taskqueue.service.TaskProducerService;
import dev.verkhovskiy.taskqueue.service.TaskQueuePartitionGuard;
import dev.verkhovskiy.taskqueue.service.TaskQueueService;
import dev.verkhovskiy.taskqueue.service.TaskRetryService;
import dev.verkhovskiy.taskqueue.service.WorkerCoordinationService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

/** Автоконфигурация библиотеки очереди задач. */
@AutoConfiguration
@EnableConfigurationProperties(TaskQueueProperties.class)
@Import({
  TaskQueueInfrastructureAutoConfiguration.class,
  MetricsConfiguration.class,
  TaskPartitioner.class,
  PartitionAssignmentPlanner.class,
  RetryBackoffPolicy.class,
  RetryExceptionClassifier.class,
  TaskHandlerRegistry.class,
  TaskQueueMetrics.class,
  TaskQueueMetricsRepository.class,
  TaskQueueMetadataRepository.class,
  TaskQueueRepository.class,
  TaskDeadLetterRepository.class,
  WorkerRegistryRepository.class,
  TaskQueuePartitionGuard.class,
  TaskQueueService.class,
  TaskExecutionService.class,
  TaskRetryService.class,
  TaskDeadLetterService.class,
  TaskProducerService.class,
  WorkerCoordinationService.class,
  QueueWorkerRuntime.class
})
public class TaskQueueAutoConfiguration {}
