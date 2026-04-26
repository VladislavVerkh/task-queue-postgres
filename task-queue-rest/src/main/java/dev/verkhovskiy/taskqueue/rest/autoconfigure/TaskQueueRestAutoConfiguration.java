package dev.verkhovskiy.taskqueue.rest.autoconfigure;

import dev.verkhovskiy.taskqueue.autoconfigure.TaskQueueAutoConfiguration;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.persistence.TaskDeadLetterRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetricsRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import dev.verkhovskiy.taskqueue.rest.TaskQueueAdminController;
import dev.verkhovskiy.taskqueue.rest.TaskQueueAdminService;
import dev.verkhovskiy.taskqueue.rest.TaskQueueRestExceptionHandler;
import dev.verkhovskiy.taskqueue.service.TaskDeadLetterService;
import dev.verkhovskiy.taskqueue.service.WorkerCoordinationService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RestController;

@AutoConfiguration(after = TaskQueueAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(RestController.class)
@ConditionalOnProperty(
    prefix = "task.queue.rest",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@ConditionalOnBean({
  TaskQueueMetricsRepository.class,
  TaskDeadLetterRepository.class,
  WorkerRegistryRepository.class,
  WorkerCoordinationService.class,
  TaskDeadLetterService.class,
  TaskQueueProperties.class
})
public class TaskQueueRestAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  TaskQueueAdminService taskQueueAdminService(
      TaskQueueMetricsRepository metricsRepository,
      TaskDeadLetterRepository deadLetterRepository,
      WorkerRegistryRepository workerRegistryRepository,
      WorkerCoordinationService workerCoordinationService,
      TaskDeadLetterService deadLetterService,
      TaskQueueProperties properties) {
    return new TaskQueueAdminService(
        metricsRepository,
        deadLetterRepository,
        workerRegistryRepository,
        workerCoordinationService,
        deadLetterService,
        properties);
  }

  @Bean
  @ConditionalOnMissingBean
  TaskQueueAdminController taskQueueAdminController(TaskQueueAdminService adminService) {
    return new TaskQueueAdminController(adminService);
  }

  @Bean
  @ConditionalOnMissingBean
  TaskQueueRestExceptionHandler taskQueueRestExceptionHandler() {
    return new TaskQueueRestExceptionHandler();
  }
}
