package dev.verkhovskiy.taskqueue.rest.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.persistence.TaskDeadLetterRepository;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetricsRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import dev.verkhovskiy.taskqueue.rest.TaskQueueAdminController;
import dev.verkhovskiy.taskqueue.rest.TaskQueueAdminService;
import dev.verkhovskiy.taskqueue.rest.TaskQueueRestExceptionHandler;
import dev.verkhovskiy.taskqueue.service.TaskDeadLetterService;
import dev.verkhovskiy.taskqueue.service.WorkerCoordinationService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;

class TaskQueueRestAutoConfigurationTest {

  private final WebApplicationContextRunner contextRunner =
      new WebApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(TaskQueueRestAutoConfiguration.class))
          .withBean(TaskQueueMetricsRepository.class, () -> mock(TaskQueueMetricsRepository.class))
          .withBean(TaskDeadLetterRepository.class, () -> mock(TaskDeadLetterRepository.class))
          .withBean(WorkerRegistryRepository.class, () -> mock(WorkerRegistryRepository.class))
          .withBean(WorkerCoordinationService.class, () -> mock(WorkerCoordinationService.class))
          .withBean(TaskDeadLetterService.class, () -> mock(TaskDeadLetterService.class))
          .withBean(TaskQueueProperties.class, TaskQueueProperties::new);

  @Test
  void registersRestControllerByDefault() {
    contextRunner.run(
        context -> {
          assertThat(context).hasSingleBean(TaskQueueAdminService.class);
          assertThat(context).hasSingleBean(TaskQueueAdminController.class);
          assertThat(context).hasSingleBean(TaskQueueRestExceptionHandler.class);
        });
  }

  @Test
  void backsOffWhenRestEndpointIsDisabled() {
    contextRunner
        .withPropertyValues("task.queue.rest.enabled=false")
        .run(
            context -> {
              assertThat(context).doesNotHaveBean(TaskQueueAdminService.class);
              assertThat(context).doesNotHaveBean(TaskQueueAdminController.class);
              assertThat(context).doesNotHaveBean(TaskQueueRestExceptionHandler.class);
            });
  }
}
