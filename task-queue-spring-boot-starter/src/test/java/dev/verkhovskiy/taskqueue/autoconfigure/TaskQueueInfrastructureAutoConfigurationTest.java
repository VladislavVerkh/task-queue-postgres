package dev.verkhovskiy.taskqueue.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.runtime.TaskQueueRuntimeShutdownStrategy;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;

class TaskQueueInfrastructureAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(TaskQueueInfrastructureAutoConfiguration.class))
          .withUserConfiguration(InfrastructureBeansConfiguration.class);

  @Test
  void createsDefaultRuntimeShutdownStrategy() {
    contextRunner.run(
        context -> assertThat(context).hasSingleBean(TaskQueueRuntimeShutdownStrategy.class));
  }

  @Test
  void backsOffWhenCustomRuntimeShutdownStrategyExists() {
    TaskQueueRuntimeShutdownStrategy customStrategy = (exitCode, message, cause) -> {};

    contextRunner
        .withBean(TaskQueueRuntimeShutdownStrategy.class, () -> customStrategy)
        .run(
            context ->
                assertThat(context.getBean(TaskQueueRuntimeShutdownStrategy.class))
                    .isSameAs(customStrategy));
  }

  @Configuration(proxyBeanMethods = false)
  static class InfrastructureBeansConfiguration {

    @Bean
    DataSource dataSource() {
      return new DriverManagerDataSource();
    }

    @Bean
    PlatformTransactionManager transactionManager(DataSource dataSource) {
      return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    TaskQueueProperties taskQueueProperties() {
      return new TaskQueueProperties();
    }
  }
}
