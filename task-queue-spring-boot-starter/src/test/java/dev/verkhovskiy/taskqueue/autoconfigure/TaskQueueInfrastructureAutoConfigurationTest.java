package dev.verkhovskiy.taskqueue.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.runtime.TaskQueueRuntimeShutdownStrategy;
import dev.verkhovskiy.taskqueue.service.TaskIdGenerator;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
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
  void registersTaskQueueAliasesForApplicationInfrastructure() {
    contextRunner.run(
        context -> {
          DataSource dataSource = context.getBean("dataSource", DataSource.class);
          PlatformTransactionManager transactionManager =
              context.getBean("transactionManager", PlatformTransactionManager.class);

          assertThat(context.getBean(TaskQueueBeanNames.DATA_SOURCE, DataSource.class))
              .isSameAs(dataSource);
          assertThat(
                  context.getBean(
                      TaskQueueBeanNames.TRANSACTION_MANAGER, PlatformTransactionManager.class))
              .isSameAs(transactionManager);
          assertThat(context.getBean(TaskQueueBeanNames.JDBC_TEMPLATE, JdbcTemplate.class))
              .extracting(JdbcTemplate::getDataSource)
              .isSameAs(dataSource);
          assertThat(
                  context
                      .getBean(
                          TaskQueueBeanNames.NAMED_PARAMETER_JDBC_TEMPLATE,
                          NamedParameterJdbcTemplate.class)
                      .getJdbcTemplate()
                      .getDataSource())
              .isSameAs(dataSource);
        });
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

  @Test
  void backsOffWhenCustomTaskIdGeneratorExists() {
    TaskIdGenerator customGenerator = () -> UUID.fromString("018f0000-0000-7000-8000-000000000001");

    contextRunner
        .withBean(TaskIdGenerator.class, () -> customGenerator)
        .run(
            context -> {
              assertThat(context).hasSingleBean(TaskIdGenerator.class);
              assertThat(context.getBean(TaskIdGenerator.class)).isSameAs(customGenerator);
            });
  }

  @Test
  void failsFastWhenTaskQueueTransactionManagerDiffersFromDefault() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(TaskQueueInfrastructureAutoConfiguration.class))
        .withUserConfiguration(MismatchedTransactionManagerConfiguration.class)
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(rootCause(context.getStartupFailure()))
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining("task-queue must use the same transaction manager");
            });
  }

  @Test
  void failsFastWhenTransactionManagerUsesDifferentDataSource() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(TaskQueueInfrastructureAutoConfiguration.class))
        .withUserConfiguration(MismatchedDataSourceConfiguration.class)
        .run(
            context -> {
              assertThat(context).hasFailed();
              assertThat(rootCause(context.getStartupFailure()))
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining(
                      "task-queue datasource and transaction manager datasource differ");
            });
  }

  private static Throwable rootCause(Throwable failure) {
    Throwable result = failure;
    while (result.getCause() != null) {
      result = result.getCause();
    }
    return result;
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

  @Configuration(proxyBeanMethods = false)
  static class MismatchedTransactionManagerConfiguration {

    @Bean
    DataSource dataSource() {
      return new DriverManagerDataSource();
    }

    @Bean
    DataSource otherDataSource() {
      return new DriverManagerDataSource();
    }

    @Bean
    PlatformTransactionManager transactionManager(@Qualifier("dataSource") DataSource dataSource) {
      return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = TaskQueueBeanNames.TRANSACTION_MANAGER)
    PlatformTransactionManager taskQueueTransactionManager(
        @Qualifier("otherDataSource") DataSource otherDataSource) {
      return new DataSourceTransactionManager(otherDataSource);
    }

    @Bean
    TaskQueueProperties taskQueueProperties() {
      return new TaskQueueProperties();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class MismatchedDataSourceConfiguration {

    @Bean
    DataSource dataSource() {
      return new DriverManagerDataSource();
    }

    @Bean(name = TaskQueueBeanNames.DATA_SOURCE)
    DataSource taskQueueDataSource() {
      return new DriverManagerDataSource();
    }

    @Bean
    PlatformTransactionManager transactionManager(@Qualifier("dataSource") DataSource dataSource) {
      return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    TaskQueueProperties taskQueueProperties() {
      return new TaskQueueProperties();
    }
  }
}
