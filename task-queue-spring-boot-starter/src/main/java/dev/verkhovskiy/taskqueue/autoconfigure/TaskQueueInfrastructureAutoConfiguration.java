package dev.verkhovskiy.taskqueue.autoconfigure;

import dev.verkhovskiy.taskqueue.config.TaskQueueBeanNames;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.runtime.TaskQueueRuntimeShutdownStrategy;
import dev.verkhovskiy.taskqueue.service.TaskIdGenerator;
import dev.verkhovskiy.taskqueue.service.UuidV7TaskIdGenerator;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.AliasRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.ResourceTransactionManager;

/**
 * Автоконфигурация инфраструктурных бинов task-queue.
 *
 * <p>Создает именованные alias-бины поверх application-бинов, чтобы библиотека и пользовательские
 * хэндлеры работали с единым пулом подключений и единым менеджером транзакций.
 */
@Configuration(proxyBeanMethods = false)
public class TaskQueueInfrastructureAutoConfiguration {

  private static final String DEFAULT_TRANSACTION_MANAGER_BEAN_NAME = "transactionManager";

  /**
   * Регистрирует алиасы task-queue для {@link DataSource} и {@link PlatformTransactionManager} без
   * создания новых bean definition.
   *
   * <p>Это важно для Spring Boot 4: если создать дополнительный {@code DataSource}-бин заранее,
   * базовая автоконфигурация datasource может не сработать (из-за
   * {@code @ConditionalOnMissingBean(DataSource.class)}).
   *
   * @return post-processor регистрации алиасов
   */
  @Bean
  public static BeanFactoryPostProcessor taskQueueInfrastructureAliasesPostProcessor() {
    return beanFactory -> {
      if (!(beanFactory instanceof AliasRegistry aliasRegistry)) {
        return;
      }

      registerAliasIfNeeded(
          beanFactory,
          aliasRegistry,
          DataSource.class,
          TaskQueueBeanNames.DATA_SOURCE,
          "dataSource");
      registerAliasIfNeeded(
          beanFactory,
          aliasRegistry,
          PlatformTransactionManager.class,
          TaskQueueBeanNames.TRANSACTION_MANAGER,
          DEFAULT_TRANSACTION_MANAGER_BEAN_NAME);
    };
  }

  /**
   * Создает {@link JdbcTemplate} task-queue, привязанный к task-queue datasource.
   *
   * @return jdbc template task-queue
   */
  @Bean(name = TaskQueueBeanNames.JDBC_TEMPLATE)
  @ConditionalOnMissingBean(name = TaskQueueBeanNames.JDBC_TEMPLATE)
  public JdbcTemplate taskQueueJdbcTemplate(
      ConfigurableListableBeanFactory beanFactory, TaskQueueProperties properties) {
    DataSource dataSource = beanFactory.getBean(TaskQueueBeanNames.DATA_SOURCE, DataSource.class);
    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
    int queryTimeoutSeconds = queryTimeoutSeconds(properties);
    if (queryTimeoutSeconds > 0) {
      jdbcTemplate.setQueryTimeout(queryTimeoutSeconds);
    }
    return jdbcTemplate;
  }

  /**
   * Создает {@link NamedParameterJdbcTemplate} task-queue.
   *
   * @param jdbcTemplate jdbc template task-queue
   * @return named parameter jdbc template task-queue
   */
  @Bean(name = TaskQueueBeanNames.NAMED_PARAMETER_JDBC_TEMPLATE)
  @ConditionalOnMissingBean(name = TaskQueueBeanNames.NAMED_PARAMETER_JDBC_TEMPLATE)
  public NamedParameterJdbcTemplate taskQueueNamedParameterJdbcTemplate(
      @Qualifier(TaskQueueBeanNames.JDBC_TEMPLATE) JdbcTemplate jdbcTemplate) {
    return new NamedParameterJdbcTemplate(jdbcTemplate);
  }

  /** Создает системные часы UTC для единого источника времени в task-queue. */
  @Bean
  @ConditionalOnMissingBean(Clock.class)
  public Clock taskQueueClock() {
    return Clock.systemUTC();
  }

  /**
   * Создает дефолтный генератор task-id (UUIDv7).
   *
   * <p>Можно переопределить пользовательским bean {@link TaskIdGenerator}.
   */
  @Bean
  @ConditionalOnMissingBean(TaskIdGenerator.class)
  public TaskIdGenerator taskIdGenerator() {
    return new UuidV7TaskIdGenerator();
  }

  /**
   * Создает стратегию graceful shutdown для фатальных ошибок runtime очереди.
   *
   * <p>Остановка контекста выполняется в отдельном потоке, чтобы не блокировать lifecycle shutdown
   * ожиданием worker-потока, который инициировал ошибку.
   */
  @Bean
  @ConditionalOnMissingBean(TaskQueueRuntimeShutdownStrategy.class)
  public TaskQueueRuntimeShutdownStrategy taskQueueRuntimeShutdownStrategy(
      ConfigurableApplicationContext applicationContext) {
    return (exitCode, message, cause) -> {
      Thread shutdownThread =
          new Thread(
              () -> SpringApplication.exit(applicationContext, () -> exitCode),
              "task-queue-runtime-shutdown");
      shutdownThread.setDaemon(false);
      shutdownThread.start();
    };
  }

  /**
   * Выполняет fail-fast валидацию инфраструктуры при старте контекста.
   *
   * @return инициализатор проверки
   */
  @Bean
  public SmartInitializingSingleton taskQueueInfrastructureValidation(
      ConfigurableListableBeanFactory beanFactory) {
    return () ->
        validateInfrastructure(
            beanFactory.getBean(TaskQueueBeanNames.DATA_SOURCE, DataSource.class),
            beanFactory.getBean(
                TaskQueueBeanNames.TRANSACTION_MANAGER, PlatformTransactionManager.class),
            beanFactory.getBean(
                DEFAULT_TRANSACTION_MANAGER_BEAN_NAME, PlatformTransactionManager.class));
  }

  /** Проверяет, что task-queue использует согласованные datasource и transaction manager. */
  private static void validateInfrastructure(
      DataSource taskQueueDataSource,
      PlatformTransactionManager taskQueueTransactionManager,
      PlatformTransactionManager defaultTransactionManager) {
    if (taskQueueTransactionManager != defaultTransactionManager) {
      throw new IllegalStateException(
          "task-queue must use the same transaction manager as application 'transactionManager'. "
              + "Configure bean '"
              + TaskQueueBeanNames.TRANSACTION_MANAGER
              + "' as an alias to 'transactionManager'.");
    }

    if (!(taskQueueTransactionManager
        instanceof ResourceTransactionManager resourceTransactionManager)) {
      return;
    }

    Object resourceFactory = resourceTransactionManager.getResourceFactory();
    if (resourceFactory instanceof DataSource transactionDataSource
        && transactionDataSource != taskQueueDataSource) {
      throw new IllegalStateException(
          "task-queue datasource and transaction manager datasource differ. "
              + "Expected same DataSource instance for beans '"
              + TaskQueueBeanNames.DATA_SOURCE
              + "' and '"
              + TaskQueueBeanNames.TRANSACTION_MANAGER
              + "'.");
    }
  }

  /**
   * Разрешает целевой бин по типу с учетом: 1) имени по умолчанию (если есть), 2) единственного
   * primary, 3) единственного кандидата.
   */
  private static <T> String resolveTargetBeanName(
      ConfigurableListableBeanFactory beanFactory,
      Class<T> beanType,
      String aliasBeanName,
      String defaultBeanName) {
    List<String> candidates =
        Arrays.stream(
                BeanFactoryUtils.beanNamesForTypeIncludingAncestors(
                    beanFactory, beanType, false, false))
            .filter(beanName -> !aliasBeanName.equals(beanName))
            .toList();

    if (candidates.isEmpty()) {
      throw new IllegalStateException(
          "No bean of type "
              + beanType.getName()
              + " available for task-queue alias '"
              + aliasBeanName
              + "'.");
    }

    if (candidates.contains(defaultBeanName)) {
      return defaultBeanName;
    }

    List<String> primaryCandidates =
        candidates.stream().filter(beanName -> isPrimary(beanFactory, beanName)).toList();
    if (primaryCandidates.size() == 1) {
      return primaryCandidates.getFirst();
    }

    if (candidates.size() == 1) {
      return candidates.getFirst();
    }

    throw new IllegalStateException(
        "Ambiguous beans for task-queue alias '"
            + aliasBeanName
            + "': "
            + candidates
            + ". Configure explicit bean '"
            + aliasBeanName
            + "'.");
  }

  private static <T> void registerAliasIfNeeded(
      ConfigurableListableBeanFactory beanFactory,
      AliasRegistry aliasRegistry,
      Class<T> beanType,
      String aliasBeanName,
      String defaultBeanName) {
    if (beanFactory.containsBean(aliasBeanName) || aliasRegistry.isAlias(aliasBeanName)) {
      return;
    }
    String targetBeanName =
        resolveTargetBeanName(beanFactory, beanType, aliasBeanName, defaultBeanName);
    aliasRegistry.registerAlias(targetBeanName, aliasBeanName);
  }

  private static boolean isPrimary(ConfigurableListableBeanFactory beanFactory, String beanName) {
    try {
      return beanFactory.getMergedBeanDefinition(beanName).isPrimary();
    } catch (NoSuchBeanDefinitionException ignored) {
      return false;
    }
  }

  private static int queryTimeoutSeconds(TaskQueueProperties properties) {
    long timeoutMillis = properties.getJdbcStatementTimeout().toMillis();
    if (timeoutMillis <= 0) {
      return 0;
    }
    long timeoutSeconds = Math.ceilDiv(timeoutMillis, 1_000L);
    return timeoutSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) timeoutSeconds;
  }
}
