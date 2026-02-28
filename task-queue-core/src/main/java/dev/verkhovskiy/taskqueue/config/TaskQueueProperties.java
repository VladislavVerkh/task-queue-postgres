package dev.verkhovskiy.taskqueue.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Duration;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Конфигурационные параметры очереди задач.
 *
 * <p>Параметры биндинга читаются из префикса {@code task.queue}.
 */
@Validated
@Getter
@Setter
@ConfigurationProperties(prefix = "task.queue")
public class TaskQueueProperties {

  @Positive private int partitionCount = 20;
  private boolean runtimeEnabled = true;
  @Positive private int workerCount = 2;
  @Positive private int pollBatchSize = 50;

  @NotNull
  @DurationMin(millis = 1)
  private Duration pollInterval = Duration.ofMillis(250);

  @NotNull
  @DurationMin(millis = 1)
  private Duration heartbeatInterval = Duration.ofSeconds(1);

  @NotNull
  @DurationMin(millis = 1)
  private Duration heartbeatTaskTimeout = Duration.ofSeconds(3);

  @NotNull
  @DurationMin(millis = 1)
  private Duration processTimeout = Duration.ofSeconds(10);

  @NotNull
  @DurationMin(millis = 0)
  private Duration heartbeatDeviation = Duration.ofSeconds(3);

  @Positive private int deadProcessTimeoutMultiplier = 3;
  @NotNull private UnregistrationAction unregistrationAction = UnregistrationAction.REREGISTER;
  private boolean stopApplicationOnHeartbeatTimeout = false;

  @NotNull
  @DurationMin(millis = 1)
  private Duration cleanupInterval = Duration.ofSeconds(1);

  @Positive private int cleanupBatchSize = 32;
  @PositiveOrZero private int retryMaxAttempts = 3;
  @NotNull private RetryBackoffStrategy retryBackoffStrategy = RetryBackoffStrategy.EXPONENTIAL;

  @NotNull
  @DurationMin(millis = 1)
  private Duration retryInitialDelay = Duration.ofSeconds(5);

  @NotNull
  @DurationMin(millis = 1)
  private Duration retryMaxDelay = Duration.ofMinutes(5);

  @DecimalMin(value = "0.0", inclusive = false)
  private double retryBackoffMultiplier = 2.0;

  @DecimalMin("0.0")
  @DecimalMax("1.0")
  private double retryJitterFactor = 0.0;

  @NotNull private List<String> retryableExceptions = List.of();
  @NotNull private List<String> notRetryableExceptions = List.of();
  private boolean retryExceptionTraverseCauses = true;
  private boolean retryDefaultRetryable = true;

  @NotNull
  private TaskHandlingTransactionMode handlingTransactionMode =
      TaskHandlingTransactionMode.TRANSACTIONAL;

  private long rebalanceLockKey = 584_231_947_015L;

  /**
   * Проверяет согласованность диапазона задержек retry.
   *
   * @return {@code true}, если максимальная задержка не меньше начальной
   */
  @AssertTrue(message = "retryMaxDelay must be greater than or equal to retryInitialDelay")
  public boolean isRetryDelayRangeValid() {
    if (retryInitialDelay == null || retryMaxDelay == null) {
      return true;
    }
    return !retryMaxDelay.minus(retryInitialDelay).isNegative();
  }
}
