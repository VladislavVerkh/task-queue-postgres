package dev.verkhovskiy.taskqueue.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.verkhovskiy.taskqueue.config.RetryBackoffStrategy;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Тесты вычисления задержек retry по backoff-политике. */
class RetryBackoffPolicyTest {

  @Test
  void exponentialBackoffIsCappedByMaxDelay() {
    TaskQueueProperties properties = defaultProperties();
    properties.setRetryBackoffStrategy(RetryBackoffStrategy.EXPONENTIAL);
    properties.setRetryInitialDelay(Duration.ofSeconds(2));
    properties.setRetryBackoffMultiplier(2.0);
    properties.setRetryMaxDelay(Duration.ofSeconds(5));
    properties.setRetryJitterFactor(0.0);
    properties.setRetryMaxAttempts(5);

    RetryBackoffPolicy policy = new RetryBackoffPolicy(properties);

    assertEquals(2_000, policy.nextRetry(0).delayMillis());
    assertEquals(4_000, policy.nextRetry(1).delayMillis());
    assertEquals(5_000, policy.nextRetry(2).delayMillis());
  }

  @Test
  void fixedBackoffUsesConstantDelay() {
    TaskQueueProperties properties = defaultProperties();
    properties.setRetryBackoffStrategy(RetryBackoffStrategy.FIXED);
    properties.setRetryInitialDelay(Duration.ofSeconds(3));
    properties.setRetryBackoffMultiplier(10.0);
    properties.setRetryJitterFactor(0.0);
    properties.setRetryMaxAttempts(3);

    RetryBackoffPolicy policy = new RetryBackoffPolicy(properties);

    assertEquals(3_000, policy.nextRetry(0).delayMillis());
    assertEquals(3_000, policy.nextRetry(1).delayMillis());
    assertEquals(3_000, policy.nextRetry(2).delayMillis());
  }

  @Test
  void doesNotRetryAfterMaxAttempts() {
    TaskQueueProperties properties = defaultProperties();
    properties.setRetryMaxAttempts(2);
    properties.setRetryJitterFactor(0.0);

    RetryBackoffPolicy policy = new RetryBackoffPolicy(properties);

    RetryBackoffDecision first = policy.nextRetry(0);
    RetryBackoffDecision second = policy.nextRetry(1);
    RetryBackoffDecision third = policy.nextRetry(2);

    assertTrue(first.shouldRetry());
    assertTrue(second.shouldRetry());
    assertFalse(third.shouldRetry());
    assertEquals(3, third.nextAttempt());
  }

  @Test
  void zeroMaxAttemptsDisablesRetry() {
    TaskQueueProperties properties = defaultProperties();
    properties.setRetryMaxAttempts(0);

    RetryBackoffPolicy policy = new RetryBackoffPolicy(properties);
    RetryBackoffDecision decision = policy.nextRetry(0);

    assertFalse(decision.shouldRetry());
    assertEquals(1, decision.nextAttempt());
  }

  @Test
  void jitterStaysWithinConfiguredRange() {
    TaskQueueProperties properties = defaultProperties();
    properties.setRetryBackoffStrategy(RetryBackoffStrategy.FIXED);
    properties.setRetryInitialDelay(Duration.ofSeconds(1));
    properties.setRetryMaxDelay(Duration.ofSeconds(1));
    properties.setRetryJitterFactor(0.5);

    RetryBackoffPolicy policy = new RetryBackoffPolicy(properties);
    long delay = policy.nextRetry(0).delayMillis();

    assertTrue(delay >= 500);
    assertTrue(delay <= 1_500);
  }

  private static TaskQueueProperties defaultProperties() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryBackoffStrategy(RetryBackoffStrategy.EXPONENTIAL);
    properties.setRetryInitialDelay(Duration.ofSeconds(1));
    properties.setRetryMaxDelay(Duration.ofMinutes(1));
    properties.setRetryBackoffMultiplier(2.0);
    properties.setRetryJitterFactor(0.0);
    properties.setRetryMaxAttempts(3);
    return properties;
  }
}
