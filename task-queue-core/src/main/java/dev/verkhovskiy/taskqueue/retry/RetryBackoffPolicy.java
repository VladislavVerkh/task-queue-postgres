package dev.verkhovskiy.taskqueue.retry;

import dev.verkhovskiy.taskqueue.config.RetryBackoffStrategy;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import io.github.resilience4j.core.IntervalFunction;
import org.springframework.stereotype.Component;

/** Политика расчета backoff-задержки и лимита попыток retry. */
@Component
public record RetryBackoffPolicy(TaskQueueProperties properties) {

  private static final long MIN_DELAY_MILLIS = 1;

  /**
   * Рассчитывает решение о следующей попытке обработки.
   *
   * @param alreadyRetriedCount количество уже выполненных retry
   * @return решение о retry с задержкой или завершении retry
   */
  public RetryBackoffDecision nextRetry(long alreadyRetriedCount) {
    int maxAttempts = properties.getRetryMaxAttempts();
    long nextAttemptLong =
        alreadyRetriedCount == Long.MAX_VALUE ? Long.MAX_VALUE : alreadyRetriedCount + 1;
    if (nextAttemptLong > maxAttempts) {
      return RetryBackoffDecision.noRetry(toIntSaturated(nextAttemptLong));
    }
    int nextAttempt = (int) nextAttemptLong;
    long delayMillis = computeDelayMillis(nextAttempt);
    return RetryBackoffDecision.retryAfter(nextAttempt, delayMillis);
  }

  /**
   * Вычисляет задержку для конкретной попытки с учетом стратегии, jitter и верхней границы.
   *
   * @param nextAttempt номер следующей попытки
   * @return задержка в миллисекундах
   */
  private long computeDelayMillis(int nextAttempt) {
    long initialDelayMillis =
        Math.max(MIN_DELAY_MILLIS, properties.getRetryInitialDelay().toMillis());
    long maxDelayMillis = properties.getRetryMaxDelay().toMillis();
    double jitterFactor = properties.getRetryJitterFactor();

    IntervalFunction intervalFunction = buildIntervalFunction(initialDelayMillis, jitterFactor);
    long delayMillis = Math.max(MIN_DELAY_MILLIS, intervalFunction.apply(nextAttempt));
    return Math.min(delayMillis, maxDelayMillis);
  }

  /**
   * Создает функцию интервала для выбранной стратегии backoff.
   *
   * @param initialDelayMillis начальная задержка
   * @param jitterFactor коэффициент случайного jitter
   * @return функция расчета интервала между попытками
   */
  private IntervalFunction buildIntervalFunction(long initialDelayMillis, double jitterFactor) {
    RetryBackoffStrategy strategy = properties.getRetryBackoffStrategy();
    return switch (strategy) {
      case FIXED ->
          jitterFactor > 0
              ? IntervalFunction.ofRandomized(initialDelayMillis, jitterFactor)
              : IntervalFunction.of(initialDelayMillis);
      case EXPONENTIAL -> {
        double multiplier = properties.getRetryBackoffMultiplier();
        yield jitterFactor > 0
            ? IntervalFunction.ofExponentialRandomBackoff(
                initialDelayMillis, multiplier, jitterFactor)
            : IntervalFunction.ofExponentialBackoff(initialDelayMillis, multiplier);
      }
    };
  }

  /**
   * Безопасно приводит номер попытки к {@code int} с насыщением по верхней границе.
   *
   * @param value номер попытки в формате {@code long}
   * @return значение в диапазоне {@code int}
   */
  private static int toIntSaturated(long value) {
    return value > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
  }
}
