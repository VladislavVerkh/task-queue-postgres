package dev.verkhovskiy.taskqueue.retry;

/**
 * Результат решения о повторной попытке обработки задачи.
 *
 * @param shouldRetry признак необходимости повторить задачу
 * @param nextAttempt номер следующей попытки
 * @param delayMillis задержка до следующей попытки в миллисекундах
 * @param retryableError признак, что ошибка относится к retryable-классу
 */
public record RetryBackoffDecision(
    boolean shouldRetry, int nextAttempt, long delayMillis, boolean retryableError) {

  /**
   * Создает решение "retry исчерпан".
   *
   * @param nextAttempt номер попытки, на которой retry прекращен
   * @return решение без повторной попытки
   */
  public static RetryBackoffDecision noRetry(int nextAttempt) {
    return new RetryBackoffDecision(false, nextAttempt, 0, true);
  }

  /**
   * Создает решение "ошибка неретраибельна".
   *
   * @param nextAttempt номер логической попытки
   * @return решение без повторной попытки для non-retryable ошибки
   */
  public static RetryBackoffDecision nonRetryable(int nextAttempt) {
    return new RetryBackoffDecision(false, nextAttempt, 0, false);
  }

  /**
   * Создает решение с повторной попыткой после заданной задержки.
   *
   * @param nextAttempt номер следующей попытки
   * @param delayMillis задержка до следующей попытки в миллисекундах
   * @return решение о повторной попытке
   */
  public static RetryBackoffDecision retryAfter(int nextAttempt, long delayMillis) {
    return new RetryBackoffDecision(true, nextAttempt, delayMillis, true);
  }
}
