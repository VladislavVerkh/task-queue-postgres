package dev.verkhovskiy.taskqueue.config;

/** Стратегия расчета задержки между попытками обработки задачи. */
public enum RetryBackoffStrategy {
  /** Фиксированная задержка для каждой попытки. */
  FIXED,
  /** Экспоненциальный рост задержки между попытками. */
  EXPONENTIAL
}
