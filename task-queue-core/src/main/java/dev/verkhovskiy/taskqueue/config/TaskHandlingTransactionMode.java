package dev.verkhovskiy.taskqueue.config;

/** Режим выполнения хэндлера задачи относительно транзакции. */
public enum TaskHandlingTransactionMode {

  /** Бизнес-обработка и {@code ack} выполняются в одной транзакции. */
  TRANSACTIONAL,

  /**
   * Бизнес-обработка выполняется вне транзакции библиотеки, подтверждение {@code ack} выполняется
   * отдельной транзакцией.
   */
  NON_TRANSACTIONAL
}
