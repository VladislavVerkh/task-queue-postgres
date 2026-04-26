package dev.verkhovskiy.taskqueue.domain;

/**
 * Ограничения публичной модели task-queue, синхронизированные со схемой PostgreSQL.
 *
 * <p>Значения должны совпадать с размером колонок {@code task_queue}/{@code
 * task_queue_dead_letter}.
 */
public final class TaskQueueLimits {

  /** Максимальная длина типа задачи: {@code task_type varchar(128)}. */
  public static final int MAX_TASK_TYPE_LENGTH = 128;

  /** Максимальная длина ключа партиционирования: {@code partition_key varchar(512)}. */
  public static final int MAX_PARTITION_KEY_LENGTH = 512;

  private TaskQueueLimits() {}
}
