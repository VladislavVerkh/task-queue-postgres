package dev.verkhovskiy.taskqueue.config;

/**
 * Политика обработки таймаута дренажа партиции при handoff.
 *
 * <p>Используется, когда партиция находится в состоянии DRAINING дольше {@code
 * handoffDrainTimeout}.
 */
public enum HandoffTimeoutAction {
  /** Продлить дедлайн дренажа и продолжать ожидание завершения in-flight задач. */
  EXTEND,
  /** Отменить handoff и вернуть партицию в ACTIVE у текущего владельца. */
  ABORT,
  /**
   * Принудительно передать партицию pending-владельцу, даже если у старого владельца еще есть
   * in-flight задачи.
   */
  FORCE
}
