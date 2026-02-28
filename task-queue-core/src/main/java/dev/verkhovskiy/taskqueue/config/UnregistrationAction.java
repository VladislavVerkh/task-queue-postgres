package dev.verkhovskiy.taskqueue.config;

/** Действие при обнаружении проблем с heartbeat/регистрацией воркера. */
public enum UnregistrationAction {
  /** Остановить процесс приложения. */
  STOP,
  /** Разрегистрировать воркер и зарегистрировать его заново с новым идентификатором. */
  REREGISTER
}
