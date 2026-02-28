package dev.verkhovskiy.taskqueue.service;

import java.util.UUID;

/** Стратегия генерации идентификаторов задач. */
@FunctionalInterface
public interface TaskIdGenerator {

  /** Возвращает новый идентификатор задачи. */
  UUID next();
}
