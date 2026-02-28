package dev.verkhovskiy.taskqueue.service;

import com.github.f4b6a3.uuid.UuidCreator;
import java.util.UUID;

/**
 * Генерирует UUID версии 7 для идентификаторов задач.
 *
 * <p>Это единая точка изменения формата task-id.
 */
public class UuidV7TaskIdGenerator implements TaskIdGenerator {

  @Override
  public UUID next() {
    return UuidCreator.getTimeOrderedEpoch();
  }
}
