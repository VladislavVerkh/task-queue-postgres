package dev.verkhovskiy.taskqueue.service;

import java.util.Objects;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** Вычисляет номер партиции для задачи по ключу. */
@Component
public class TaskPartitioner {

  /**
   * Возвращает номер партиции в диапазоне {@code [1..partitionCount]}.
   *
   * @param partitionKey ключ партиционирования (если {@code null}, будет сгенерирован случайный)
   * @param partitionCount количество доступных партиций
   * @return номер партиции
   */
  public int partition(String partitionKey, int partitionCount) {
    if (partitionCount <= 0) {
      throw new IllegalArgumentException("partitionCount must be > 0");
    }
    String key = partitionKey == null ? UUID.randomUUID().toString() : partitionKey;
    return Math.floorMod(Objects.hashCode(key), partitionCount) + 1;
  }
}
