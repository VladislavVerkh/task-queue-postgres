package dev.verkhovskiy.taskqueue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** Тесты расчета партиции по ключу задачи. */
class TaskPartitionerTest {

  private final TaskPartitioner partitioner = new TaskPartitioner();

  @Test
  void returnsSamePartitionForSameKey() {
    int partitionOne = partitioner.partition("taxi:123", 20);
    int partitionTwo = partitioner.partition("taxi:123", 20);

    assertEquals(partitionOne, partitionTwo);
  }

  @Test
  void returnsPartitionInRange() {
    IntStream.range(0, 100)
        .forEach(
            i -> {
              int partition = partitioner.partition("key-" + i, 20);
              assertTrue(partition >= 1 && partition <= 20);
            });
  }
}
