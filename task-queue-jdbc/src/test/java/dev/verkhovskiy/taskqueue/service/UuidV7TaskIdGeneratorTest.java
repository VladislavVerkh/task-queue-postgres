package dev.verkhovskiy.taskqueue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class UuidV7TaskIdGeneratorTest {

  private final UuidV7TaskIdGenerator generator = new UuidV7TaskIdGenerator();

  @Test
  void nextReturnsUuidVersion7WithRfcVariant() {
    UUID taskId = generator.next();

    assertEquals(7, taskId.version());
    assertEquals(2, taskId.variant());
  }

  @Test
  void nextReturnsDifferentIds() {
    UUID first = generator.next();
    UUID second = generator.next();

    assertNotEquals(first, second);
  }
}
