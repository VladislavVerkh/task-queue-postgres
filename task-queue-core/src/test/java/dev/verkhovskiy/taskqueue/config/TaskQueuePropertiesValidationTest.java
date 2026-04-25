package dev.verkhovskiy.taskqueue.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Тесты bean-validation ограничений для настроек очереди задач. */
class TaskQueuePropertiesValidationTest {

  @Test
  void validDefaultsPassValidation() {
    TaskQueueProperties properties = new TaskQueueProperties();

    Set<ConstraintViolation<TaskQueueProperties>> violations = validate(properties);

    assertTrue(violations.isEmpty());
  }

  @Test
  void rejectsRetryJitterFactorOutOfRange() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryJitterFactor(1.2);

    Set<ConstraintViolation<TaskQueueProperties>> violations = validate(properties);

    assertEquals(1, violations.size());
    assertEquals("retryJitterFactor", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void rejectsNonPositiveBackoffMultiplier() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryBackoffMultiplier(0.0);

    Set<ConstraintViolation<TaskQueueProperties>> violations = validate(properties);

    assertEquals(1, violations.size());
    assertEquals(
        "retryBackoffMultiplier", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void rejectsRetryMaxDelayBelowInitialDelay() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryInitialDelay(Duration.ofSeconds(5));
    properties.setRetryMaxDelay(Duration.ofSeconds(1));

    Set<ConstraintViolation<TaskQueueProperties>> violations = validate(properties);

    assertEquals(1, violations.size());
    assertEquals("retryDelayRangeValid", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void rejectsNonPositiveHandoffDrainTimeout() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setHandoffDrainTimeout(Duration.ZERO);

    Set<ConstraintViolation<TaskQueueProperties>> violations = validate(properties);

    assertEquals(1, violations.size());
    assertEquals("handoffDrainTimeout", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void rejectsNonPositiveTaskLeaseTimeout() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setTaskLeaseTimeout(Duration.ZERO);

    Set<ConstraintViolation<TaskQueueProperties>> violations = validate(properties);

    assertEquals(1, violations.size());
    assertEquals("taskLeaseTimeout", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void rejectsNonPositiveShutdownTimeout() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setShutdownTimeout(Duration.ZERO);

    Set<ConstraintViolation<TaskQueueProperties>> violations = validate(properties);

    assertEquals(1, violations.size());
    assertEquals("shutdownTimeout", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void rejectsNonPositiveQueueMetricsInterval() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setQueueMetricsInterval(Duration.ZERO);

    Set<ConstraintViolation<TaskQueueProperties>> violations = validate(properties);

    assertEquals(1, violations.size());
    assertEquals("queueMetricsInterval", violations.iterator().next().getPropertyPath().toString());
  }

  @Test
  void rejectsNonPositiveHandoffReconcileInterval() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setHandoffReconcileInterval(Duration.ZERO);

    Set<ConstraintViolation<TaskQueueProperties>> violations = validate(properties);

    assertEquals(1, violations.size());
    assertEquals(
        "handoffReconcileInterval", violations.iterator().next().getPropertyPath().toString());
  }

  private static Set<ConstraintViolation<TaskQueueProperties>> validate(
      TaskQueueProperties properties) {
    try (var validatorFactory = Validation.buildDefaultValidatorFactory()) {
      return validatorFactory.getValidator().validate(properties);
    }
  }
}
