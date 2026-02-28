package dev.verkhovskiy.taskqueue.retry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Тесты классификатора retryable/non-retryable исключений. */
class RetryExceptionClassifierTest {

  @Test
  void matchesNotRetryableFirst() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryableExceptions(List.of(RuntimeException.class.getName()));
    properties.setNotRetryableExceptions(List.of(IllegalStateException.class.getName()));
    properties.setRetryDefaultRetryable(true);

    RetryExceptionClassifier classifier = new RetryExceptionClassifier(properties);

    assertFalse(classifier.isRetryable(new IllegalStateException("boom")));
  }

  @Test
  void matchesRetryableWhenDefaultIsFalse() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryableExceptions(List.of(IOException.class.getName()));
    properties.setRetryDefaultRetryable(false);

    RetryExceptionClassifier classifier = new RetryExceptionClassifier(properties);

    assertTrue(classifier.isRetryable(new IOException("temporary")));
    assertFalse(classifier.isRetryable(new IllegalArgumentException("bad input")));
  }

  @Test
  void traversesCauseWhenEnabled() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryableExceptions(List.of(IOException.class.getName()));
    properties.setRetryDefaultRetryable(false);
    properties.setRetryExceptionTraverseCauses(true);

    RetryExceptionClassifier classifier = new RetryExceptionClassifier(properties);
    RuntimeException wrapper = new RuntimeException(new IOException("io"));

    assertTrue(classifier.isRetryable(wrapper));
  }

  @Test
  void doesNotTraverseCauseWhenDisabled() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryableExceptions(List.of(IOException.class.getName()));
    properties.setRetryDefaultRetryable(false);
    properties.setRetryExceptionTraverseCauses(false);

    RetryExceptionClassifier classifier = new RetryExceptionClassifier(properties);
    RuntimeException wrapper = new RuntimeException(new IOException("io"));

    assertFalse(classifier.isRetryable(wrapper));
  }

  @Test
  void rejectsUnknownClassInConfiguration() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryableExceptions(List.of("com.example.UnknownException"));

    assertThrows(IllegalArgumentException.class, () -> new RetryExceptionClassifier(properties));
  }

  @Test
  void rejectsNonThrowableClassInConfiguration() {
    TaskQueueProperties properties = new TaskQueueProperties();
    properties.setRetryableExceptions(List.of(String.class.getName()));

    assertThrows(IllegalArgumentException.class, () -> new RetryExceptionClassifier(properties));
  }
}
