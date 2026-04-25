package dev.verkhovskiy.taskqueue.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.exception.PartitionCountMismatchException;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetadataRepository;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskQueuePartitionGuardTest {

  @Mock private TaskQueueMetadataRepository metadataRepository;

  private TaskQueueProperties properties;
  private TaskQueuePartitionGuard guard;

  @BeforeEach
  void setUp() {
    properties = new TaskQueueProperties();
    properties.setPartitionCount(20);
    guard = new TaskQueuePartitionGuard(metadataRepository, properties);
  }

  @Test
  void storesPartitionCountOnFirstStartup() {
    when(metadataRepository.insertIfAbsent("partition-count", "20")).thenReturn(true);

    guard.validatePartitionCount();

    verify(metadataRepository, never()).findValue("partition-count");
  }

  @Test
  void acceptsExistingMatchingPartitionCount() {
    when(metadataRepository.insertIfAbsent("partition-count", "20")).thenReturn(false);
    when(metadataRepository.findValue("partition-count")).thenReturn(Optional.of("20"));

    guard.validatePartitionCount();
  }

  @Test
  void failsFastWhenPartitionCountChanged() {
    when(metadataRepository.insertIfAbsent("partition-count", "20")).thenReturn(false);
    when(metadataRepository.findValue("partition-count")).thenReturn(Optional.of("10"));

    assertThrows(PartitionCountMismatchException.class, () -> guard.validatePartitionCount());
  }
}
