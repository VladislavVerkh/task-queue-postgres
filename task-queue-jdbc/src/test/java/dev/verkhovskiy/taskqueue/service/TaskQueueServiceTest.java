package dev.verkhovskiy.taskqueue.service;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.domain.TaskEnqueueRequest;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository;
import dev.verkhovskiy.taskqueue.persistence.WorkerRegistryRepository;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TaskQueueServiceTest {

  @Mock private TaskQueueRepository queueRepository;
  @Mock private WorkerRegistryRepository workerRegistryRepository;
  @Mock private TaskPartitioner partitioner;
  @Mock private TaskIdGenerator taskIdGenerator;

  private TaskQueueProperties properties;
  private TaskQueueService queueService;

  @BeforeEach
  void setUp() {
    properties = new TaskQueueProperties();
    queueService =
        new TaskQueueService(
            queueRepository, workerRegistryRepository, partitioner, properties, taskIdGenerator);
  }

  @Test
  void rejectsTooLongTaskTypeBeforeSql() {
    TaskEnqueueRequest request = new TaskEnqueueRequest("x".repeat(129), "key", "{}", null);

    assertThatThrownBy(() -> queueService.enqueue(request))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("taskType length must be <= 128");

    verifyNoInteractions(queueRepository, partitioner, taskIdGenerator);
  }

  @Test
  void rejectsTooLongPartitionKeyBeforeSql() {
    TaskEnqueueRequest request = new TaskEnqueueRequest("type", "x".repeat(513), "{}", null);

    assertThatThrownBy(() -> queueService.enqueue(request))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("partitionKey length must be <= 512");

    verifyNoInteractions(queueRepository, partitioner, taskIdGenerator);
  }

  @Test
  void rejectsNonPositiveMaxCountBeforeSql() {
    assertThatThrownBy(() -> queueService.dequeueForWorker("worker-1", 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxCount must be greater than 0");

    verifyNoInteractions(workerRegistryRepository, queueRepository);
  }

  @Test
  void rejectsNegativeAvailableAfterBeforeSql() {
    TaskEnqueueRequest request = new TaskEnqueueRequest("type", "key", "{}", null);

    assertThatThrownBy(() -> queueService.enqueue(request, Duration.ofMillis(-1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("availableAfter must be greater than or equal to 0");

    verifyNoInteractions(queueRepository, partitioner, taskIdGenerator);
  }

  @Test
  void enqueuesDelayRelativeToDatabaseTime() {
    UUID taskId = UUID.fromString("018f0000-0000-7000-8000-000000000001");
    Duration delay = Duration.ofSeconds(5);
    TaskEnqueueRequest request = new TaskEnqueueRequest("type", "key", "{}", null);
    when(taskIdGenerator.next()).thenReturn(taskId);
    when(partitioner.partition("key", properties.getPartitionCount())).thenReturn(7);

    queueService.enqueue(request, delay);

    verify(queueRepository).enqueue(taskId, "type", "{}", "key", 7, null, delay);
  }
}
