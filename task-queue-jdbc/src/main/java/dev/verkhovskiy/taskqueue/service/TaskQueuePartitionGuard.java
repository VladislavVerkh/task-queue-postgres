package dev.verkhovskiy.taskqueue.service;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.exception.PartitionCountMismatchException;
import dev.verkhovskiy.taskqueue.persistence.TaskQueueMetadataRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Service;

/** Проверяет, что количество партиций не изменилось без миграции. */
@Service
@RequiredArgsConstructor
public class TaskQueuePartitionGuard implements SmartInitializingSingleton {

  static final String PARTITION_COUNT_METADATA_KEY = "partition-count";

  private final TaskQueueMetadataRepository metadataRepository;
  private final TaskQueueProperties properties;

  /** Выполняет fail-fast проверку metadata после инициализации контекста. */
  @Override
  public void afterSingletonsInstantiated() {
    validatePartitionCount();
  }

  /** Сохраняет partition-count при первом запуске и проверяет его при последующих стартах. */
  public void validatePartitionCount() {
    String configuredPartitionCount = Integer.toString(properties.getPartitionCount());
    if (metadataRepository.insertIfAbsent(PARTITION_COUNT_METADATA_KEY, configuredPartitionCount)) {
      return;
    }

    String storedPartitionCount =
        metadataRepository
            .findValue(PARTITION_COUNT_METADATA_KEY)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "task_queue_metadata does not contain partition-count"));
    if (!configuredPartitionCount.equals(storedPartitionCount)) {
      throw new PartitionCountMismatchException(storedPartitionCount, configuredPartitionCount);
    }
  }
}
