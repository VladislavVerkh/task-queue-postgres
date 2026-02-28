package dev.verkhovskiy.taskqueue.sample.handler;

import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.handler.TaskHandler;
import dev.verkhovskiy.taskqueue.sample.dto.SampleLogTaskPayload;
import dev.verkhovskiy.taskqueue.sample.mapper.SampleTaskPayloadMapper;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Демонстрационный обработчик задач: логирует payload и имитирует обработку через sleep. */
@Slf4j
@Component
@RequiredArgsConstructor
public class SampleLogSleepTaskHandler implements TaskHandler {

  /** Тип задачи, который обрабатывается данным обработчиком. */
  public static final String TASK_TYPE = "sample.log-sleep";

  /** Максимальная задержка одной задачи (миллисекунды) для защиты от ошибочного payload. */
  private static final long MAX_SLEEP_MS = 60_000;

  private final SampleTaskPayloadMapper payloadMapper;

  @Override
  public String taskType() {
    return TASK_TYPE;
  }

  /**
   * Выполняет обработку задачи: парсит JSON, пишет лог и выполняет sleep на заданное время.
   *
   * @param task задача из очереди
   */
  @Override
  public void handle(QueuedTask task) {
    SampleLogTaskPayload payload = payloadMapper.fromJson(task.payload());
    long sleepMs = normalizeSleep(payload.sleepMs());
    UUID taskId = task.taskId();

    log.info(
        "Start processing sample task: taskId={}, partitionKey={}, message='{}', sleepMs={}",
        taskId,
        task.partitionKey(),
        payload.message(),
        sleepMs);

    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Task processing interrupted", e);
    }

    log.info("Finished processing sample task: taskId={}", taskId);
  }

  /**
   * Нормализует задержку: отрицательные значения обнуляются, слишком большие ограничиваются верхней
   * границей.
   *
   * @param requestedSleepMs запрошенная задержка
   * @return безопасная задержка для sleep
   */
  private long normalizeSleep(long requestedSleepMs) {
    if (requestedSleepMs <= 0) {
      return 0;
    }
    return Math.min(requestedSleepMs, MAX_SLEEP_MS);
  }
}
