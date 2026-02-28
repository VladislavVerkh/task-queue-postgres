package dev.verkhovskiy.taskqueue.sample.api;

import dev.verkhovskiy.taskqueue.sample.dto.EnqueueLogSleepTaskRequest;
import dev.verkhovskiy.taskqueue.sample.dto.EnqueueLogSleepTaskResponse;
import dev.verkhovskiy.taskqueue.sample.dto.SampleLogTaskPayload;
import dev.verkhovskiy.taskqueue.sample.handler.SampleLogSleepTaskHandler;
import dev.verkhovskiy.taskqueue.sample.mapper.SampleTaskPayloadMapper;
import dev.verkhovskiy.taskqueue.service.TaskProducer;
import jakarta.validation.Valid;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** REST API постановки демонстрационных задач в очередь. */
@RestController
@RequestMapping("/api/tasks")
@RequiredArgsConstructor
public class SampleTaskEnqueueController {

  /** Значение sleep по умолчанию для demo-задачи. */
  private static final long DEFAULT_SLEEP_MS = 1_000;

  private final TaskProducer taskProducer;
  private final SampleTaskPayloadMapper payloadMapper;

  /**
   * Создает задачу типа {@code sample.log-sleep}.
   *
   * @param request тело запроса
   * @return идентификатор созданной задачи
   */
  @PostMapping("/log-sleep")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public EnqueueLogSleepTaskResponse enqueueLogSleepTask(
      @RequestBody @Valid EnqueueLogSleepTaskRequest request) {
    SampleLogTaskPayload payload =
        new SampleLogTaskPayload(
            request.message(), request.sleepMs() == null ? DEFAULT_SLEEP_MS : request.sleepMs());

    String payloadJson = payloadMapper.toJson(payload);
    UUID taskId =
        request.availableAt() == null
            ? taskProducer.enqueue(
                SampleLogSleepTaskHandler.TASK_TYPE, request.partitionKey(), payloadJson)
            : taskProducer.enqueue(
                SampleLogSleepTaskHandler.TASK_TYPE,
                request.partitionKey(),
                payloadJson,
                request.availableAt());

    return new EnqueueLogSleepTaskResponse(taskId);
  }
}
