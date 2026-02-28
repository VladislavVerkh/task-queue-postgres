package dev.verkhovskiy.taskqueue.sample.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.verkhovskiy.taskqueue.sample.dto.SampleLogTaskPayload;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Маппер JSON payload очереди в типизированный DTO демонстрационной задачи. */
@Component
@RequiredArgsConstructor
public class SampleTaskPayloadMapper {

  private final ObjectMapper objectMapper;

  /**
   * Преобразует JSON payload в {@link SampleLogTaskPayload}.
   *
   * @param payload json-строка из очереди
   * @return распарсенный DTO
   */
  public SampleLogTaskPayload fromJson(String payload) {
    try {
      return objectMapper.readValue(payload, SampleLogTaskPayload.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid task payload json", e);
    }
  }

  /**
   * Сериализует DTO задачи в JSON-строку для постановки в очередь.
   *
   * @param payload dto полезной нагрузки
   * @return json-представление dto
   */
  public String toJson(SampleLogTaskPayload payload) {
    try {
      return objectMapper.writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize task payload", e);
    }
  }
}
