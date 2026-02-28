package dev.verkhovskiy.taskqueue.handler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/** Реестр обработчиков задач, индексированный по типу задачи. */
@Component
public class TaskHandlerRegistry {

  private final Map<String, TaskHandler> handlersByType;

  /**
   * Создает реестр обработчиков и валидирует отсутствие дублирующих типов.
   *
   * @param handlers список зарегистрированных обработчиков
   */
  public TaskHandlerRegistry(List<TaskHandler> handlers) {
    this.handlersByType =
        handlers.stream()
            .collect(
                Collectors.toMap(
                    TaskHandler::taskType,
                    Function.identity(),
                    TaskHandlerRegistry::throwDuplicateHandler));
  }

  /**
   * Ищет обработчик по типу задачи.
   *
   * @param taskType тип задачи
   * @return обработчик, если найден
   */
  public Optional<TaskHandler> findByType(String taskType) {
    return Optional.ofNullable(handlersByType.get(taskType));
  }

  /**
   * Генерирует исключение при попытке зарегистрировать два обработчика одного типа.
   *
   * @param left первый обработчик
   * @param right второй обработчик
   * @return не возвращает значение, всегда выбрасывает исключение
   */
  private static TaskHandler throwDuplicateHandler(TaskHandler left, TaskHandler right) {
    throw new IllegalStateException(
        "Duplicate task handler for type '%s'".formatted(left.taskType()));
  }
}
