package dev.verkhovskiy.taskqueue.retry;

import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

/**
 * Классификатор исключений на retryable и non-retryable.
 *
 * <p>Логика совместима по смыслу с kafka-style классификацией: приоритет non-retryable выше
 * retryable, опционально учитывается цепочка причин.
 */
@Component
public class RetryExceptionClassifier {

  private final List<Class<? extends Throwable>> retryableExceptions;
  private final List<Class<? extends Throwable>> notRetryableExceptions;
  private final boolean traverseCauses;
  private final boolean defaultRetryable;

  /**
   * Создает классификатор на основе конфигурации приложения.
   *
   * @param properties параметры очереди задач
   */
  public RetryExceptionClassifier(TaskQueueProperties properties) {
    this.retryableExceptions =
        resolveExceptionClasses(
            properties.getRetryableExceptions(), "task.queue.retryable-exceptions");
    this.notRetryableExceptions =
        resolveExceptionClasses(
            properties.getNotRetryableExceptions(), "task.queue.not-retryable-exceptions");
    this.traverseCauses = properties.isRetryExceptionTraverseCauses();
    this.defaultRetryable = properties.isRetryDefaultRetryable();
  }

  /**
   * Определяет, можно ли повторять задачу после указанной ошибки.
   *
   * @param throwable исходная ошибка обработки
   * @return {@code true}, если ошибка считается retryable
   */
  public boolean isRetryable(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (matches(current, notRetryableExceptions)) {
        return false;
      }
      if (matches(current, retryableExceptions)) {
        return true;
      }
      current = traverseCauses ? current.getCause() : null;
    }
    return defaultRetryable;
  }

  /**
   * Проверяет, соответствует ли ошибка хотя бы одному типу из списка.
   *
   * @param throwable ошибка
   * @param exceptionTypes список типов исключений
   * @return {@code true}, если найдено совпадение по иерархии типов
   */
  private static boolean matches(
      Throwable throwable, List<Class<? extends Throwable>> exceptionTypes) {
    return exceptionTypes.stream()
        .anyMatch(exceptionType -> exceptionType.isAssignableFrom(throwable.getClass()));
  }

  /**
   * Преобразует список имен классов исключений в список классов.
   *
   * @param classNames имена классов
   * @param propertyName имя свойства для сообщений об ошибке
   * @return неизменяемый список классов исключений
   */
  private static List<Class<? extends Throwable>> resolveExceptionClasses(
      List<String> classNames, String propertyName) {
    return classNames.stream()
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .map(value -> resolveExceptionClass(value, propertyName))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Разрешает имя класса в тип исключения и валидирует принадлежность к {@link Throwable}.
   *
   * @param value имя класса
   * @param propertyName имя свойства для сообщений об ошибке
   * @return класс исключения
   */
  private static Class<? extends Throwable> resolveExceptionClass(
      String value, String propertyName) {
    try {
      Class<?> resolved =
          ClassUtils.forName(value, RetryExceptionClassifier.class.getClassLoader());
      if (!Throwable.class.isAssignableFrom(resolved)) {
        throw new IllegalArgumentException(
            "Property '" + propertyName + "' contains non-Throwable type: " + value);
      }
      return resolved.asSubclass(Throwable.class);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          "Property '" + propertyName + "' contains unknown class: " + value, e);
    }
  }
}
