package dev.verkhovskiy.taskqueue.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Точка входа в sample-приложение, использующее библиотеку очереди задач. */
@SpringBootApplication
public class TaskQueueSampleApplication {

  /**
   * Запускает Spring Boot приложение.
   *
   * @param args аргументы командной строки
   */
  public static void main(String[] args) {
    SpringApplication.run(TaskQueueSampleApplication.class, args);
  }
}
