package dev.verkhovskiy.taskqueue.rest;

import org.springframework.http.HttpStatus;

final class TaskQueueRestException extends RuntimeException {

  private final HttpStatus status;
  private final String title;

  private TaskQueueRestException(HttpStatus status, String title, String message) {
    super(message);
    this.status = status;
    this.title = title;
  }

  static TaskQueueRestException badRequest(String message) {
    return new TaskQueueRestException(HttpStatus.BAD_REQUEST, "Bad request", message);
  }

  static TaskQueueRestException notFound(String message) {
    return new TaskQueueRestException(HttpStatus.NOT_FOUND, "Not found", message);
  }

  HttpStatus status() {
    return status;
  }

  String title() {
    return title;
  }
}
