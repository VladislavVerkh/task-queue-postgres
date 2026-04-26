package dev.verkhovskiy.taskqueue.rest;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice(assignableTypes = TaskQueueAdminController.class)
public class TaskQueueRestExceptionHandler {

  @ExceptionHandler(TaskQueueRestException.class)
  public ResponseEntity<ProblemDetail> handleTaskQueueRestException(
      TaskQueueRestException exception) {
    return problem(exception.status(), exception.title(), exception.getMessage());
  }

  @ExceptionHandler({
    HttpMessageNotReadableException.class,
    MethodArgumentTypeMismatchException.class,
    MissingServletRequestParameterException.class,
    MethodArgumentNotValidException.class,
    IllegalArgumentException.class
  })
  public ResponseEntity<ProblemDetail> handleBadRequest(Exception exception) {
    return problem(HttpStatus.BAD_REQUEST, "Bad request", exception.getMessage());
  }

  @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
  public ResponseEntity<ProblemDetail> handleMethodNotAllowed(Exception exception) {
    return problem(HttpStatus.METHOD_NOT_ALLOWED, "Method not allowed", exception.getMessage());
  }

  @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
  public ResponseEntity<ProblemDetail> handleUnsupportedMediaType(Exception exception) {
    return problem(
        HttpStatus.UNSUPPORTED_MEDIA_TYPE, "Unsupported media type", exception.getMessage());
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ProblemDetail> handleUnexpected(Exception exception) {
    return problem(
        HttpStatus.INTERNAL_SERVER_ERROR,
        "Internal server error",
        "Task queue REST request failed");
  }

  private static ResponseEntity<ProblemDetail> problem(
      HttpStatus status, String title, String detail) {
    ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
    problem.setTitle(title);
    return ResponseEntity.status(status)
        .contentType(MediaType.APPLICATION_PROBLEM_JSON)
        .body(problem);
  }
}
