package dev.verkhovskiy.taskqueue.runtime;

import dev.verkhovskiy.taskqueue.config.TaskHandlingTransactionMode;
import dev.verkhovskiy.taskqueue.config.TaskQueueProperties;
import dev.verkhovskiy.taskqueue.config.UnregistrationAction;
import dev.verkhovskiy.taskqueue.domain.QueuedTask;
import dev.verkhovskiy.taskqueue.exception.WorkerRegistrationAlreadyExistsException;
import dev.verkhovskiy.taskqueue.exception.WorkerRegistrationNotFoundException;
import dev.verkhovskiy.taskqueue.handler.TaskHandler;
import dev.verkhovskiy.taskqueue.handler.TaskHandlerRegistry;
import dev.verkhovskiy.taskqueue.metrics.TaskQueueMetrics;
import dev.verkhovskiy.taskqueue.retry.RetryBackoffDecision;
import dev.verkhovskiy.taskqueue.service.TaskExecutionService;
import dev.verkhovskiy.taskqueue.service.TaskQueueService;
import dev.verkhovskiy.taskqueue.service.TaskRetryService;
import dev.verkhovskiy.taskqueue.service.WorkerCoordinationService;
import jakarta.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Рантайм-координатор обработки задач.
 *
 * <p>Отвечает за запуск worker-потоков, регистрацию воркеров, heartbeat, обработку задач и реакцию
 * на отказоустойчивость.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(
    prefix = "task.queue",
    name = "runtime-enabled",
    havingValue = "true",
    matchIfMissing = true)
public class QueueWorkerRuntime {

  private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);
  private static final int HEARTBEAT_TIMEOUT_EXIT_CODE = 220;
  private static final int PROCESS_UNREGISTERED_EXIT_CODE = 230;

  private final TaskQueueProperties properties;
  private final WorkerCoordinationService workerCoordinationService;
  private final TaskQueueService queueService;
  private final TaskRetryService retryService;
  private final TaskHandlerRegistry handlerRegistry;
  private final TaskExecutionService taskExecutionService;
  private final TaskQueueMetrics metrics;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicBoolean destroyTriggered = new AtomicBoolean(false);
  private final long pid = ProcessHandle.current().pid();
  private final String host = resolveHost();

  private ExecutorService workerExecutor;
  private ExecutorService heartbeatExecutor;

  /** Запускает фоновые циклы обработки и heartbeat после старта приложения. */
  @EventListener(ApplicationReadyEvent.class)
  public void start() {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    int workerThreadCount = properties.getWorkerCount() + 1;
    workerExecutor =
        Executors.newFixedThreadPool(
            workerThreadCount,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setDaemon(true);
              thread.setName("task-runtime-worker-" + THREAD_COUNTER.incrementAndGet());
              return thread;
            });
    heartbeatExecutor =
        Executors.newFixedThreadPool(
            Math.max(1, properties.getWorkerCount()),
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setDaemon(true);
              thread.setName("task-runtime-heartbeat-" + THREAD_COUNTER.incrementAndGet());
              return thread;
            });

    IntStream.range(0, properties.getWorkerCount())
        .forEach(workerIndex -> workerExecutor.submit(() -> workerLoop(workerIndex)));
    workerExecutor.submit(this::cleanupLoop);
  }

  /** Останавливает все фоновые executors при завершении приложения. */
  @PreDestroy
  public void stop() {
    if (!running.compareAndSet(true, false)) {
      return;
    }
    if (workerExecutor == null) {
      return;
    }
    workerExecutor.shutdownNow();
    if (heartbeatExecutor != null) {
      heartbeatExecutor.shutdownNow();
    }
    try {
      if (!workerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        log.warn("Task worker runtime was not stopped gracefully within timeout");
      }
      if (heartbeatExecutor != null && !heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        log.warn("Task heartbeat executor was not stopped gracefully within timeout");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Основной цикл воркера: регистрация, выборка задач, обработка и перерегистрация.
   *
   * @param workerIndex индекс воркер-потока в текущем процессе
   */
  private void workerLoop(int workerIndex) {
    WorkerSession session = null;

    while (running.get() && !Thread.currentThread().isInterrupted()) {
      try {
        if (session == null || session.needsReregister()) {
          if (session != null) {
            session.stopHeartbeat();
          }
          session = registerWorkerSession(workerIndex);
        }

        if (session.shouldStopApplication()) {
          destroyApplication(
              session.stopExitCode(),
              "Stopping application because heartbeat policy requested STOP, workerId="
                  + session.workerId(),
              session.stopCause());
        }

        List<QueuedTask> tasks =
            queueService.dequeueForWorker(session.workerId(), properties.getPollBatchSize());

        if (tasks.isEmpty()) {
          sleep(properties.getPollInterval());
          continue;
        }

        for (QueuedTask task : tasks) {
          if (!running.get()
              || Thread.currentThread().isInterrupted()
              || session.needsReregister()
              || session.shouldStopApplication()) {
            break;
          }
          processTask(task);
        }
      } catch (RuntimeException e) {
        log.error("Worker {} loop failed", workerIndex, e);
        sleep(properties.getPollInterval());
      }
    }

    if (session != null) {
      session.stopHeartbeat();
      try {
        workerCoordinationService.unregisterWorker(session.workerId());
      } catch (RuntimeException e) {
        log.warn("Failed to unregister worker {} on shutdown", session.workerId(), e);
      }
    }
  }

  /**
   * Выполняет обработку одной задачи с подтверждением либо retry/finalize при ошибке.
   *
   * @param task задача из очереди
   */
  private void processTask(QueuedTask task) {
    try {
      if (properties.getHandlingTransactionMode() == TaskHandlingTransactionMode.TRANSACTIONAL) {
        taskExecutionService.handleAndAcknowledge(task);
      } else {
        processTaskNonTransactional(task);
      }
    } catch (Exception e) {
      log.error("Task {} failed, taskType={}", task.taskId(), task.taskType(), e);
      try {
        RetryBackoffDecision retryDecision =
            retryService.retryOrFinalize(task.taskId(), task.delayCount(), e);
        logRetryDecision(task, retryDecision, e);
      } catch (RuntimeException retryError) {
        log.error("Failed to schedule retry for task {}", task.taskId(), retryError);
      }
    }
  }

  /**
   * Выполняет обработку задачи без общей транзакции с подтверждением.
   *
   * <p>Хэндлер выполняется вне транзакции библиотеки, подтверждение делается отдельной транзакцией
   * через {@link TaskQueueService#acknowledge(UUID)}.
   *
   * @param task задача из очереди
   * @throws Exception ошибка бизнес-обработки
   */
  private void processTaskNonTransactional(QueuedTask task) throws Exception {
    TaskHandler handler =
        handlerRegistry
            .findByType(task.taskType())
            .orElseThrow(
                () -> new IllegalStateException("No task handler for type " + task.taskType()));
    handler.handle(task);
    queueService.acknowledge(task.taskId());
  }

  /**
   * Логирует результат retry-решения с разделением на retryable и non-retryable сценарии.
   *
   * @param task задача
   * @param retryDecision решение retry-сервиса
   * @param failure исходная ошибка обработки
   */
  private void logRetryDecision(
      QueuedTask task, RetryBackoffDecision retryDecision, Exception failure) {
    if (!retryDecision.shouldRetry()) {
      if (retryDecision.retryableError()) {
        log.error(
            "Task {} exceeded retry attempts (attempt={}), removed from queue",
            task.taskId(),
            retryDecision.nextAttempt());
      } else {
        log.error(
            "Task {} is non-retryable (error={}), removed from queue",
            task.taskId(),
            failure.getClass().getName());
      }
      return;
    }

    log.warn(
        "Task {} scheduled for retry attempt {} after {} ms",
        task.taskId(),
        retryDecision.nextAttempt(),
        retryDecision.delayMillis());
  }

  /** Периодически очищает "мертвые" воркеры и инициирует ребаланс при необходимости. */
  private void cleanupLoop() {
    long reconcileIntervalNanos = Math.max(1L, properties.getHandoffReconcileInterval().toNanos());
    long nextReconcileAt = System.nanoTime();
    while (running.get() && !Thread.currentThread().isInterrupted()) {
      try {
        int cleaned = workerCoordinationService.cleanUpDeadWorkers();
        if (cleaned > 0) {
          log.info("Cleaned {} dead task queue workers", cleaned);
        }
      } catch (RuntimeException e) {
        log.error("Dead worker cleanup failed", e);
      }

      long nowNanos = System.nanoTime();
      if (nowNanos >= nextReconcileAt) {
        try {
          workerCoordinationService.reconcileHandoffs();
        } catch (RuntimeException e) {
          log.error("Handoff reconcile failed", e);
        }
        nextReconcileAt = nowNanos + reconcileIntervalNanos;
      }
      sleep(properties.getCleanupInterval());
    }
  }

  /**
   * Регистрирует воркер-сессию с повторными попытками при коллизии идентификатора.
   *
   * @param workerIndex индекс воркер-потока
   * @return активная сессия воркера
   */
  private WorkerSession registerWorkerSession(int workerIndex) {
    while (running.get() && !Thread.currentThread().isInterrupted()) {
      String workerId = newWorkerId(workerIndex);
      try {
        workerCoordinationService.registerWorker(workerId);
        log.info("Registered queue worker {}", workerId);
        HeartbeatMonitor heartbeatMonitor = new HeartbeatMonitor(workerId);
        heartbeatMonitor.start();
        return new WorkerSession(workerId, heartbeatMonitor);
      } catch (WorkerRegistrationAlreadyExistsException e) {
        log.warn("Generated worker id collision {}, retrying", workerId);
      }
    }
    throw new IllegalStateException("Worker stopped while registering queue worker");
  }

  /**
   * Формирует идентификатор воркера с ограничением длины под поле БД.
   *
   * @param workerIndex индекс воркер-потока
   * @return идентификатор воркера
   */
  private String newWorkerId(int workerIndex) {
    String value =
        "%s-%d-w%d-%s"
            .formatted(host, pid, workerIndex, UUID.randomUUID().toString().substring(0, 8));
    if (value.length() <= 100) {
      return value;
    }
    return value.substring(value.length() - 100);
  }

  /**
   * Приостанавливает текущий поток на заданную длительность.
   *
   * @param duration длительность сна
   */
  private void sleep(Duration duration) {
    try {
      Thread.sleep(Math.max(1, duration.toMillis()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Принудительно останавливает JVM по заданной политике отказа.
   *
   * @param exitCode код завершения процесса
   * @param message сообщение для лога
   * @param cause причина остановки
   */
  private void destroyApplication(int exitCode, String message, Throwable cause) {
    if (!destroyTriggered.compareAndSet(false, true)) {
      return;
    }
    log.error(message, cause);
    Runtime.getRuntime().halt(exitCode);
  }

  /**
   * Определяет имя хоста для формирования workerId.
   *
   * @return очищенное имя хоста либо fallback-значение
   */
  private String resolveHost() {
    try {
      return InetAddress.getLocalHost().getHostName().replaceAll("[^a-zA-Z0-9\\-]", "");
    } catch (UnknownHostException e) {
      return "unknown-host";
    }
  }

  /** Монитор heartbeat конкретного воркера. */
  private final class HeartbeatMonitor {
    private final String workerId;
    private final Thread thread;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private volatile boolean needsReregister = false;
    private volatile boolean shouldStopApplication = false;
    private volatile Throwable stopCause;
    private volatile int stopExitCode = PROCESS_UNREGISTERED_EXIT_CODE;
    private volatile long lastSuccessHeartbeatNanos = 0L;

    /**
     * Создает монитор heartbeat для воркера.
     *
     * @param workerId идентификатор воркера
     */
    private HeartbeatMonitor(String workerId) {
      this.workerId = workerId;
      this.thread = new Thread(this::loop, "queue-heartbeat-" + workerId);
      this.thread.setDaemon(true);
    }

    /** Запускает поток heartbeat-монитора. */
    private void start() {
      lastSuccessHeartbeatNanos = System.nanoTime();
      thread.start();
    }

    /** Останавливает монитор heartbeat. */
    private void stop() {
      cancelled.set(true);
      thread.interrupt();
    }

    private boolean needsReregister() {
      return needsReregister;
    }

    private boolean shouldStopApplication() {
      return shouldStopApplication;
    }

    private Throwable stopCause() {
      return stopCause;
    }

    private int stopExitCode() {
      return stopExitCode;
    }

    /** Цикл heartbeat: проверяет стагнацию и отправляет heartbeat с таймаутом. */
    private void loop() {
      while (running.get() && !cancelled.get() && !Thread.currentThread().isInterrupted()) {
        sleep(properties.getHeartbeatInterval());
        if (!running.get() || cancelled.get() || Thread.currentThread().isInterrupted()) {
          return;
        }

        if (checkHeartbeatStall()) {
          return;
        }
        if (sendHeartbeatWithTimeout()) {
          return;
        }
      }
    }

    /**
     * Проверяет, не "застыл" ли heartbeat слишком надолго.
     *
     * @return {@code true}, если требуется прекратить цикл heartbeat
     */
    private boolean checkHeartbeatStall() {
      long timeoutMillis = properties.getProcessTimeout().toMillis();
      if (timeoutMillis <= 0) {
        return false;
      }
      long elapsedMillis =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastSuccessHeartbeatNanos);
      if (elapsedMillis <= timeoutMillis) {
        return false;
      }

      String message =
          "Last successful heartbeat is stale for worker %s: %d ms"
              .formatted(workerId, elapsedMillis);
      handleUnregisterSignal(message, null, HEARTBEAT_TIMEOUT_EXIT_CODE);
      return true;
    }

    /**
     * Отправляет heartbeat в отдельном executor с ограничением по времени.
     *
     * @return {@code true}, если heartbeat-цикл должен завершиться
     */
    private boolean sendHeartbeatWithTimeout() {
      long startedAt = System.nanoTime();
      Future<?> future =
          heartbeatExecutor.submit(() -> workerCoordinationService.heartbeatWorker(workerId));
      try {
        future.get(properties.getHeartbeatTaskTimeout().toMillis(), TimeUnit.MILLISECONDS);
        lastSuccessHeartbeatNanos = System.nanoTime();
        metrics.heartbeatSuccess(System.nanoTime() - startedAt);
        return false;
      } catch (TimeoutException e) {
        future.cancel(true);
        metrics.heartbeatTimeout();
        String message = "Heartbeat timeout for worker " + workerId;
        if (properties.isStopApplicationOnHeartbeatTimeout()) {
          requestApplicationStop(HEARTBEAT_TIMEOUT_EXIT_CODE, message, e);
        } else {
          handleUnregisterSignal(message, e, HEARTBEAT_TIMEOUT_EXIT_CODE);
        }
        return true;
      } catch (InterruptedException e) {
        future.cancel(true);
        Thread.currentThread().interrupt();
        return true;
      } catch (ExecutionException e) {
        future.cancel(true);
        Throwable cause = e.getCause();
        if (cause instanceof WorkerRegistrationNotFoundException) {
          metrics.heartbeatNotFound();
          handleUnregisterSignal(
              "Heartbeat worker not found for " + workerId, cause, PROCESS_UNREGISTERED_EXIT_CODE);
          return true;
        }
        metrics.heartbeatFailure();
        log.error("Heartbeat failure for worker {}", workerId, cause);
        return false;
      } catch (RuntimeException e) {
        future.cancel(true);
        metrics.heartbeatFailure();
        log.error("Heartbeat runtime failure for worker {}", workerId, e);
        return false;
      }
    }

    /**
     * Реакция на сигнал разрегистрации в зависимости от политики приложения.
     *
     * @param message текст причины
     * @param cause исходное исключение
     * @param exitCode код завершения процесса при STOP-политике
     */
    private void handleUnregisterSignal(String message, Throwable cause, int exitCode) {
      if (properties.getUnregistrationAction() == UnregistrationAction.STOP) {
        requestApplicationStop(exitCode, message, cause);
        return;
      }

      log.warn("{} -> worker will re-register with a new id", message, cause);
      try {
        workerCoordinationService.unregisterWorker(workerId);
      } catch (RuntimeException e) {
        log.warn("Failed to unregister stale worker {} before re-register", workerId, e);
      }
      needsReregister = true;
      cancelled.set(true);
    }

    /**
     * Переводит рантайм в состояние остановки приложения.
     *
     * @param exitCode код завершения процесса
     * @param message сообщение причины
     * @param cause исходное исключение
     */
    private void requestApplicationStop(int exitCode, String message, Throwable cause) {
      shouldStopApplication = true;
      stopCause = cause;
      stopExitCode = exitCode;
      cancelled.set(true);
      destroyApplication(exitCode, message, cause);
    }
  }

  /**
   * Состояние активной воркер-сессии.
   *
   * @param workerId идентификатор воркера
   * @param heartbeatMonitor монитор heartbeat воркера
   */
  private record WorkerSession(String workerId, HeartbeatMonitor heartbeatMonitor) {
    /** Останавливает heartbeat текущей сессии. */
    private void stopHeartbeat() {
      heartbeatMonitor.stop();
    }

    private boolean needsReregister() {
      return heartbeatMonitor.needsReregister();
    }

    private boolean shouldStopApplication() {
      return heartbeatMonitor.shouldStopApplication();
    }

    private Throwable stopCause() {
      return heartbeatMonitor.stopCause();
    }

    private int stopExitCode() {
      return heartbeatMonitor.stopExitCode();
    }
  }
}
