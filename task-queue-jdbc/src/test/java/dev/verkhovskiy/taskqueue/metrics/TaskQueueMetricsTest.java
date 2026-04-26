package dev.verkhovskiy.taskqueue.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.verkhovskiy.taskqueue.persistence.TaskQueueRepository.PartitionLagMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.junit.jupiter.api.Test;

class TaskQueueMetricsTest {

  @Test
  void setPartitionLagResetsMissingPartitionGaugesToZero() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    TaskQueueMetrics metrics = new TaskQueueMetrics(meterRegistry);

    metrics.setPartitionLag(
        List.of(new PartitionLagMetrics(1, 3, 30), new PartitionLagMetrics(2, 5, 50)));

    assertGauge(meterRegistry, "task.queue.partition.ready", 1, 3);
    assertGauge(meterRegistry, "task.queue.partition.oldest_ready_age.seconds", 1, 30);
    assertGauge(meterRegistry, "task.queue.partition.ready", 2, 5);
    assertGauge(meterRegistry, "task.queue.partition.oldest_ready_age.seconds", 2, 50);

    metrics.setPartitionLag(List.of(new PartitionLagMetrics(2, 7, 70)));

    assertGauge(meterRegistry, "task.queue.partition.ready", 1, 0);
    assertGauge(meterRegistry, "task.queue.partition.oldest_ready_age.seconds", 1, 0);
    assertGauge(meterRegistry, "task.queue.partition.ready", 2, 7);
    assertGauge(meterRegistry, "task.queue.partition.oldest_ready_age.seconds", 2, 70);
  }

  private static void assertGauge(
      MeterRegistry meterRegistry, String meterName, int partitionNum, double expectedValue) {
    assertEquals(
        expectedValue,
        meterRegistry
            .get(meterName)
            .tag("partition", Integer.toString(partitionNum))
            .gauge()
            .value());
  }
}
