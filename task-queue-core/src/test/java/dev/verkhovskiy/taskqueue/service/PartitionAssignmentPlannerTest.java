package dev.verkhovskiy.taskqueue.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Тесты планировщика распределения партиций между воркерами. */
class PartitionAssignmentPlannerTest {

  private final PartitionAssignmentPlanner planner = new PartitionAssignmentPlanner();

  @Test
  void distributesTwentyPartitionsAcrossTwoProcesses() {
    Map<Integer, String> assignment = planner.plan(List.of("p1", "p2"), 20);

    long p1Count = assignment.values().stream().filter("p1"::equals).count();
    long p2Count = assignment.values().stream().filter("p2"::equals).count();

    assertEquals(20, assignment.size());
    assertEquals(10, p1Count);
    assertEquals(10, p2Count);
  }

  @Test
  void distributesTwentyPartitionsAcrossFourProcesses() {
    Map<Integer, String> assignment = planner.plan(List.of("p1", "p2", "p3", "p4"), 20);

    assertEquals(20, assignment.size());
    assertEquals(5, assignment.values().stream().filter("p1"::equals).count());
    assertEquals(5, assignment.values().stream().filter("p2"::equals).count());
    assertEquals(5, assignment.values().stream().filter("p3"::equals).count());
    assertEquals(5, assignment.values().stream().filter("p4"::equals).count());
  }

  @Test
  void keepsOnePartitionPerProcessWhenProcessesMoreThanPartitions() {
    Map<Integer, String> assignment = planner.plan(List.of("p1", "p2", "p3", "p4", "p5"), 3);

    assertEquals(3, assignment.size());
    assertEquals("p1", assignment.get(1));
    assertEquals("p2", assignment.get(2));
    assertEquals("p3", assignment.get(3));
  }

  @Test
  void keepsExistingAssignmentsWhenAlreadyBalanced() {
    Map<Integer, String> current =
        Map.of(
            1, "p1",
            2, "p1",
            3, "p2",
            4, "p2");

    Map<Integer, String> assignment = planner.plan(List.of("p1", "p2"), 4, current);

    assertEquals(current, assignment);
  }

  @Test
  void keepsRemainingPartitionsWhenWorkerGivesUpOnePartition() {
    Map<Integer, String> current =
        Map.of(
            1, "p1",
            2, "p1",
            3, "p1",
            4, "p2",
            5, "p2",
            6, "p2");

    Map<Integer, String> assignment = planner.plan(List.of("p1", "p2", "p3"), 6, current);

    assertEquals(2, assignment.values().stream().filter("p1"::equals).count());
    assertEquals(2, assignment.values().stream().filter("p2"::equals).count());
    assertEquals(2, assignment.values().stream().filter("p3"::equals).count());
    assertEquals("p1", assignment.get(1));
    assertEquals("p1", assignment.get(2));
    assertEquals("p2", assignment.get(4));
    assertEquals("p2", assignment.get(5));
  }

  @Test
  void whenWorkerIsRemovedKeepsOtherWorkersPartitions() {
    Map<Integer, String> current =
        Map.of(
            1, "p1",
            2, "p1",
            3, "p2",
            4, "p2",
            5, "p3",
            6, "p3");

    Map<Integer, String> assignment = planner.plan(List.of("p1", "p2"), 6, current);

    assertEquals(3, assignment.values().stream().filter("p1"::equals).count());
    assertEquals(3, assignment.values().stream().filter("p2"::equals).count());
    assertEquals("p1", assignment.get(1));
    assertEquals("p1", assignment.get(2));
    assertEquals("p2", assignment.get(3));
    assertEquals("p2", assignment.get(4));
  }
}
