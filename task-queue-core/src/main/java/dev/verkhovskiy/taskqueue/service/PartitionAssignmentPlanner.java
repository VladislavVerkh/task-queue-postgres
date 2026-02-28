package dev.verkhovskiy.taskqueue.service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.springframework.stereotype.Component;

/**
 * Строит план закрепления партиций за воркерами.
 *
 * <p>Алгоритм минимизирует количество переносимых партиций: сначала оставляет воркерам максимально
 * возможную часть текущих закреплений, затем перераспределяет только "лишние" и "свободные"
 * партиции.
 */
@Component
public class PartitionAssignmentPlanner {

  /**
   * Строит план закрепления без учета предыдущего состояния.
   *
   * @param workerIds список воркеров в стабильном порядке
   * @param partitionCount общее количество партиций
   * @return отображение {@code partition -> worker}
   */
  public Map<Integer, String> plan(List<String> workerIds, int partitionCount) {
    return plan(workerIds, partitionCount, Map.of());
  }

  /**
   * Строит план закрепления с учетом текущих assignments.
   *
   * @param workerIds список воркеров в стабильном порядке
   * @param partitionCount общее количество партиций
   * @param currentAssignments текущее закрепление {@code partition -> worker}
   * @return новый план закрепления {@code partition -> worker}
   */
  public Map<Integer, String> plan(
      List<String> workerIds, int partitionCount, Map<Integer, String> currentAssignments) {
    if (partitionCount <= 0) {
      throw new IllegalArgumentException("partitionCount must be > 0");
    }
    if (workerIds.isEmpty()) {
      return Map.of();
    }

    Map<String, Integer> targetCounts = targetCounts(workerIds, partitionCount);
    Map<String, List<Integer>> existingByWorker =
        existingByWorker(workerIds, partitionCount, currentAssignments);

    Map<Integer, String> assignment = new LinkedHashMap<>(partitionCount);
    Map<String, Integer> keptCounts = new HashMap<>();
    Set<Integer> keptPartitions = new HashSet<>();

    workerIds.forEach(
        workerId -> {
          List<Integer> partitions = existingByWorker.get(workerId);
          Collections.sort(partitions);
          int keepCount = Math.min(targetCounts.get(workerId), partitions.size());
          IntStream.range(0, keepCount)
              .mapToObj(partitions::get)
              .forEach(
                  partition -> {
                    assignment.put(partition, workerId);
                    keptPartitions.add(partition);
                  });
          keptCounts.put(workerId, keepCount);
        });

    Deque<Integer> availablePartitions =
        IntStream.rangeClosed(1, partitionCount)
            .filter(partition -> !keptPartitions.contains(partition))
            .boxed()
            .collect(Collectors.toCollection(ArrayDeque::new));

    workerIds.forEach(
        workerId -> {
          int targetCount = targetCounts.get(workerId);
          int currentCount = keptCounts.get(workerId);
          int need = targetCount - currentCount;
          IntStream.range(0, need)
              .forEach(
                  ignored -> {
                    Integer partition = availablePartitions.pollFirst();
                    if (partition == null) {
                      throw new IllegalStateException("Not enough partitions for rebalance plan");
                    }
                    assignment.put(partition, workerId);
                  });
        });

    return IntStream.rangeClosed(1, partitionCount)
        .boxed()
        .collect(
            Collectors.toMap(
                Function.identity(), assignment::get, (left, right) -> left, LinkedHashMap::new));
  }

  /**
   * Вычисляет целевое количество партиций на воркер.
   *
   * <p>Распределение выполняется равномерно: сначала базовая квота, затем остаток добавляется
   * первым воркерам по порядку.
   */
  private static Map<String, Integer> targetCounts(List<String> workerIds, int partitionCount) {
    int base = partitionCount / workerIds.size();
    int remainder = partitionCount % workerIds.size();
    Map<String, Integer> target = new LinkedHashMap<>(workerIds.size());
    IntStream.range(0, workerIds.size())
        .forEach(index -> target.put(workerIds.get(index), base + (index < remainder ? 1 : 0)));
    return target;
  }

  /**
   * Группирует текущие партиции по воркерам и отбрасывает нерелевантные записи.
   *
   * <p>Игнорируются партиции вне диапазона и assignments на неизвестные воркеры.
   */
  private static Map<String, List<Integer>> existingByWorker(
      List<String> workerIds, int partitionCount, Map<Integer, String> currentAssignments) {
    Map<String, List<Integer>> grouped =
        workerIds.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    ignored -> new ArrayList<>(),
                    (left, right) -> left,
                    LinkedHashMap::new));

    currentAssignments.forEach(
        (partition, workerId) -> {
          if (partition == null || partition < 1 || partition > partitionCount) {
            return;
          }
          List<Integer> partitions = grouped.get(workerId);
          if (partitions != null) {
            partitions.add(partition);
          }
        });
    return grouped;
  }
}
