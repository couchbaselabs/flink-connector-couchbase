/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connector.flink.internal.dcp;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PartitionHelper {
  private PartitionHelper() {
    throw new AssertionError("not instantiable");
  }

  public static List<Integer> getAssignedPartitions(int numPartitions, RuntimeContext ctx) {
    final int taskIndex = ctx.getIndexOfThisSubtask();
    final int numTasks = ctx.getNumberOfParallelSubtasks();

    final List<Integer> allPartitions = IntStream.range(0, numPartitions).boxed().collect(toList());
    return chunks(allPartitions, numTasks).get(taskIndex);
  }

  /**
   * Splits the given list into the requested number of chunks.
   * The smallest and largest chunks are guaranteed to differ in size by no more than 1.
   * If the requested number of chunks is greater than the number of items,
   * some chunks will be empty.
   */
  public static <T> List<List<T>> chunks(List<T> items, int chunks) {
    if (chunks <= 0) {
      throw new IllegalArgumentException("chunks must be > 0");
    }
    requireNonNull(items);

    final int maxChunkSize = ((items.size() - 1) / chunks) + 1; // size / chunks, rounded up
    final int numFullChunks = chunks - (maxChunkSize * chunks - items.size());

    final List<List<T>> result = new ArrayList<>(chunks);

    int startIndex = 0;
    for (int i = 0; i < chunks; i++) {
      int endIndex = startIndex + maxChunkSize;
      if (i >= numFullChunks) {
        endIndex--;
      }
      result.add(items.subList(startIndex, endIndex));
      startIndex = endIndex;
    }
    return result;
  }
}
