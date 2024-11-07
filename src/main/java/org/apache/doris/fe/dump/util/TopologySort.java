package org.apache.doris.fe.dump.util;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class TopologySort<T> {
    // Map to store event nodes and their indegrees
    private final SetMultimap<T, T> partialOrders = LinkedHashMultimap.create();
    private final Map<T, Integer> indegreeMap = new HashMap<>();

    // Add a partial order between two events
    public void addPartialOrder(T before, T after) {
        // Get or create the nodes
        partialOrders.put(before, after);

        // Update indegrees
        indegreeMap.put(after, indegreeMap.getOrDefault(after, 0) + 1);
        indegreeMap.putIfAbsent(before, 0);
    }

    // Perform topological sort
    public List<T> sort() {
        List<T> sortedList = new ArrayList<>();
        Queue<T> queue = new LinkedList<>();

        // Add nodes with zero indegree to the queue
        for (Map.Entry<T, Integer> entry : indegreeMap.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        // Process the queue
        while (!queue.isEmpty()) {
            T current = queue.poll();
            sortedList.add(current);

            // Reduce the indegree of the next node
            for (T next : partialOrders.get(current)) {
                int indegree = indegreeMap.get(next) - 1;
                indegreeMap.put(next, indegree);
                if (indegree == 0) {
                    queue.offer(next);
                }
            }
        }

        // Check for cycle
        // if (sortedList.size() != partialOrders.size()) {
        //     throw new RuntimeException("Graph has a cycle, topological sort not possible");
        // }

        return sortedList;
    }
}
