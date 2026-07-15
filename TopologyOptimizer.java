/*
 * SECTOR B — TOPOLOGY CONSTRUCTION & OPTIMIZATION (core contribution)
 * ------------------------------------------------------------------
 * Owns: turning a latency matrix into an overlay topology, and
 * continuously improving it.
 *
 * Ships 4 strategies to compare in your report:
 *   1. RandomOverlay          - control / baseline
 *   2. ShortestPathTree       - Dijkstra from the broadcast source
 *                               (optimal per-node latency, but the
 *                               source can end up with huge fan-out)
 *   3. DegreeBoundedTree      - Prim-style MST growth with a max
 *                               fan-out cap per node (more realistic
 *                               — no node can blast to 50 children)
 *   4. AdaptiveGossipOptimizer- decentralized, T-Man-style: each node
 *                               periodically asks a random neighbor
 *                               for ITS neighbor list, and swaps in
 *                               any candidate that lowers local cost.
 *                               This is the piece that makes the
 *                               overlay "self-optimizing" rather than
 *                               a one-shot computation — good for the
 *                               "powerful project" bar.
 *
 * Output of every strategy is a Topology: node -> set of neighbors
 * (undirected mesh) or node -> parent (for pure trees). Sector C
 * consumes this to actually push messages.
 * ------------------------------------------------------------------
 */

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

public class TopologyOptimizer {

    // COMMON DATA MODEL
    public static class Topology {
        public final Map<Integer, Set<Integer>> adjacency = new HashMap<>();

        public void addEdge(int a, int b) {
            adjacency.computeIfAbsent(a, k -> new HashSet<>()).add(b);
            adjacency.computeIfAbsent(b, k -> new HashSet<>()).add(a);
        }

        public void removeEdge(int a, int b) {
            adjacency.getOrDefault(a, Set.of()).remove(b);
            adjacency.getOrDefault(b, Set.of()).remove(a);
        }

        public Set<Integer> neighborsOf(int node) {
            return adjacency.getOrDefault(node, Collections.emptySet());
        }

        /** Sum of edge count per node — a proxy for broadcast "stress" on that node. */
        public int degree(int node) {
            return neighborsOf(node).size();
        }
    }

    // A latency matrix: node -> node -> ms. Supplied by Sector A's LatencyOracle.
    public static class LatencyMatrix {
        public final Map<Integer, Map<Integer, Double>> data;
        public LatencyMatrix(Map<Integer, Map<Integer, Double>> data) { this.data = data; }
        public double get(int a, int b) { return data.get(a).get(b); }
    }

    // 1. RANDOM BASELINE
    public static Topology randomOverlay(Set<Integer> nodes, int fanOut, long seed) {
        Topology t = new Topology();
        List<Integer> ids = new ArrayList<>(nodes);
        Random rng = new Random(seed);
        for (int a : ids) {
            Collections.shuffle(ids, rng);
            int added = 0;
            for (int b : ids) {
                if (b == a || added >= fanOut) continue;
                t.addEdge(a, b);
                added++;
            }
        }
        return t;
    }

    // 2. SHORTEST PATH TREE (Dijkstra)
    public static Topology shortestPathTree(Set<Integer> nodes, LatencyMatrix lat, int source) {
        Topology tree = new Topology();
        Map<Integer, Double> dist = new HashMap<>();
        Map<Integer, Integer> parent = new HashMap<>();
        for (int n : nodes) dist.put(n, Double.MAX_VALUE);
        dist.put(source, 0.0);

        PriorityQueue<int[]> pq = new PriorityQueue<>(Comparator.comparingDouble(x -> dist.get(x[0])));
        pq.add(new int[]{source});
        Set<Integer> visited = new HashSet<>();

        while (!pq.isEmpty()) {
            int u = pq.poll()[0];
            if (!visited.add(u)) continue;
            for (int v : nodes) {
                if (v == u || visited.contains(v)) continue;
                double alt = dist.get(u) + lat.get(u, v);
                if (alt < dist.get(v)) {
                    dist.put(v, alt);
                    parent.put(v, u);
                    pq.add(new int[]{v});
                }
            }
        }
        for (Map.Entry<Integer, Integer> e : parent.entrySet()) {
            tree.addEdge(e.getKey(), e.getValue());
        }
        return tree;
    }

    // 3. DEGREE-BOUNDED LATENCY TREE
    /**
     * Prim-growth MST, but a node stops accepting new children once it
     * hits maxFanOut — forces the tree to "branch out" instead of
     * collapsing onto one hub, which is what a real broadcast source
     * needs (bounded upload bandwidth).
    */
    public static Topology degreeBoundedTree(Set<Integer> nodes, LatencyMatrix lat, int source, int maxFanOut) {
        Topology tree = new Topology();
        Set<Integer> inTree = new HashSet<>();
        inTree.add(source);

        while (inTree.size() < nodes.size()) {
            double best = Double.MAX_VALUE;
            int bestU = -1, bestV = -1;
            for (int u : inTree) {
                if (tree.degree(u) >= maxFanOut) continue; // respects fan-out cap
                for (int v : nodes) {
                    if (inTree.contains(v)) continue;
                    double d = lat.get(u, v);
                    if (d < best) { best = d; bestU = u; bestV = v; }
                }
            }
            if (bestV == -1) break; // every in-tree node saturated; nodes left unreachable — report this!
            tree.addEdge(bestU, bestV);
            inTree.add(bestV);
        }
        return tree;
    }

    // 4. ADAPTIVE GOSSIP OPTIMIZER
    /**
     * Decentralized local-search: run this as a periodic background
     * task on EVERY node (each node only sees its own neighbors +
     * gossiped neighbor lists — no global view, which is what makes
     * this genuinely "distributed" rather than a batch computation).
    */
    public static class AdaptiveGossipOptimizer {
        private final Topology topology;
        private final LatencyMatrix lat;
        private final int maxFanOut;
        private final Random rng;

        public AdaptiveGossipOptimizer(Topology topology, LatencyMatrix lat, int maxFanOut, long seed) {
            this.topology = topology;
            this.lat = lat;
            this.maxFanOut = maxFanOut;
            this.rng = new Random(seed);
        }

        /** Local cost = sum of latency to this node's current neighbors. Lower is better. */
        private double localCost(int node, Set<Integer> neighbors) {
            double cost = 0;
            for (int n : neighbors) cost += lat.get(node, n);
            return cost;
        }

        /**
         * One optimization round for a single node: pick a random
         * current neighbor, look at THEIR neighbors (this is the
         * "gossip" — in the real system this is an actual
         * TOPOLOGY_UPDATE message exchange, see Sector C), and swap
         * in any candidate that lowers this node's local cost while
         * respecting the fan-out cap. Converges toward a
         * locality-clustered overlay over many rounds.
         */
        public void optimizeRound(int node) {
            Set<Integer> myNeighbors = topology.neighborsOf(node);
            if (myNeighbors.isEmpty()) return;

            int probeVia = new ArrayList<>(myNeighbors).get(rng.nextInt(myNeighbors.size()));
            Set<Integer> candidates = topology.neighborsOf(probeVia);

            double currentCost = localCost(node, myNeighbors);
            for (int candidate : candidates) {
                if (candidate == node || myNeighbors.contains(candidate)) continue;

                // try swapping candidate in for our currently-worst neighbor
                int worst = myNeighbors.stream()
                        .max(Comparator.comparingDouble(n -> lat.get(node, n)))
                        .orElse(-1);
                if (worst == -1) continue;

                double worstEdgeCost = lat.get(node, worst);
                double candidateCost = lat.get(node, candidate);

                if (candidateCost < worstEdgeCost && topology.degree(candidate) < maxFanOut) {
                    topology.removeEdge(node, worst);
                    topology.addEdge(node, candidate);
                    return; // one improving swap per round keeps this stable/incremental
                }
            }
        }

        /** Convenience driver for offline experiments (real system: each node schedules this itself). */
        public void runRounds(Set<Integer> nodes, int rounds) {
            for (int r = 0; r < rounds; r++) {
                for (int n : nodes) optimizeRound(n);
            }
        }
    }

    // METRICS
    public static double treeDiameterMs(Topology t, LatencyMatrix lat, int source) {
        // longest source-to-leaf path — this IS your headline "broadcast latency" metric
        Map<Integer, Double> dist = new HashMap<>();
        Deque<Integer> queue = new ArrayDeque<>();
        dist.put(source, 0.0);
        queue.add(source);
        double max = 0;
        while (!queue.isEmpty()) {
            int u = queue.poll();
            for (int v : t.neighborsOf(u)) {
                if (!dist.containsKey(v)) {
                    double d = dist.get(u) + lat.get(u, v);
                    dist.put(v, d);
                    max = Math.max(max, d);
                    queue.add(v);
                }
            }
        }
        return max;
    }
}