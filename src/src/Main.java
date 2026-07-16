import com.optimizer.TopologyOptimizer;
import com.transport.OverlayTransport.*;
import com.optimizer.TopologyOptimizer.*;
import com.engine.BroadcastEngine.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting Distributed Broadcast Simulation...\n");

        int NUM_NODES = 5;
        int SOURCE_NODE = 1;
        int BASE_PORT = 9000;
        String STRATEGY = "adaptive_gossip";

        // JSON logging for Main.py
        File logDir = new File("logs/" + STRATEGY);
        if (!logDir.exists()) logDir.mkdirs();

        Map<Integer, PrintWriter> loggers = new HashMap<>();
        for (int i = 1; i <= NUM_NODES; i++) {
            loggers.put(i, new PrintWriter(new FileWriter(new File(logDir, "node-" + i + ".log"))));
        }

        // Tracking when a broadcast originally started so we can calculate latency
        Map<String, Long> globalOriginTimes = new ConcurrentHashMap<>();

        // 1. SECTOR A: Initialize Physical Transport & Latency
        System.out.println("1. Spinning up Transport Layer (Sector A)");
        LatencyOracle oracle = new LatencyOracle(42L, 0.05, 5.0);

        Map<Integer, Node> physicalNodes = new HashMap<>();
        Set<Integer> nodeIds = new HashSet<>();

        for (int i = 1; i <= NUM_NODES; i++) {
            nodeIds.add(i);
            Node node = new Node(i, BASE_PORT + i, oracle);
            physicalNodes.put(i, node);
            node.start();
        }

        for (Node n1 : physicalNodes.values()) {
            for (Node n2 : physicalNodes.values()) {
                if (n1.nodeId != n2.nodeId) {
                    n1.addPeer(n2.nodeId, "localhost", BASE_PORT + n2.nodeId);
                }
            }
        }

        // 2. SECTOR B: Build Overlay Topology
        System.out.println("2. Building Broadcast Topology (Sector B)");
        LatencyMatrix latencyMatrix = new LatencyMatrix(oracle.fullMatrix(nodeIds));
        Topology tree = TopologyOptimizer.degreeBoundedTree(nodeIds, latencyMatrix, SOURCE_NODE, 2);

        Map<Integer, Integer> parentMap = new HashMap<>();
        Map<Integer, Set<Integer>> childrenMap = new HashMap<>();
        for (int i : nodeIds) childrenMap.put(i, new HashSet<>());

        Queue<Integer> queue = new LinkedList<>();
        Set<Integer> visited = new HashSet<>();
        queue.add(SOURCE_NODE);
        visited.add(SOURCE_NODE);

        while (!queue.isEmpty()) {
            int current = queue.poll();
            for (int neighbor : tree.neighborsOf(current)) {
                if (!visited.contains(neighbor)) {
                    visited.add(neighbor);
                    parentMap.put(neighbor, current);
                    childrenMap.get(current).add(neighbor);
                    queue.add(neighbor);
                }
            }
        }

        // 3. SECTOR C: Wire up the Broadcast Engine & JSON Logs
        System.out.println("3. Wiring Broadcast Engine to Transport (Sector C)");
        Map<Integer, BroadcastNode> broadcastNodes = new HashMap<>();

        for (int i = 1; i <= NUM_NODES; i++) {
            final int id = i;
            Node physicalNode = physicalNodes.get(id);

            // BRIDGE 1: Broadcast -> Transport
            BroadcastNode bNode = getBroadcastNode(loggers, id, physicalNode);
            bNode.setTree(parentMap.get(id), childrenMap.get(id));

            // BRIDGE 2: On Delivery
            bNode.onDeliver(m -> {
                System.out.println("[Node " + id + "] Delivered: '" + m.content + "'");

                // Write "DELIVER" event to JSON Log
                long originMs = globalOriginTimes.getOrDefault(m.messageId, System.currentTimeMillis());
                loggers.get(id).printf("{\"event\": \"DELIVER\", \"node\": %d, \"messageId\": \"%s\", \"originMs\": %d, \"recvMs\": %d}\n",
                        id, m.messageId, originMs, System.currentTimeMillis());
                loggers.get(id).flush();
            });

            broadcastNodes.put(id, bNode);

            // BRIDGE 3: Transport -> Broadcast
            physicalNode.onMessage((fromPeer, rawMsg) -> {
                if ("BROADCAST".equals(rawMsg.type)) {
                    String[] parts = rawMsg.payload.split("::");
                    BroadcastMessage m = new BroadcastMessage(
                            Integer.parseInt(parts[0]),
                            Long.parseLong(parts[1]),
                            parts[2]
                    );
                    broadcastNodes.get(id).handleIncoming(m, fromPeer);
                }
            });
        }

        // 4. Execution & Testing
        System.out.println("4. Initiating Broadcast from Source Node " + SOURCE_NODE + "\n");

        // Tracking the exact millisecond this message enters the system
        String msgId = SOURCE_NODE + ":" + 1001;
        globalOriginTimes.put(msgId, System.currentTimeMillis());

        broadcastNodes.get(SOURCE_NODE).initiateBroadcast("Project Integration Successful!", 1001);

        Thread.sleep(2000);

        System.out.println("\n5. Shutting down");
        for (Node n : physicalNodes.values()) {
            n.shutdown();
        }
        for (PrintWriter writer : loggers.values()) {
            writer.close();
        }
        System.exit(0);
    }

    private static BroadcastNode getBroadcastNode(Map<Integer, PrintWriter> loggers, int id, Node physicalNode) {
        BroadcastNode.BiSender sender = (peerId, messageId, bMsg) -> {
            // Write "SEND" event to JSON Log
            loggers.get(id).printf("{\"event\": \"SEND\", \"node\": %d, \"messageId\": \"%s\", \"toNode\": %d}\n",
                    id, messageId, peerId);
            loggers.get(id).flush();

            String payload = bMsg.originId + "::" + bMsg.seqNum + "::" + bMsg.content;
            physicalNode.send(peerId, new Message(id, "BROADCAST", payload));
        };

        BroadcastNode bNode = new BroadcastNode(id, sender);
        return bNode;
    }
}