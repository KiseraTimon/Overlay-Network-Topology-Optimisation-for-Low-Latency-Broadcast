package com.transport;

/*
 * SECTOR A — TRANSPORT & LATENCY SUBSTRATE
 * ------------------------------------------------------------------
 * Owns: the "physical" layer every other sector builds on.
 *
 *   - Node:            a real OS process listening on a TCP port.
 *   - Message:         wire format (length-prefixed JSON-ish text).
 *   - LatencyOracle:   assigns each node a synthetic coordinate and
 *                      derives a pairwise latency matrix from it, so
 *                      the whole system can be latency-tested on one
 *                      laptop without real geographic distribution.
 *   - ShapedLink:      wraps a socket send so it actually incurs the
 *                      synthetic one-way delay before the bytes hit
 *                      the wire (delay-queue backed by a scheduler).
 *
 * Sectors B/C build on top of Node.send()/Node.onMessage() — they
 * should never touch raw sockets themselves.
 * ------------------------------------------------------------------
 */

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class OverlayTransport {

    // LATENCY ORACLE
    /**
     * Every node gets a synthetic 2D coordinate. Latency between two
     * nodes = base propagation delay (distance-based) + queueing
     * jitter. Swap this out later for real measured RTTs if you get
     * access to multiple machines/regions.
     */
    public static class LatencyOracle {
        /**
         * IMPORTANT: this oracle is instantiated separately inside EVERY node's
         * JVM process (there is no shared object across processes). Coordinates
         * must therefore be a deterministic function of (globalSeed, nodeId) --
         * NOT of call order -- otherwise node 3's process and node 7's process
         * would disagree about where node 3 is. Jitter is likewise derived
         * deterministically per (a,b) pair so both endpoints agree on the delay.
         */
        private final Map<Integer, double[]> coords = new HashMap<>();
        private final long seed;
        private final double msPerUnitDistance;
        private final double jitterMs;

        public LatencyOracle(long seed, double msPerUnitDistance, double jitterMs) {
            this.seed = seed;
            this.msPerUnitDistance = msPerUnitDistance;
            this.jitterMs = jitterMs;
        }

        public void registerNode(int nodeId) {
            Random perNodeRng = new Random(seed ^ (nodeId * 0x9E3779B97F4A7C15L));
            coords.put(nodeId, new double[]{perNodeRng.nextDouble() * 1000, perNodeRng.nextDouble() * 1000});
        }

        public double latencyMs(int a, int b) {
            double[] pa = coords.get(a), pb = coords.get(b);
            double dx = pa[0] - pb[0], dy = pa[1] - pb[1];
            double dist = Math.sqrt(dx * dx + dy * dy);
            double base = dist * msPerUnitDistance;
            long pairSeed = seed ^ (Math.min(a, b) * 1_000_003L + Math.max(a, b));
            double jitter = new Random(pairSeed).nextDouble() * jitterMs;
            return Math.max(1.0, base + jitter);
        }

        public Map<Integer, Map<Integer, Double>> fullMatrix(Set<Integer> nodeIds) {
            Map<Integer, Map<Integer, Double>> matrix = new HashMap<>();
            for (int i : nodeIds) {
                Map<Integer, Double> row = new HashMap<>();
                for (int j : nodeIds) {
                    if (i != j) row.put(j, latencyMs(i, j));
                }
                matrix.put(i, row);
            }
            return matrix;
        }
    }

    // WIRE MESSAGE
    public static class Message implements Serializable {
        public final int senderId;
        public final String type;      // e.g. "BROADCAST", "TOPOLOGY_UPDATE", "PING"
        public final String payload;   // JSON-encoded body, sector-specific
        public final long originTimestampMs;

        public Message(int senderId, String type, String payload) {
            this.senderId = senderId;
            this.type = type;
            this.payload = payload;
            this.originTimestampMs = System.currentTimeMillis();
        }

        String serialize() {
            return senderId + "\u0001" + type + "\u0001" + payload + "\u0001" + originTimestampMs;
        }

        static Message deserialize(String line) {
            String[] parts = line.split("\u0001", 4);
            Message m = new Message(Integer.parseInt(parts[0]), parts[1], parts[2]);
            return m;
        }
    }

    // NODE
    public static class Node {
        public final int nodeId;
        public final int port;
        private final LatencyOracle oracle;
        private final ScheduledExecutorService delayScheduler = Executors.newScheduledThreadPool(2);
        private final Map<Integer, InetSocketAddress> peerAddresses = new ConcurrentHashMap<>();
        private final Map<Integer, Socket> outboundSockets = new ConcurrentHashMap<>();
        private BiConsumer<Integer, Message> messageHandler = (id, m) -> {};
        private volatile boolean running = true;

        public Node(int nodeId, int port, LatencyOracle oracle) {
            this.nodeId = nodeId;
            this.port = port;
            this.oracle = oracle;
            oracle.registerNode(nodeId);
        }

        public void onMessage(BiConsumer<Integer, Message> handler) {
            this.messageHandler = handler;
        }

        /** Start listening for inbound connections. Called once. */
        public void start() throws IOException {
            ServerSocket server = new ServerSocket(port);
            Thread acceptLoop = new Thread(() -> {
                while (running) {
                    try {
                        Socket client = server.accept();
                        Thread reader = new Thread(() -> handleInbound(client));
                        reader.setDaemon(true);
                        reader.start();
                    } catch (IOException e) {
                        if (running) System.err.println("[node " + nodeId + "] accept error: " + e.getMessage());
                    }
                }
            });
            acceptLoop.setDaemon(true);
            acceptLoop.start();
        }

        private void handleInbound(Socket client) {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()))) {
                String line;
                while ((line = in.readLine()) != null) {
                    Message m = Message.deserialize(line);
                    messageHandler.accept(m.senderId, m);
                }
            } catch (IOException ignored) {
            }
        }

        public void addPeer(int peerId, String host, int peerPort) {
            peerAddresses.put(peerId, new InetSocketAddress(host, peerPort));
        }

        public void removePeer(int peerId) {
            peerAddresses.remove(peerId);
            Socket s = outboundSockets.remove(peerId);
            if (s != null) try { s.close(); } catch (IOException ignored) {}
        }

        /**
         * Send a message to a peer, artificially delayed by the
         * synthetic one-way latency from the LatencyOracle. This is
         * how Sector A "shapes" traffic without needing real
         * geographic distribution.
         */
        public void send(int peerId, Message m) {
            double delayMs = oracle.latencyMs(nodeId, peerId);
            delayScheduler.schedule(() -> actuallySend(peerId, m), (long) delayMs, TimeUnit.MILLISECONDS);
        }

        private void actuallySend(int peerId, Message m) {
            try {
                Socket s = outboundSockets.computeIfAbsent(peerId, id -> {
                    try {
                        InetSocketAddress addr = peerAddresses.get(id);
                        Socket sock = new Socket();
                        sock.connect(addr, 2000);
                        return sock;
                    } catch (IOException e) {
                        return null;
                    }
                });
                if (s == null) return;
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                out.println(m.serialize());
            } catch (Exception e) {
                System.err.println("[node " + nodeId + "] send to " + peerId + " failed: " + e.getMessage());
                removePeer(peerId); // trigger reconnect next time; Sector C treats this as a failure signal
            }
        }

        public Set<Integer> knownPeers() {
            return peerAddresses.keySet();
        }

        public void shutdown() {
            running = false;
            delayScheduler.shutdownNow();
        }
    }

    // DEMO
    public static void main(String[] args) throws Exception {
        LatencyOracle oracle = new LatencyOracle(42, 0.05, 5.0);

        Node n1 = new Node(1, 9001, oracle);
        Node n2 = new Node(2, 9002, oracle);
        n2.onMessage((from, m) -> System.out.println("[node 2] got '" + m.payload + "' from " + from
                + " (network delay ~" + (System.currentTimeMillis() - m.originTimestampMs) + "ms)"));

        n1.start();
        n2.start();
        n1.addPeer(2, "localhost", 9002);

        n1.send(2, new Message(1, "BROADCAST", "hello overlay"));
        Thread.sleep(1000);

        n1.shutdown();
        n2.shutdown();
    }
}