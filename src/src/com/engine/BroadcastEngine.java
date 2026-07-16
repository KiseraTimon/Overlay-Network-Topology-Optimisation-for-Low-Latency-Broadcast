package com.engine;

/*
 * SECTOR C — BROADCAST DISSEMINATION PROTOCOL
 * ------------------------------------------------------------------
 * Owns: actually getting a message from the source to every node,
 * on top of whatever topology Sector B built.
 *
 *   - TreePush:     fast path. Push message to all tree children.
 *   - GossipRepair: safety net. Every node periodically compares its
 *                   "seen message" set with a random peer and pulls
 *                   anything it's missing (anti-entropy). This is
 *                   what saves you when a tree parent dies mid-relay
 *                   — pure tree broadcast has NO redundancy, so a
 *                   single failure silently orphans a whole subtree
 *                   without this.
 *   - FailureRepair: when a node stops responding, its children
 *                    re-parent onto the nearest surviving ancestor
 *                    (queried from Sector B's topology) instead of
 *                    waiting for a full re-optimization pass.
 *
 * This sector is where your "robustness under churn" experiments
 * live — kill nodes mid-broadcast and measure recovery time / message
 * loss, that's your strongest result for the report.
 * ------------------------------------------------------------------
 */

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class BroadcastEngine {

    // MESSAGE ENVELOPE
    public static class BroadcastMessage {
        public final String messageId;   // unique per broadcast (e.g. sourceId + seqNum)
        public final int originId;
        public final long seqNum;
        public final String content;
        public final long createdAtMs;
        public int hopCount;

        public BroadcastMessage(int originId, long seqNum, String content) {
            this.originId = originId;
            this.seqNum = seqNum;
            this.content = content;
            this.messageId = originId + ":" + seqNum;
            this.createdAtMs = System.currentTimeMillis();
            this.hopCount = 0;
        }
    }

    // NODE-LOCAL BROADCAST STATE
    public static class BroadcastNode {
        public final int nodeId;
        private final Set<String> seenMessages = ConcurrentHashMap.newKeySet();
        private final Map<String, BroadcastMessage> messageStore = new ConcurrentHashMap<>();
        private final Map<String, Long> firstSeenAtMs = new ConcurrentHashMap<>();

        // supplied by whoever wires this node up (Sector A transport + Sector B topology)
        private Set<Integer> treeChildren = new HashSet<>();
        private Integer treeParent = null;
        private Consumer<BroadcastMessage> onDeliver = m -> {};
        private BiSender sender; // abstraction over Sector A's Node.send()

        public interface BiSender { void send(int peerId, String messageId, BroadcastMessage m); }

        public BroadcastNode(int nodeId, BiSender sender) {
            this.nodeId = nodeId;
            this.sender = sender;
        }

        public void setTree(Integer parent, Set<Integer> children) {
            this.treeParent = parent;
            this.treeChildren = children;
        }

        public void onDeliver(Consumer<BroadcastMessage> handler) { this.onDeliver = handler; }

        /** Called by the source node to kick off a broadcast. */
        public void initiateBroadcast(String content, long seqNum) {
            BroadcastMessage m = new BroadcastMessage(nodeId, seqNum, content);
            handleIncoming(m, nodeId);
        }

        /**
         * Core dedup-and-relay logic. Called both for messages that
         * arrive via the tree AND messages pulled in via gossip
         * repair — the dedup set makes both paths safe to combine.
         */
        public void handleIncoming(BroadcastMessage m, int fromPeer) {
            if (!seenMessages.add(m.messageId)) return; // already delivered — stop here (this bounds "stress")

            firstSeenAtMs.put(m.messageId, System.currentTimeMillis());
            messageStore.put(m.messageId, m);
            onDeliver.accept(m);

            m.hopCount++;
            for (int child : treeChildren) {
                if (child != fromPeer) sender.send(child, m.messageId, m);
            }
        }

        public Set<String> seenMessageIds() { return seenMessages; }
        public BroadcastMessage getMessage(String id) { return messageStore.get(id); }
        public long deliveryLatencyMs(String messageId, long broadcastStartMs) {
            Long t = firstSeenAtMs.get(messageId);
            return t == null ? -1 : t - broadcastStartMs;
        }
    }

    // GOSSIP ANTI-ENTROPY REPAIR
    /**
     * Periodic background task: pick a random mesh peer (NOT just
     * tree neighbors — use a wider "gossip peer" set from Sector B),
     * exchange seen-message-id sets, pull anything missing.
    */
    public static class GossipRepair {
        private final BroadcastNode node;
        private final Set<Integer> gossipPeers;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private final GossipTransport transport;

        public interface GossipTransport {
            Set<String> requestSeenIds(int peerId);
            BroadcastMessage requestMessage(int peerId, String messageId);
        }

        public GossipRepair(BroadcastNode node, Set<Integer> gossipPeers, GossipTransport transport) {
            this.node = node;
            this.gossipPeers = gossipPeers;
            this.transport = transport;
        }

        public void startPeriodic(long periodMs) {
            scheduler.scheduleAtFixedRate(this::runOnce, periodMs, periodMs, TimeUnit.MILLISECONDS);
        }

        void runOnce() {
            if (gossipPeers.isEmpty()) return;
            List<Integer> peers = new ArrayList<>(gossipPeers);
            int peer = peers.get(new Random().nextInt(peers.size()));

            Set<String> theirIds = transport.requestSeenIds(peer);
            Set<String> missing = new HashSet<>(theirIds);
            missing.removeAll(node.seenMessageIds());

            for (String id : missing) {
                BroadcastMessage m = transport.requestMessage(peer, id);
                if (m != null) node.handleIncoming(m, peer);
            }
        }

        public void stop() { scheduler.shutdownNow(); }
    }

    /**
     * FAILURE-TRIGGERED RE-PARENTING
     * Called by a failure detector (heartbeat timeout from Sector A)
     * when treeParent stops responding. Walks up Sector B's last-known
     * topology to find the nearest still-alive ancestor.
    */
    public static Integer findReparentTarget(
        int failedNode,
        Map<Integer, Integer> treeParentMap,
        Set<Integer> aliveNodes
    ) {
        Integer candidate = treeParentMap.get(failedNode);
        int hops = 0;
        while (candidate != null && !aliveNodes.contains(candidate) && hops < 20) {
            candidate = treeParentMap.get(candidate);
            hops++;
        }
        return candidate;
    }
}