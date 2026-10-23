package com.example.cdc.projector;

import com.example.cdc.protocol.Protocol;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** All correctness state is checkpointed together with the Kafka source position. */
public final class ProjectorState implements Serializable {
    public Map<String, Protocol.VersionToken> objectVersions = new HashMap<>();
    public Map<String, JsonNode> objectPayloads = new HashMap<>();
    public Map<String, Assembly> assemblies = new LinkedHashMap<>();
    public long lastPlannedCommitSequence = -1;

    public static final class Assembly implements Serializable {
        public String nativeTransactionId;
        public String canonicalTransactionId;
        public long firstSeenEpochMs;
        public long beginOffset = -1;
        public long endOffset = -1;
        public int expectedEventCount = -1;
        public Map<String, Integer> expectedCollectionCounts;
        public boolean beginSeen;
        public boolean endSeen;
        public boolean warned;
        public boolean incompletePublished;
        public boolean dlqPublished;
        public String beginHash;
        public String endHash;
        public String commitPosition;
        public Map<Long, CapturedChange> changes = new java.util.TreeMap<>();
        public Map<Long, String> changeHashes = new HashMap<>();
    }

    public static final class CapturedChange implements Serializable {
        public String collection;
        public long sequence;
        public String sourceEventId;
        public String sourcePosition;
        public JsonNode before;
        public JsonNode after;
        public String operation;
        public long sourceTimestampMs;
    }
}
