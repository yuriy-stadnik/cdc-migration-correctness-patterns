package com.example.cdc.projector;

import com.example.cdc.protocol.ProtocolV2.*;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;

/** One checkpoint includes capture evidence, baseline, producer claims, graph and immutable planned output (not destination apply receipts). */
public final class V2ProjectorState {
    public int formatVersion = 2;
    public String lane;
    public String haltedReason;
    public Baseline activeBaseline;
    public Baseline candidateBaseline;
    public Map<String, BaselineOwner> baselineOwners = new TreeMap<>();
    public Map<String, JsonNode> observations = new TreeMap<>();
    public Map<String, String> observationTransactions = new TreeMap<>();
    public Map<String, Producer> producers = new TreeMap<>();
    public Map<String, Pending> pending = new TreeMap<>();
    public Map<String, Completed> completed = new TreeMap<>();
    public Map<String, Set<String>> edges = new TreeMap<>();
    public Map<String, String> quarantine = new TreeMap<>();
    public Set<String> assembledEnds = new TreeSet<>();
    public Map<String, String> waiting = new TreeMap<>();
    public Map<String, JsonNode> snapshots = new TreeMap<>();

    public static final class Producer {
        public ProducerReference reference;
        public OwnerIdentity owner;
        public OwnerState state;
        public boolean finalRevision;
        public String commitPosition;
        public JsonNode payload;
    }
    public static final class Net {
        public OwnerIdentity owner;
        public OwnerState beforeState;
        public OwnerState result;
        public List<JournalRevision> revisions = new ArrayList<>();
        public JsonNode before;
        public JsonNode after;
    }
    public static final class Pending {
        public String transactionId;
        public String captureEvidence;
        public SourceBoundary boundary;
        public List<Net> nets = new ArrayList<>();
    }
    public static final class Completed {
        public String captureEvidence;
        public String mutationSetHash;
        public List<OutputRecord> output = new ArrayList<>();
    }
}
