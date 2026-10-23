package com.example.cdc.projector;

import com.example.cdc.protocol.Protocol;
import com.example.cdc.protocol.ProtocolCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

import static com.example.cdc.protocol.Protocol.*;

/**
 * Single ordered planning lane. An incomplete earlier transaction deliberately blocks the projection frontier.
 */
public final class TransactionProjectorFunction extends KeyedProcessFunction<String, CaptureRecord, OutputRecord> {
    private static final long WARN_AFTER_MS = 120_000;
    private static final long INCOMPLETE_AFTER_MS = 600_000;
    private static final long DLQ_AFTER_MS = 86_400_000;
    // Protocol hashes sort maps; a planning queue must preserve insertion order instead.
    private static final ObjectMapper STATE_MAPPER = ProtocolCodec.mapper().copy()
            .disable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    private final String sourceDatabaseId;
    private final String sourceIncarnation;
    private final String captureScopeId;
    private transient ValueState<String> checkpointedState;
    private transient InventoryProjectionPlanner planner;
    private transient Counter completedTransactions;
    private transient Counter incompleteTransactions;
    private transient Counter quarantinedTransactions;

    public TransactionProjectorFunction(String sourceDatabaseId, String sourceIncarnation, String captureScopeId) {
        this.sourceDatabaseId = sourceDatabaseId;
        this.sourceIncarnation = sourceIncarnation;
        this.captureScopeId = captureScopeId;
    }

    @Override
    public void open(Configuration parameters) {
        checkpointedState = getRuntimeContext().getState(new ValueStateDescriptor<>("transaction-projector-v1", String.class));
        planner = new InventoryProjectionPlanner();
        completedTransactions = getRuntimeContext().getMetricGroup().counter("completed_transactions");
        incompleteTransactions = getRuntimeContext().getMetricGroup().counter("incomplete_transactions");
        quarantinedTransactions = getRuntimeContext().getMetricGroup().counter("quarantined_transactions");
    }

    @Override
    public void processElement(CaptureRecord record, Context context, Collector<OutputRecord> out) throws Exception {
        ProjectorState state = load();
        JsonNode root = ProtocolCodec.mapper().readTree(record.value());
        long now = context.timerService().currentProcessingTime();
        try {
            if (isBoundary(root)) processBoundary(state, record, root, now, context);
            else processData(state, record, root, now, context);
        } catch (RuntimeException protocolFailure) {
            ProjectorState.Assembly tx = assemblyForFailure(state, record, root);
            if (tx == null) throw protocolFailure;
            if (!tx.dlqPublished) {
                out.collect(control(tx, ControlType.FAILED, SourceStatus.FAILED,
                        protocolFailure.getMessage(), DLQ_TOPIC));
                quarantinedTransactions.inc();
                tx.dlqPublished = true;
            }
        }
        drainPlanningFrontier(state, out);
        save(state);
    }

    private void processBoundary(ProjectorState state, CaptureRecord record, JsonNode root, long now, Context context) {
        String rawId = requiredText(root, "id");
        String nativeId = ProtocolCodec.normalizePostgresTransactionId(rawId);
        ProjectorState.Assembly tx = assembly(state, nativeId, now, context);
        String status = requiredText(root, "status");
        String boundaryHash = ProtocolCodec.sha256(root);
        if ("BEGIN".equals(status)) {
            if (tx.beginHash != null && !tx.beginHash.equals(boundaryHash)) {
                throw new IllegalArgumentException("conflicting duplicate BEGIN metadata");
            }
            tx.beginHash = boundaryHash;
            tx.beginSeen = true;
            tx.beginOffset = record.offset();
        } else if ("END".equals(status)) {
            if (tx.endHash != null && !tx.endHash.equals(boundaryHash)) {
                throw new IllegalArgumentException("conflicting duplicate END metadata");
            }
            tx.endHash = boundaryHash;
            tx.endSeen = true;
            tx.endOffset = record.offset();
            tx.expectedEventCount = root.path("event_count").asInt(-1);
            if (root.path("data_collections").isArray()) {
                tx.expectedCollectionCounts = new java.util.HashMap<>();
                for (JsonNode collection : root.path("data_collections")) {
                    tx.expectedCollectionCounts.merge(requiredText(collection, "data_collection"),
                            collection.path("event_count").asInt(-1), Integer::sum);
                }
            }
            tx.commitPosition = rawId.contains(":") ? rawId.substring(rawId.indexOf(':') + 1) : tx.commitPosition;
        } else {
            throw new IllegalArgumentException("unsupported Debezium transaction status " + status);
        }
    }

    private void processData(ProjectorState state, CaptureRecord record, JsonNode root, long now, Context context) {
        JsonNode source = root.path("source");
        String collection = requiredText(source, "schema") + "." + requiredText(source, "table");
        JsonNode transaction = root.get("transaction");
        boolean snapshot = "r".equals(root.path("op").asText()) && (transaction == null || transaction.isNull());
        String nativeId;
        long sequence;
        if (snapshot) {
            nativeId = "snapshot-" + record.partition() + "-" + record.offset();
            sequence = 1;
        } else {
            if (transaction == null || transaction.isNull()) {
                throw new IllegalArgumentException("non-snapshot record has no transaction metadata at offset " + record.offset());
            }
            nativeId = ProtocolCodec.normalizePostgresTransactionId(requiredText(transaction, "id"));
            sequence = transaction.path("total_order").asLong(-1);
            if (sequence < 1) throw new IllegalArgumentException("invalid source transaction sequence");
        }
        ProjectorState.Assembly tx = snapshot
                ? state.assemblies.computeIfAbsent(nativeId, ignored -> newAssembly(nativeId, now))
                : assembly(state, nativeId, now, context);
        if (snapshot) {
            tx.beginSeen = true;
            tx.endSeen = true;
            tx.expectedEventCount = 1;
            tx.expectedCollectionCounts = Map.of(collection, 1);
            tx.beginOffset = record.offset();
            tx.endOffset = record.offset();
        }
        String changeHash = ProtocolCodec.sha256(root);
        ProjectorState.CapturedChange change = new ProjectorState.CapturedChange();
        change.collection = collection;
        change.sequence = sequence;
        change.before = nullIfMissing(root.get("before"));
        change.after = nullIfMissing(root.get("after"));
        change.operation = requiredText(root, "op");
        change.sourceTimestampMs = root.path("ts_ms").asLong();
        String lsn = source.path("lsn").asText("unknown");
        change.sourceEventId = sourceDatabaseId + "/" + sourceIncarnation + "/" + collection + "/" + lsn + "/" + sequence;
        change.sourcePosition = lsn;
        if (!tx.endSeen) tx.commitPosition = lsn;
        if (AssemblyRules.add(tx, change, changeHash) == AssemblyRules.FragmentResult.IDENTICAL_DUPLICATE) return;
    }

    private void drainPlanningFrontier(ProjectorState state, Collector<OutputRecord> out) {
        while (true) {
            // BEGIN/first-observed order is not source commit order: overlapping database
            // transactions may commit in the opposite order. END offsets form the ordered commit
            // lane. A transaction without END has no assigned commit position and cannot block an
            // END that has already occurred; an incomplete transaction with an earlier END can.
            Map.Entry<String, ProjectorState.Assembly> next = state.assemblies.entrySet().stream()
                    .filter(entry -> entry.getValue().endSeen)
                    .min(java.util.Comparator.comparingLong(entry -> entry.getValue().endOffset))
                    .orElse(null);
            if (next == null) return;
            ProjectorState.Assembly tx = next.getValue();
            if (tx.dlqPublished || !AssemblyRules.complete(tx)) return;
            try {
                SourceStatus sourceStatus = tx.nativeTransactionId.startsWith("snapshot-")
                        ? SourceStatus.SNAPSHOT : SourceStatus.COMMITTED;
                InventoryProjectionPlanner.Planned planned = planner.plan(state, tx, sourceDatabaseId,
                        sourceIncarnation, captureScopeId, sourceStatus);
                for (Mutation mutation : planned.mutations()) {
                    out.collect(new OutputRecord(MUTATION_TOPIC, mutation.transactionId(), ProtocolCodec.json(mutation)));
                }
                out.collect(new OutputRecord(CONTROL_TOPIC, planned.manifest().transactionId(),
                        ProtocolCodec.json(planned.manifest())));
                completedTransactions.inc();
                state.assemblies.remove(next.getKey());
            } catch (RuntimeException protocolFailure) {
                if (!tx.dlqPublished) {
                    out.collect(control(tx, ControlType.FAILED, SourceStatus.FAILED, protocolFailure.getMessage(), DLQ_TOPIC));
                    quarantinedTransactions.inc();
                    tx.dlqPublished = true;
                }
                return;
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<OutputRecord> out) throws Exception {
        ProjectorState state = load();
        for (ProjectorState.Assembly tx : state.assemblies.values()) {
            long age = timestamp - tx.firstSeenEpochMs;
            if (age >= DLQ_AFTER_MS && !tx.dlqPublished) {
                out.collect(control(tx, ControlType.DLQ, SourceStatus.INCOMPLETE,
                        "transaction remained incomplete for 24 hours", DLQ_TOPIC));
                quarantinedTransactions.inc();
                tx.dlqPublished = true;
            } else if (age >= INCOMPLETE_AFTER_MS && !tx.incompletePublished) {
                out.collect(control(tx, ControlType.INCOMPLETE, SourceStatus.INCOMPLETE,
                        incompleteReason(tx), CONTROL_TOPIC));
                incompleteTransactions.inc();
                tx.incompletePublished = true;
            } else if (age >= WARN_AFTER_MS) {
                tx.warned = true; // exported through metrics/logging by the runtime; no state transition at warning age.
                System.err.println("transaction assembly warning transaction_id=" + tx.canonicalTransactionId
                        + " age_ms=" + age + " " + incompleteReason(tx));
            }
        }
        save(state);
    }

    private OutputRecord control(ProjectorState.Assembly tx, ControlType type, SourceStatus status,
                                 String reason, String topic) {
        ControlMessage message = new ControlMessage(VERSION, type, tx.canonicalTransactionId,
                sourceDatabaseId, sourceIncarnation, captureScopeId, tx.nativeTransactionId, status,
                "POSTGRES_LSN", tx.commitPosition, tx.endOffset, GENERATION, 1, 1, null,
                tx.changes.size(), Map.of(), 0, null, null, tx.firstSeenEpochMs,
                tx.firstSeenEpochMs + INCOMPLETE_AFTER_MS, reason);
        return new OutputRecord(topic, tx.canonicalTransactionId, ProtocolCodec.json(message));
    }

    private ProjectorState.Assembly assembly(ProjectorState state, String nativeId, long now, Context context) {
        ProjectorState.Assembly tx = state.assemblies.computeIfAbsent(nativeId, ignored -> newAssembly(nativeId, now));
        context.timerService().registerProcessingTimeTimer(tx.firstSeenEpochMs + WARN_AFTER_MS);
        context.timerService().registerProcessingTimeTimer(tx.firstSeenEpochMs + INCOMPLETE_AFTER_MS);
        context.timerService().registerProcessingTimeTimer(tx.firstSeenEpochMs + DLQ_AFTER_MS);
        return tx;
    }

    private ProjectorState.Assembly newAssembly(String nativeId, long now) {
        ProjectorState.Assembly tx = new ProjectorState.Assembly();
        tx.nativeTransactionId = nativeId;
        tx.canonicalTransactionId = ProtocolCodec.canonicalTransactionId(sourceDatabaseId,
                sourceIncarnation, captureScopeId, nativeId);
        tx.firstSeenEpochMs = now;
        return tx;
    }

    private ProjectorState.Assembly assemblyForFailure(ProjectorState state, CaptureRecord record, JsonNode root) {
        try {
            if (isBoundary(root)) {
                return state.assemblies.get(ProtocolCodec.normalizePostgresTransactionId(requiredText(root, "id")));
            }
            JsonNode transaction = root.get("transaction");
            if (transaction != null && !transaction.isNull()) {
                return state.assemblies.get(ProtocolCodec.normalizePostgresTransactionId(requiredText(transaction, "id")));
            }
            if ("r".equals(root.path("op").asText())) return state.assemblies.get("snapshot-" + record.partition() + "-" + record.offset());
        } catch (RuntimeException ignored) {
            // Without a trustworthy transaction identity there is no safe protocol control record to emit.
        }
        return null;
    }

    private ProjectorState load() throws Exception {
        String json = checkpointedState.value();
        return json == null ? new ProjectorState() : ProtocolCodec.read(json, ProjectorState.class);
    }

    private void save(ProjectorState state) throws Exception { checkpointedState.update(STATE_MAPPER.writeValueAsString(state)); }
    private static boolean isBoundary(JsonNode root) { return root.hasNonNull("status") && root.hasNonNull("id"); }
    private static String incompleteReason(ProjectorState.Assembly tx) {
        return "missing transaction fragments: begin=" + tx.beginSeen + ", end=" + tx.endSeen
                + ", expected=" + tx.expectedEventCount + ", received=" + tx.changes.size()
                + ", expected_collections=" + tx.expectedCollectionCounts
                + ", missing=" + AssemblyRules.missingSequences(tx);
    }
    private static JsonNode nullIfMissing(JsonNode node) { return node == null || node.isNull() ? null : node; }
    private static String requiredText(JsonNode node, String field) {
        if (node == null || !node.hasNonNull(field)) throw new IllegalArgumentException("missing field " + field);
        return node.get(field).asText();
    }
}
