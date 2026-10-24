package com.example.cdc.projector;

import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import static com.example.cdc.protocol.ProtocolV2.*;
import static com.example.cdc.projector.V2Capture.*;

/** Deterministic planning over verified source revisions. No clock, Kafka offset, or generated row version. */
public final class V2Projector {
    private final String database, incarnation, scope;
    private final V2Capture capture;
    public V2Projector(String database, String incarnation, String scope) {
        this.database = database; this.incarnation = incarnation; this.scope = scope;
        capture = new V2Capture(database, incarnation);
    }
    public List<OutputRecord> accept(V2ProjectorState state, CaptureRecord record) {
        List<OutputRecord> output = new ArrayList<>();
        String failureKey = "record:" + record.topic() + ":" + record.partition() + ":" + record.offset();
        try {
            String lane = ProtocolV2Codec.json(List.of(database, incarnation, scope, GENERATION));
            if (state.formatVersion != 2 || state.lane != null && !state.lane.equals(lane))
                throw new IllegalArgumentException("incompatible projector checkpoint lane");
            state.lane = lane;
            if (!record.topic().equals(CAPTURE_TOPIC) || record.partition() != 0)
                throw new IllegalArgumentException("v2 requires the registered single capture partition");
            JsonNode root = immutable(parse(record.value()));
            if (boundary(root) && !Set.of("BEGIN", "END").contains(text(root, "status")))
                throw new IllegalArgumentException("unsupported transaction boundary");
            if (root.path("op").asText().equals("r")) return output;
            String key = observationKey(root); failureKey = key;
            JsonNode old = state.observations.get(key);
            if (old != null && !ProtocolV2Codec.json(old).equals(ProtocolV2Codec.json(root)))
                throw new IllegalArgumentException("conflicting duplicate capture occurrence");
            if (old == null) state.observations.put(key, root);
            assemble(state, output);
            drain(state, output);
        } catch (RuntimeException failure) {
            quarantine(state, failureKey, failure.getMessage(), output);
            String tx = state.observationTransactions.get(failureKey);
            if (tx != null) {
                quarantine(state, tx, "immutable capture evidence conflicted", output);
                if (state.completed.containsKey(tx)) state.haltedReason = "completed transaction capture conflict: " + tx;
            }
        }
        return output;
    }

    private void assemble(V2ProjectorState state, List<OutputRecord> output) {
        List<JsonNode> ends = state.observations.values().stream().filter(V2Capture::boundary)
                .filter(n -> text(n, "status").equals("END")).sorted(Comparator.comparing(V2Capture::position)).toList();
        for (JsonNode end : ends) {
            String endKey = observationKey(end);
            if (state.assembledEnds.contains(endKey) || state.quarantine.containsKey(endKey)) continue;
            List<JsonNode> begins = state.observations.values().stream().filter(V2Capture::boundary)
                    .filter(n -> text(n, "status").equals("BEGIN") && xid(n).equals(xid(end))
                            && position(n).compareTo(position(end)) <= 0)
                    .sorted(Comparator.comparing(V2Capture::position).reversed()).toList();
            if (begins.isEmpty()) { state.waiting.put(endKey, "missing BEGIN occurrence evidence"); continue; }
            JsonNode begin = begins.get(0);
            // A reused XID cannot share overlapping BEGIN/END intervals. Never merge by bare XID.
            long matches = ends.stream().filter(e -> xid(e).equals(xid(end))
                    && position(e).compareTo(position(begin)) >= 0 && position(e).compareTo(position(end)) <= 0).count();
            if (matches != 1) { quarantine(state, endKey, "ambiguous source occurrence boundaries", output); continue; }
            List<JsonNode> rows = state.observations.values().stream().filter(n -> !boundary(n))
                    .filter(n -> xid(n).equals(xid(end)) && position(n).compareTo(position(begin)) >= 0
                            && position(n).compareTo(position(end)) <= 0).toList();
            try {
                if (state.quarantine.containsKey(observationKey(begin))
                        || rows.stream().anyMatch(r -> state.quarantine.containsKey(observationKey(r))))
                    throw new IllegalArgumentException("source occurrence contains conflicting capture evidence");
                if (rows.size() > number(end, "event_count")) throw new IllegalArgumentException("too many source fragments");
                V2ProjectorState.Pending pending = capture.assemble(begin, end, rows);
                if (pending == null) { state.waiting.put(endKey, "missing capture fragments/sequence gap"); continue; }
                V2ProjectorState.Completed completed = state.completed.get(pending.transactionId);
                if (completed != null) {
                    if (!completed.captureEvidence.equals(pending.captureEvidence)) {
                        state.haltedReason = "completed transaction evidence changed: " + pending.transactionId;
                        quarantine(state, pending.transactionId, state.haltedReason, output);
                        throw new IllegalArgumentException(state.haltedReason);
                    }
                    state.assembledEnds.add(endKey); state.waiting.remove(endKey); continue;
                }
                V2ProjectorState.Pending prior = state.pending.get(pending.transactionId);
                if (prior != null && !prior.captureEvidence.equals(pending.captureEvidence))
                    throw new IllegalArgumentException("source UUID reused for different transaction occurrence");
                // Validate all claims first; publication to the index is atomic at transaction granularity.
                Map<String, V2ProjectorState.Producer> claims = new TreeMap<>();
                for (V2ProjectorState.Net net : pending.nets) for (JournalRevision revision : net.revisions) {
                    var p = new V2ProjectorState.Producer(); p.owner = net.owner; p.state = revision.after();
                    p.finalRevision = revision.after().equals(net.result);
                    p.reference = new ProducerReference(ProducerType.TRANSACTION, pending.transactionId);
                    p.commitPosition = pending.boundary.commitPosition();
                    p.payload = p.finalRevision ? net.after : null;
                    String key = producerKey(net.owner, p.state.version());
                    var existing = state.producers.get(key);
                    if (existing != null && (!existing.reference.equals(p.reference)
                            || !existing.state.equals(p.state) || !same(existing.payload,p.payload)))
                        throw new IllegalArgumentException("multiple producers claim source owner revision");
                    claims.put(key, p);
                }
                state.producers.putAll(claims); state.pending.put(pending.transactionId, pending);
                state.observationTransactions.put(observationKey(begin), pending.transactionId);
                state.observationTransactions.put(endKey, pending.transactionId);
                for (JsonNode row : rows) state.observationTransactions.put(observationKey(row), pending.transactionId);
                state.assembledEnds.add(endKey); state.waiting.remove(endKey);
            } catch (RuntimeException failure) { quarantine(state, endKey, failure.getMessage(), output); }
        }
    }
    private String producerKey(OwnerIdentity owner, Long version) {
        return ProtocolV2Codec.json(List.of(scope, GENERATION, owner, version));
    }
    private V2ProjectorState.Producer predecessor(V2ProjectorState state, V2ProjectorState.Pending tx, V2ProjectorState.Net net) {
        if (net.beforeState.presence() == Presence.ABSENT) {
            V2ProjectorState.Producer p = new V2ProjectorState.Producer();
            p.owner = net.owner; p.state = net.beforeState; p.finalRevision = true;
            p.reference = new ProducerReference(ProducerType.BASELINE, EMPTY_BASELINE_ID);
            return p;
        }
        var p = state.producers.get(producerKey(net.owner, net.beforeState.version()));
        if (p == null) return null;
        if (!p.state.equals(net.beforeState) || !p.finalRevision)
            throw new IllegalArgumentException("predecessor is intermediate or has different presence");
        if (!same(p.payload, net.before)) throw new IllegalArgumentException("predecessor payload disagrees with source journal");
        if (p.reference.id().equals(tx.transactionId)) throw new IllegalArgumentException("self predecessor");
        return p;
    }

    private void drain(V2ProjectorState state, List<OutputRecord> output) {
        if (state.haltedReason != null) return;
        boolean progress;
        do {
            progress = false;
            for (var tx : new ArrayList<>(state.pending.values())) {
                if (state.quarantine.containsKey(tx.transactionId)) continue;
                try {
                    Set<String> edges = new TreeSet<>();
                    boolean missing = false;
                    for (var net : tx.nets) {
                        var p = predecessor(state, tx, net);
                        if (p == null) { missing = true; continue; }
                        if (p.reference.type() == ProducerType.TRANSACTION) edges.add(p.reference.id());
                    }
                    state.edges.put(tx.transactionId, edges);
                    Set<String> cycle = cycleMembers(state.edges);
                    for (String member : cycle) quarantine(state, member, "source owner dependency cycle", output);
                    if (state.quarantine.containsKey(tx.transactionId)) continue;
                    if (missing || edges.stream().anyMatch(id -> !state.completed.containsKey(id) || state.quarantine.containsKey(id))) {
                        state.waiting.put(tx.transactionId, missing ? "unresolved source revision producer" : "predecessor not yet planned");
                        continue;
                    }
                    List<OutputRecord> planned = plan(state, tx);
                    var completed = new V2ProjectorState.Completed(); completed.captureEvidence = tx.captureEvidence; completed.output = planned;
                    completed.mutationSetHash = ProtocolV2Codec.readManifest(CONTROL_TOPIC, planned.get(planned.size()-1).value()).mutationSetHash();
                    state.completed.put(tx.transactionId, completed); state.pending.remove(tx.transactionId); state.waiting.remove(tx.transactionId);
                    output.addAll(planned); progress = true;
                } catch (Unresolved unresolved) { state.waiting.put(tx.transactionId, unresolved.getMessage()); }
                catch (RuntimeException failure) { quarantine(state, tx.transactionId, failure.getMessage(), output); }
            }
        } while (progress);
    }
    static Set<String> cycleMembers(Map<String, Set<String>> graph) {
        Set<String> cycle = new TreeSet<>();
        for (String start : graph.keySet()) {
            Set<String> visited = new HashSet<>(); Deque<String> work = new ArrayDeque<>(graph.get(start));
            while (!work.isEmpty()) {
                String next = work.pop();
                if (next.equals(start)) { cycle.add(start); break; }
                if (visited.add(next)) work.addAll(graph.getOrDefault(next, Set.of()));
            }
        }
        return cycle;
    }
    private List<OutputRecord> plan(V2ProjectorState state, V2ProjectorState.Pending tx) {
        List<OwnerTransition> transitions = new ArrayList<>();
        List<Dependency> dependencies = new ArrayList<>();
        Map<String, V2ProjectorState.Net> finalStates = new TreeMap<>();
        for (var net : tx.nets) {
            var p = predecessor(state, tx, net);
            if (p == null) throw new Unresolved("unresolved predecessor");
            if (p.commitPosition != null && lsn(p.commitPosition).compareTo(lsn(tx.boundary.commitPosition())) >= 0)
                throw new IllegalArgumentException("producer occurs after dependent source commit");
            var expected = new Predecessor(net.beforeState, p.reference);
            transitions.add(new OwnerTransition(tx.transactionId, net.owner, expected, net.result, net.revisions, net.before, net.after));
            dependencies.add(new Dependency(DependencyType.WRITE_PREDECESSOR,
                    net.owner, net.beforeState, p.reference, "source row predecessor"));
            finalStates.put(ProtocolV2Codec.json(net.owner), net);
        }
        // Presence is checked against the source state as of this transaction, not the latest arrival.
        for (var net : tx.nets) if (target(net.owner) != null && net.result.presence() == Presence.PRESENT) {
            for (OwnerIdentity parent : parents(net.owner, net.after)) {
                var sameTx = finalStates.get(ProtocolV2Codec.json(parent));
                if (sameTx != null) {
                    if (sameTx.result.presence() != Presence.PRESENT) throw new IllegalArgumentException("same-transaction parent deleted");
                    continue;
                }
                V2ProjectorState.Producer parentState = parentPresence(state, parent, tx.boundary.commitPosition());
                if (parentState == null) throw new Unresolved("unresolved parent presence");
                if (parentState.state.presence() != Presence.PRESENT) throw new IllegalArgumentException("parent absent at source boundary");
                dependencies.add(new Dependency(DependencyType.EXISTS, parent, parentState.state, parentState.reference, "foreign-key presence"));
                if (parentState.reference.type() == ProducerType.TRANSACTION)
                    state.edges.get(tx.transactionId).add(parentState.reference.id());
            }
        }
        // Validate surviving children when a parent is deleted, using source commit evidence.
        Map<String, V2ProjectorState.Producer> known = new TreeMap<>();
        for (var p : state.producers.values()) {
            if (!p.finalRevision || p.commitPosition == null || lsn(p.commitPosition).compareTo(lsn(tx.boundary.commitPosition())) >= 0
                    || p.reference.type() == ProducerType.TRANSACTION && !state.completed.containsKey(p.reference.id())) continue;
            String key = ProtocolV2Codec.json(p.owner);
            var prior = known.get(key);
            if (prior == null || lsn(p.commitPosition).compareTo(lsn(prior.commitPosition)) > 0) known.put(key, p);
        }
        for (var deleted : tx.nets) if (deleted.result.presence() == Presence.DELETED && target(deleted.owner) != null) {
            for (var p : known.values()) {
                var changed = finalStates.get(ProtocolV2Codec.json(p.owner));
                OwnerState finalState = changed == null ? p.state : changed.result;
                JsonNode payload = changed == null ? p.payload : changed.after;
                if (finalState.presence() == Presence.PRESENT && parents(p.owner, payload).contains(deleted.owner))
                    throw new IllegalArgumentException("parent deletion leaves surviving child");
            }
        }
        dependencies.sort(Comparator.comparing(ProtocolV2Codec::json));
        List<Mutation> mutations = new ArrayList<>(); long ordinal = 0;
        for (OwnerTransition transition : transitions) {
            ObjectIdentity object = target(transition.owner());
            if (object == null) continue;
            Operation operation = transition.result().presence() == Presence.PRESENT
                    ? transition.expected().state().presence() == Presence.PRESENT ? Operation.UPDATE : Operation.INSERT
                    : Operation.DELETE;
            mutations.add(ProtocolV2Codec.signMutation(new Mutation(VERSION, SCHEMA_VERSION, RecordType.MUTATION,
                    tx.transactionId, database, incarnation, scope, GENERATION, EMPTY_BASELINE_ID,
                    ++ordinal, "PENDING", "PENDING", transition.owner(), object, operation, transition.expected(), transition.result(),
                    dependencies, transition.before(), transition.after())));
        }
        Manifest manifest = ProtocolV2Codec.signManifest(new Manifest(VERSION, SCHEMA_VERSION, RecordType.MANIFEST,
                tx.transactionId, database, incarnation, scope, GENERATION, EMPTY_BASELINE_ID, tx.boundary,
                transitions.size(), mutations.size(), transitions, "PENDING"), mutations);
        List<OutputRecord> output = new ArrayList<>();
        for (Mutation mutation : mutations) output.add(new OutputRecord(MUTATION_TOPIC, tx.transactionId, ProtocolV2Codec.json(mutation)));
        output.add(new OutputRecord(CONTROL_TOPIC, tx.transactionId, ProtocolV2Codec.json(manifest)));
        return output;
    }
    private V2ProjectorState.Producer parentPresence(V2ProjectorState state, OwnerIdentity parent, String commitPosition) {
        V2ProjectorState.Producer result = null;
        for (var producer : state.producers.values()) {
            if (!producer.owner.equals(parent) || !producer.finalRevision || producer.commitPosition == null
                    || lsn(producer.commitPosition).compareTo(lsn(commitPosition)) >= 0
                    || producer.reference.type() == ProducerType.TRANSACTION
                        && !state.completed.containsKey(producer.reference.id())) continue;
            if (result == null || lsn(producer.commitPosition).compareTo(lsn(result.commitPosition)) > 0) result = producer;
        }
        return result;
    }
    private ObjectIdentity target(OwnerIdentity owner) {
        String entity = switch (owner.collection()) {
            case "inventory.customers" -> "client.customers";
            case "inventory.orders" -> "operational.orders";
            case "inventory.order_items" -> "operational.order_items";
            case "inventory.products" -> "operational.products";
            default -> null;
        };
        return entity == null ? null : new ObjectIdentity(entity, owner.primaryKey());
    }
    private List<OwnerIdentity> parents(OwnerIdentity owner, JsonNode payload) {
        List<OwnerIdentity> parents = new ArrayList<>();
        switch (owner.collection()) {
            case "inventory.orders" -> {
                if (payload.hasNonNull("customer_id")) {
                    var customer = ProtocolV2Codec.mapper().createObjectNode(); customer.set("id", payload.get("customer_id"));
                    parents.add(capture.owner("inventory.customers", customer));
                }
            }
            case "inventory.order_items" -> {
                var order = ProtocolV2Codec.mapper().createObjectNode(); order.set("id", payload.get("order_id"));
                var product = ProtocolV2Codec.mapper().createObjectNode(); product.set("name", payload.get("product_name"));
                parents.add(capture.owner("inventory.orders", order)); parents.add(capture.owner("inventory.products", product));
            }
            default -> { }
        }
        return parents;
    }
    private void quarantine(V2ProjectorState state, String key, String reason, List<OutputRecord> output) {
        if (state.quarantine.putIfAbsent(key, reason == null ? "invalid source evidence" : reason) == null) {
            output.add(new OutputRecord(DLQ_TOPIC, key, ProtocolV2Codec.json(Map.of("protocolVersion", VERSION,
                    "recordType", "CAPTURE_FAILURE", "sourceDatabaseId", database, "sourceIncarnation", incarnation,
                    "captureScopeId", scope, "projectionGeneration", GENERATION, "evidenceKey", key,
                    "reason", state.quarantine.get(key)))));
        }
    }
    private static final class Unresolved extends RuntimeException { Unresolved(String message) { super(message); } }
}
