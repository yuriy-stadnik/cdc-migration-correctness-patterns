package com.example.cdc.projector;

import com.example.cdc.protocol.Protocol;
import com.example.cdc.protocol.ProtocolCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.example.cdc.protocol.Protocol.*;

/** Deterministic inventory-v1 projection. It produces at most one net mutation per object per source transaction. */
public final class InventoryProjectionPlanner {
    public record Planned(ControlMessage manifest, List<Mutation> mutations) {}

    private record Candidate(ObjectIdentity object, JsonNode before, JsonNode after,
                             String sourceEventId, String sourcePosition, long sourceSequence) {}

    public Planned plan(ProjectorState state, ProjectorState.Assembly tx,
                        String sourceDatabaseId, String sourceIncarnation, String captureScopeId,
                        SourceStatus sourceStatus) {
        List<Candidate> candidates = new ArrayList<>();
        for (ProjectorState.CapturedChange change : tx.changes.values()) {
            switch (change.collection) {
                case "inventory.customers" -> customer(change, candidates);
                case "inventory.accounts" -> account(change, candidates);
                case "inventory.orders_flat" -> order(change, candidates);
                default -> throw new IllegalArgumentException("unsupported captured collection " + change.collection);
            }
        }

        // A source transaction with two contributions to one projected object is ambiguous in v1.
        Map<String, Candidate> unique = new LinkedHashMap<>();
        for (Candidate candidate : candidates) {
            String key = ProtocolCodec.canonicalObjectKey(candidate.object);
            Candidate prior = unique.putIfAbsent(key, candidate);
            if (prior != null && !sameDesiredState(prior, candidate)) {
                throw new IllegalArgumentException("conflicting projected contributions for " + key);
            }
        }
        List<Candidate> ordered = unique.values().stream()
                .sorted(Comparator.comparing((Candidate c) -> c.object.entity())
                        .thenComparing(c -> ProtocolCodec.canonicalObjectKey(c.object)))
                .toList();

        Map<String, Candidate> writes = new HashMap<>();
        ordered.forEach(c -> writes.put(ProtocolCodec.canonicalObjectKey(c.object), c));
        List<Mutation> mutations = new ArrayList<>();
        long sequence = 0;
        for (Candidate candidate : ordered) {
            String objectKey = ProtocolCodec.canonicalObjectKey(candidate.object);
            VersionToken expected = state.objectVersions.getOrDefault(objectKey,
                    VersionToken.absentBaseline(GENERATION));
            JsonNode desired = candidate.after;
            if ((desired == null || desired.isNull()) && expected.presence() == Presence.ABSENT) continue;
            Operation operation = desired == null || desired.isNull()
                    ? Operation.DELETE
                    : expected.presence() == Presence.PRESENT ? Operation.UPDATE : Operation.INSERT;
            sequence++;
            VersionToken next = new VersionToken(GENERATION, tx.canonicalTransactionId, sequence,
                    operation == Operation.DELETE ? Presence.DELETED : Presence.PRESENT);
            String payloadHash = ProtocolCodec.payloadHash(candidate.before, candidate.after);
            String mutationId = ProtocolCodec.sha256(Map.of(
                    "generation", GENERATION,
                    "transaction", tx.canonicalTransactionId,
                    "sequence", sequence,
                    "object", candidate.object,
                    "payloadHash", payloadHash));

            List<Dependency> dependencies = new ArrayList<>();
            if (!"BASELINE".equals(expected.producerTransactionId())) {
                dependencies.add(new Dependency(DependencyType.WRITE_PREDECESSOR,
                        expected.producerTransactionId(), candidate.object, expected, "previous object revision"));
            }
            addForeignKeyDependencies(state, writes, candidate, dependencies);
            mutations.add(new Mutation(VERSION, tx.canonicalTransactionId, sourceDatabaseId,
                    sourceIncarnation, captureScopeId, GENERATION, 1, sequence, mutationId,
                    candidate.object, operation, List.of(candidate.sourceEventId),
                    List.of(candidate.sourceSequence), List.of(new SourcePosition("POSTGRES_LSN", candidate.sourcePosition)),
                    expected, expected, next, dependencies,
                    mutationId, payloadHash, candidate.before, candidate.after));
        }

        String mutationSetHash = ProtocolCodec.mutationSetHash(mutations);
        Map<String, Integer> collectionCounts = new TreeMap<>();
        tx.changes.values().forEach(c -> collectionCounts.merge(c.collection, 1, Integer::sum));
        long now = tx.firstSeenEpochMs;
        ControlMessage unsigned = new ControlMessage(VERSION, ControlType.MANIFEST,
                tx.canonicalTransactionId, sourceDatabaseId, sourceIncarnation, captureScopeId,
                tx.nativeTransactionId, sourceStatus, "POSTGRES_LSN", tx.commitPosition,
                tx.endOffset, GENERATION, 1, 1, null, tx.changes.size(), collectionCounts,
                mutations.size(), mutationSetHash, "PENDING", now, now + 600_000, null);
        String manifestHash = ProtocolCodec.sha256(unsigned);
        ControlMessage manifest = new ControlMessage(VERSION, ControlType.MANIFEST,
                tx.canonicalTransactionId, sourceDatabaseId, sourceIncarnation, captureScopeId,
                tx.nativeTransactionId, sourceStatus, "POSTGRES_LSN", tx.commitPosition,
                tx.endOffset, GENERATION, 1, 1, null, tx.changes.size(), collectionCounts,
                mutations.size(), mutationSetHash, manifestHash, now, now + 600_000, null);

        // State advances only after every validation and hash succeeds.
        for (Mutation mutation : mutations) {
            String key = ProtocolCodec.canonicalObjectKey(mutation.object());
            state.objectVersions.put(key, mutation.newObjectVersion());
            if (mutation.operation() == Operation.DELETE) state.objectPayloads.remove(key);
            else state.objectPayloads.put(key, mutation.after());
        }
        state.lastPlannedCommitSequence = tx.endOffset;
        return new Planned(manifest, List.copyOf(mutations));
    }

    private static boolean sameDesiredState(Candidate a, Candidate b) {
        return java.util.Objects.equals(a.after, b.after);
    }

    private static void customer(ProjectorState.CapturedChange c, List<Candidate> out) {
        JsonNode row = row(c);
        if (row == null) return;
        transition(out, "client.customers", key(c.before, "id", "int64"), projectCustomer(c.before),
                key(c.after, "id", "int64"), projectCustomer(c.after), c);
    }

    private static void account(ProjectorState.CapturedChange c, List<Candidate> out) {
        JsonNode row = row(c);
        if (row == null) return;
        transition(out, "client.addresses", accountKey(c.before, "address_type", "HOME"), projectAddress(c.before),
                accountKey(c.after, "address_type", "HOME"), projectAddress(c.after), c);
        for (String type : List.of("HOME", "WORK", "MOBILE")) {
            transition(out, "operational.contact_numbers", accountKey(c.before, "phone_type", type), projectPhone(c.before, type),
                    accountKey(c.after, "phone_type", type), projectPhone(c.after, type), c);
        }
    }

    private static void order(ProjectorState.CapturedChange c, List<Candidate> out) {
        JsonNode row = row(c);
        if (row == null) return;
        transition(out, "operational.orders", orderKey(c.before), projectOrder(c.before),
                orderKey(c.after), projectOrder(c.after), c);
        transition(out, "operational.order_items", itemKey(c.before), projectItem(c.before),
                itemKey(c.after), projectItem(c.after), c);
        // Products are shared projections. Deletes do not delete the catalog object; conflicting writes quarantine.
        if (c.after != null && !c.after.isNull()) {
            String productName = text(c.after, "product_name");
            out.add(new Candidate(identity("operational.products", "name", "text", productName),
                    projectProduct(c.before), projectProduct(c.after), c.sourceEventId, c.sourcePosition, c.sequence));
        }
    }

    private static void transition(List<Candidate> out, String entity,
                                   Map<String, TypedValue> beforeKey, JsonNode beforePayload,
                                   Map<String, TypedValue> afterKey, JsonNode afterPayload,
                                   ProjectorState.CapturedChange change) {
        if (beforeKey != null && afterKey != null && beforeKey.equals(afterKey)) {
            out.add(new Candidate(identity(entity, beforeKey), beforePayload, afterPayload, change.sourceEventId, change.sourcePosition, change.sequence));
        } else {
            if (beforeKey != null && beforePayload != null) out.add(new Candidate(identity(entity, beforeKey), beforePayload, null, change.sourceEventId, change.sourcePosition, change.sequence));
            if (afterKey != null && afterPayload != null) out.add(new Candidate(identity(entity, afterKey), null, afterPayload, change.sourceEventId, change.sourcePosition, change.sequence));
        }
    }

    private static Map<String, TypedValue> key(JsonNode row, String field, String type) {
        String value = text(row, field);
        return value == null ? null : Map.of(field, new TypedValue(type, value));
    }

    private static Map<String, TypedValue> accountKey(JsonNode row, String discriminator, String value) {
        String customer = text(row, "customer_id");
        return customer == null ? null : Map.of("customer_id", new TypedValue("int64", customer), discriminator, new TypedValue("text", value));
    }

    private static Map<String, TypedValue> itemKey(JsonNode row) {
        String order = text(row, "order_id"), product = text(row, "product_name");
        return order == null || product == null ? null : Map.of("order_id", new TypedValue("int64", order), "product_name", new TypedValue("text", product));
    }

    private static Map<String, TypedValue> orderKey(JsonNode row) {
        String id = text(row, "order_id");
        return id == null ? null : Map.of("id", new TypedValue("int64", id));
    }

    private static void addForeignKeyDependencies(ProjectorState state, Map<String, Candidate> writes,
                                                   Candidate candidate, List<Dependency> dependencies) {
        List<ObjectIdentity> parents = new ArrayList<>();
        JsonNode row = candidate.after;
        if (row == null || row.isNull()) return;
        switch (candidate.object.entity()) {
            case "client.addresses" -> parents.add(identity("client.customers", "id", "int64", text(row, "customer_id")));
            case "operational.orders" -> parents.add(identity("client.customers", "id", "int64", text(row, "customer_id")));
            case "operational.order_items" -> {
                parents.add(identity("operational.orders", "id", "int64", text(row, "order_id")));
                parents.add(identity("operational.products", "name", "text", text(row, "product_name")));
            }
            default -> { return; }
        }
        for (ObjectIdentity parent : parents) {
            String key = ProtocolCodec.canonicalObjectKey(parent);
            Candidate sameTx = writes.get(key);
            if (sameTx != null && sameTx.after != null && !sameTx.after.isNull()) continue;
            VersionToken version = state.objectVersions.get(key);
            dependencies.add(new Dependency(DependencyType.EXISTS,
                    version == null ? null : version.producerTransactionId(), parent, null,
                    "destination foreign-key parent"));
        }
    }

    private static JsonNode row(ProjectorState.CapturedChange c) {
        return c.after != null && !c.after.isNull() ? c.after : c.before;
    }

    private static ObjectIdentity identity(String entity, String key, String type, String value) {
        return identity(entity, Map.of(key, new TypedValue(type, value)));
    }

    private static ObjectIdentity identity(String entity, Map<String, TypedValue> key) {
        return new ObjectIdentity(entity, key);
    }

    private static ObjectNode object() { return ProtocolCodec.mapper().createObjectNode(); }
    private static String text(JsonNode node, String field) {
        if (node == null || node.isNull() || node.get(field) == null || node.get(field).isNull()) return null;
        return node.get(field).asText();
    }
    private static void copy(ObjectNode target, JsonNode source, String... fields) {
        for (String field : fields) if (source != null && source.has(field)) target.set(field, source.get(field));
    }
    private static JsonNode projectCustomer(JsonNode row) {
        if (row == null || row.isNull()) return null;
        ObjectNode o = object(); copy(o, row, "id", "first_name", "last_name", "email", "status", "created_at", "updated_at"); return o;
    }
    private static JsonNode projectAddress(JsonNode row) {
        if (row == null || row.isNull() || text(row, "street") == null) return null;
        ObjectNode o = object(); copy(o, row, "customer_id", "street", "city", "state", "zip_code"); o.put("address_type", "HOME"); return o;
    }
    private static JsonNode projectPhone(JsonNode row, String type) {
        String sourceField = type.toLowerCase() + "_phone";
        if (row == null || row.isNull() || text(row, sourceField) == null) return null;
        ObjectNode o = object(); copy(o, row, "customer_id"); o.put("phone_type", type); o.put("phone_number", text(row, sourceField)); return o;
    }
    private static JsonNode projectProduct(JsonNode row) {
        if (row == null || row.isNull()) return null;
        ObjectNode o = object(); o.put("name", text(row, "product_name")); o.put("category", text(row, "product_category"));
        if (row.has("unit_price")) o.set("current_price", row.get("unit_price")); return o;
    }
    private static JsonNode projectOrder(JsonNode row) {
        if (row == null || row.isNull()) return null;
        ObjectNode o = object(); o.put("id", text(row, "order_id")); copy(o, row, "customer_id", "order_date"); o.put("status", "COMPLETED"); return o;
    }
    private static JsonNode projectItem(JsonNode row) {
        if (row == null || row.isNull()) return null;
        ObjectNode o = object(); o.put("order_id", text(row, "order_id")); o.put("product_name", text(row, "product_name")); copy(o, row, "quantity");
        if (row.has("unit_price")) o.set("price_at_purchase", row.get("unit_price")); return o;
    }
}
