package com.example.cdc.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/** The only supported wire contract. Every source owner maps one-to-one to a destination row. */
public final class ProtocolV2 {
    public static final int VERSION = 2;
    public static final int SCHEMA_VERSION = 2;
    public static final String CAPTURE_TOPIC = "cdc.source.capture.v2";
    public static final String CONTROL_TOPIC = "cdc.tx.control.v2";
    public static final String MUTATION_TOPIC = "cdc.tx.mutations.v2";
    public static final String BASELINE_TOPIC = "cdc.tx.baseline.v2";
    public static final String DLQ_TOPIC = "cdc.tx.dlq.v2";
    public static final String GENERATION = "inventory-direct-v2";

    private ProtocolV2() {}

    public enum RecordType { MUTATION, MANIFEST, BASELINE, BASELINE_OWNER }
    public enum Presence { ABSENT, PRESENT, DELETED }
    public enum ProducerType { TRANSACTION, BASELINE }
    public enum Operation { INSERT, UPDATE, DELETE }
    public enum DependencyType { WRITE_PREDECESSOR, AFTER_TRANSACTION, EXISTS, NOT_EXISTS }
    public enum BaselineStatus { PREPARING, RECONCILING, ACTIVE, RETIRED, FAILED }

    static void require(String value, String label) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(label + " is required");
    }

    static void requireVersion(int protocolVersion, int schemaVersion, RecordType actual, RecordType expected) {
        if (protocolVersion != VERSION || schemaVersion != SCHEMA_VERSION || actual != expected)
            throw new IllegalArgumentException("unsupported or mixed protocol/schema/record type");
    }

    private static Map<String, String> keyShape(String collection) {
        return switch (collection) {
            case "inventory.customers", "client.customers", "inventory.orders", "operational.orders" ->
                    Map.of("id", "int64");
            case "inventory.addresses", "client.addresses" ->
                    Map.of("customer_id", "int64", "address_type", "text");
            case "inventory.contact_numbers", "operational.contact_numbers" ->
                    Map.of("customer_id", "int64", "phone_type", "text");
            case "inventory.order_items", "operational.order_items" ->
                    Map.of("order_id", "int64", "product_name", "text");
            case "inventory.products", "operational.products" -> Map.of("name", "text");
            default -> throw new IllegalArgumentException("unsupported key collection " + collection);
        };
    }

    private static void validateKey(String collection, Map<String, TypedValue> key) {
        Map<String, String> expected = keyShape(collection);
        if (!key.keySet().equals(expected.keySet()) || key.entrySet().stream().anyMatch(e ->
                e.getValue() == null || !expected.get(e.getKey()).equals(e.getValue().type())))
            throw new IllegalArgumentException("incorrect typed key for " + collection);
        if ((collection.equals("inventory.contact_numbers") || collection.equals("operational.contact_numbers"))
                && !List.of("HOME", "WORK", "MOBILE").contains(key.get("phone_type").value()))
            throw new IllegalArgumentException("unsupported phone type");
    }

    private static void validateImageVersion(JsonNode image, OwnerState state) {
        if (image == null || image.isNull()) return;
        if (!image.isObject()) throw new IllegalArgumentException("owner image must be an object");
        JsonNode version = image.get("version");
        if (version != null && (!version.isIntegralNumber() || !version.canConvertToLong()
                || state.version() == null || version.longValue() != state.version()))
            throw new IllegalArgumentException("owner image version mismatch");
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record TypedValue(String type, String value) {
        public TypedValue {
            require(type, "key type");
            if (value == null) throw new IllegalArgumentException("key value is required");
            if (type.equals("int64")) {
                try {
                    if (!Long.toString(Long.parseLong(value)).equals(value))
                        throw new IllegalArgumentException("noncanonical int64 key");
                } catch (NumberFormatException e) { throw new IllegalArgumentException("invalid int64 key", e); }
            } else if (!type.equals("text")) throw new IllegalArgumentException("unsupported key type " + type);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record ObjectIdentity(String entity, Map<String, TypedValue> primaryKey) {
        public ObjectIdentity {
            require(entity, "destination entity");
            if (!List.of("client.customers", "client.addresses", "operational.contact_numbers",
                    "operational.orders", "operational.order_items", "operational.products").contains(entity))
                throw new IllegalArgumentException("unsupported destination entity " + entity);
            primaryKey = Map.copyOf(primaryKey);
            validateKey(entity, primaryKey);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record OwnerIdentity(String sourceDatabaseId, String sourceIncarnation,
                                String collection, Map<String, TypedValue> primaryKey) {
        public OwnerIdentity {
            require(sourceDatabaseId, "source database id");
            require(sourceIncarnation, "source incarnation");
            require(collection, "owner collection");
            if (!List.of("inventory.customers", "inventory.addresses", "inventory.contact_numbers",
                    "inventory.orders", "inventory.order_items", "inventory.products").contains(collection))
                throw new IllegalArgumentException("unsupported owner collection " + collection);
            primaryKey = Map.copyOf(primaryKey);
            validateKey(collection, primaryKey);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record OwnerState(Presence presence, Long version) {
        public OwnerState {
            if (presence == null) throw new IllegalArgumentException("presence is required");
            if (presence == Presence.ABSENT ? version != null : version == null || version <= 0)
                throw new IllegalArgumentException("invalid owner version for " + presence);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record ProducerReference(ProducerType type, String id) {
        public ProducerReference {
            if (type == null) throw new IllegalArgumentException("producer type is required");
            require(id, "producer id");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record Predecessor(OwnerState state, ProducerReference producer) {
        public Predecessor {
            if (state == null || producer == null) throw new IllegalArgumentException("predecessor state and producer required");
            if (state.presence() == Presence.ABSENT && producer.type() != ProducerType.BASELINE)
                throw new IllegalArgumentException("absence requires baseline proof");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record Dependency(DependencyType type, OwnerIdentity owner, OwnerState requiredState,
                             ProducerReference producer, String reason) {
        public Dependency {
            if (type == null || owner == null || requiredState == null || producer == null)
                throw new IllegalArgumentException("complete dependency required");
            require(reason, "dependency reason");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record JournalRevision(long ordinal, OwnerState before, OwnerState after) {
        public JournalRevision {
            if (ordinal < 1 || before == null || after == null || after.presence() == Presence.ABSENT)
                throw new IllegalArgumentException("invalid journal revision");
            long expected;
            try { expected = before.version() == null ? 1 : Math.addExact(before.version(), 1); }
            catch (ArithmeticException e) { throw new IllegalArgumentException("source version overflow", e); }
            if (after.version() != expected) throw new IllegalArgumentException("discontinuous source revision");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record OwnerTransition(String transactionId, OwnerIdentity owner, Predecessor expected,
                                  OwnerState result, List<JournalRevision> revisions,
                                  JsonNode before, JsonNode after) {
        public OwnerTransition {
            require(transactionId, "transition transaction id");
            if (owner == null || expected == null || result == null || revisions == null || revisions.isEmpty())
                throw new IllegalArgumentException("complete source transition required");
            revisions = List.copyOf(revisions);
            if (!revisions.get(0).before().equals(expected.state())
                    || !revisions.get(revisions.size() - 1).after().equals(result))
                throw new IllegalArgumentException("journal endpoints disagree with transition");
            long priorOrdinal = 0; OwnerState prior = null;
            for (JournalRevision revision : revisions) {
                if (revision.ordinal() <= priorOrdinal || prior != null && !prior.equals(revision.before()))
                    throw new IllegalArgumentException("journal chain is unordered or discontinuous");
                priorOrdinal = revision.ordinal(); prior = revision.after();
            }
            if (result.presence() == Presence.DELETED && after != null && !after.isNull())
                throw new IllegalArgumentException("deleted owner cannot have an after image");
            validateImageVersion(before, expected.state());
            validateImageVersion(after, result);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record Mutation(int protocolVersion, int schemaVersion, RecordType recordType,
                           String transactionId, String sourceDatabaseId, String sourceIncarnation,
                           String captureScopeId, String projectionGeneration, String baselineId,
                           long mutationSequence, String mutationId, String idempotencyKey,
                           OwnerIdentity owner, ObjectIdentity object, Operation operation,
                           Predecessor expected, OwnerState result, List<Dependency> dependencies,
                           JsonNode before, JsonNode after) {
        public Mutation {
            requireVersion(protocolVersion, schemaVersion, recordType, RecordType.MUTATION);
            require(transactionId, "transaction id"); require(sourceDatabaseId, "source database id");
            require(sourceIncarnation, "source incarnation"); require(captureScopeId, "capture scope id");
            require(projectionGeneration, "projection generation"); require(baselineId, "baseline id");
            require(mutationId, "mutation id"); require(idempotencyKey, "idempotency key");
            if (mutationSequence < 1 || owner == null || object == null || operation == null
                    || expected == null || result == null) throw new IllegalArgumentException("incomplete mutation descriptor");
            dependencies = List.copyOf(dependencies);
            if (!sourceDatabaseId.equals(owner.sourceDatabaseId()) || !sourceIncarnation.equals(owner.sourceIncarnation()))
                throw new IllegalArgumentException("mutation owner source identity mismatch");
            String target = switch (owner.collection()) {
                case "inventory.customers" -> "client.customers";
                case "inventory.addresses" -> "client.addresses";
                case "inventory.contact_numbers" -> "operational.contact_numbers";
                case "inventory.orders" -> "operational.orders";
                case "inventory.order_items" -> "operational.order_items";
                case "inventory.products" -> "operational.products";
                default -> throw new IllegalArgumentException("unsupported source owner");
            };
            if (!target.equals(object.entity()) || !object.primaryKey().equals(owner.primaryKey()))
                throw new IllegalArgumentException("destination object must use its source row primary key");
            if (operation == Operation.DELETE) {
                if (result.presence() != Presence.DELETED || after != null && !after.isNull()
                        || expected.state().presence() != Presence.PRESENT)
                    throw new IllegalArgumentException("invalid delete transition");
            } else if (result.presence() != Presence.PRESENT || after == null || after.isNull()
                    || operation == Operation.INSERT && expected.state().presence() == Presence.PRESENT
                    || operation == Operation.UPDATE && expected.state().presence() != Presence.PRESENT)
                throw new IllegalArgumentException("invalid present transition");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record SourceBoundary(String sourceTransactionUuid, String nativeTransactionId,
                                 String beginPosition, String commitPosition, long journalCount,
                                 int capturedSourceEventCount, Map<String, Integer> sourceCollectionCounts) {
        public SourceBoundary {
            try {
                if (!UUID.fromString(sourceTransactionUuid).toString().equals(sourceTransactionUuid))
                    throw new IllegalArgumentException("noncanonical source transaction UUID");
            } catch (RuntimeException e) { throw new IllegalArgumentException("source transaction UUID required", e); }
            require(nativeTransactionId, "native transaction id"); require(beginPosition, "begin position");
            require(commitPosition, "commit position");
            if (journalCount < 0 || capturedSourceEventCount < 0) throw new IllegalArgumentException("negative source count");
            sourceCollectionCounts = Map.copyOf(sourceCollectionCounts);
            if (sourceCollectionCounts.values().stream().anyMatch(n -> n == null || n < 0)
                    || sourceCollectionCounts.values().stream().mapToLong(Integer::longValue).sum() != capturedSourceEventCount)
                throw new IllegalArgumentException("source collection counts disagree");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record Manifest(int protocolVersion, int schemaVersion, RecordType recordType,
                           String transactionId, String sourceDatabaseId, String sourceIncarnation,
                           String captureScopeId, String projectionGeneration, String baselineId,
                           SourceBoundary boundary, int sourceTransitionCount,
                           int destinationDescriptorCount, List<OwnerTransition> sourceTransitions,
                           String mutationSetHash) {
        public Manifest {
            requireVersion(protocolVersion, schemaVersion, recordType, RecordType.MANIFEST);
            require(transactionId, "transaction id"); require(sourceDatabaseId, "source database id");
            require(sourceIncarnation, "source incarnation"); require(captureScopeId, "capture scope id");
            require(projectionGeneration, "projection generation"); require(baselineId, "baseline id");
            require(mutationSetHash, "mutation set hash");
            if (boundary == null || sourceTransitionCount < 0 || destinationDescriptorCount < 0)
                throw new IllegalArgumentException("invalid manifest counts or boundary");
            sourceTransitions = List.copyOf(sourceTransitions);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record SnapshotBoundary(String positionKind, String position, boolean complete,
                                   long rowCount, long tombstoneCount) {
        public SnapshotBoundary {
            require(positionKind, "snapshot position kind"); require(position, "snapshot position");
            if (rowCount < 0 || tombstoneCount < 0) throw new IllegalArgumentException("negative baseline count");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record BaselineOwner(int protocolVersion, int schemaVersion, RecordType recordType,
                                String baselineId, String sourceDatabaseId, String sourceIncarnation,
                                String captureScopeId, String projectionGeneration,
                                OwnerIdentity owner, OwnerState state, JsonNode payload,
                                String snapshotPosition) {
        public BaselineOwner {
            requireVersion(protocolVersion, schemaVersion, recordType, RecordType.BASELINE_OWNER);
            require(baselineId, "baseline id"); require(sourceDatabaseId, "source database id");
            require(sourceIncarnation, "source incarnation"); require(captureScopeId, "capture scope id");
            require(projectionGeneration, "projection generation"); require(snapshotPosition, "snapshot position");
            if (owner == null || state == null || state.presence() == Presence.ABSENT
                    || !owner.sourceDatabaseId().equals(sourceDatabaseId) || !owner.sourceIncarnation().equals(sourceIncarnation))
                throw new IllegalArgumentException("baseline owner identity/state mismatch");
            if (state.presence() == Presence.DELETED && payload != null && !payload.isNull()
                    || state.presence() == Presence.PRESENT && (payload == null || payload.isNull()))
                throw new IllegalArgumentException("baseline owner payload/presence mismatch");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record Baseline(int protocolVersion, int schemaVersion, RecordType recordType,
                           String baselineId, BaselineStatus status, String sourceDatabaseId,
                           String sourceIncarnation, String captureScopeId,
                           String projectionGeneration, SnapshotBoundary boundary,
                           long ownerRecordCount) {
        public Baseline {
            requireVersion(protocolVersion, schemaVersion, recordType, RecordType.BASELINE);
            require(baselineId, "baseline id"); require(sourceDatabaseId, "source database id");
            require(sourceIncarnation, "source incarnation"); require(captureScopeId, "capture scope id");
            require(projectionGeneration, "projection generation");
            if (status == null || boundary == null) throw new IllegalArgumentException("baseline status/boundary required");
            long total;
            try { total = Math.addExact(boundary.rowCount(), boundary.tombstoneCount()); }
            catch (ArithmeticException e) { throw new IllegalArgumentException("baseline owner count overflow", e); }
            if (ownerRecordCount < 0 || ownerRecordCount != total)
                throw new IllegalArgumentException("baseline owner count mismatch");
            if (status == BaselineStatus.ACTIVE && !boundary.complete())
                throw new IllegalArgumentException("active baseline requires a complete boundary");
        }
    }
}
