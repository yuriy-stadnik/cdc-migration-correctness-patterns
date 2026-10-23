package com.example.cdc.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

/** Versioned wire contract shared by the source projector and destination apply engine. */
public final class Protocol {
    public static final int VERSION = 1;
    public static final String CONTROL_TOPIC = "cdc.tx.control.v1";
    public static final String MUTATION_TOPIC = "cdc.tx.mutations.v1";
    public static final String DLQ_TOPIC = "cdc.tx.dlq.v1";
    public static final String GENERATION = "inventory-projection-v1";

    private Protocol() {}

    public enum ControlType { MANIFEST, INCOMPLETE, FAILED, DLQ }
    public enum SourceStatus { COMMITTED, SNAPSHOT, INCOMPLETE, FAILED }
    public enum Operation { INSERT, UPDATE, DELETE }
    public enum Presence { ABSENT, PRESENT, DELETED }
    public enum DependencyType { WRITE_PREDECESSOR, AFTER_TRANSACTION, EXISTS, NOT_EXISTS }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record SourcePosition(String kind, String value) {
        public SourcePosition { require(kind, "source position kind"); require(value, "source position value"); }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record TypedValue(String type, String value) {
        public TypedValue {
            require(type, "primary-key type");
            require(value, "primary-key value");
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record ObjectIdentity(String entity, Map<String, TypedValue> primaryKey) {
        public ObjectIdentity {
            require(entity, "entity");
            primaryKey = Map.copyOf(primaryKey);
            if (primaryKey.isEmpty()) throw new IllegalArgumentException("primary key is empty");
        }
    }

    /**
     * A revision is the identity of the source transaction mutation that produced an object state.
     * A baseline token represents a proven ABSENT/DELETED/PRESENT state and is not a Kafka position.
     */
    @JsonIgnoreProperties(ignoreUnknown = false)
    public record VersionToken(
            String generation,
            String producerTransactionId,
            long mutationSequence,
            Presence presence
    ) {
        public VersionToken {
            require(generation, "version generation");
            require(producerTransactionId, "version producer transaction");
            if (mutationSequence < 0) throw new IllegalArgumentException("negative mutation sequence");
            if (presence == null) throw new IllegalArgumentException("version presence is required");
        }

        public static VersionToken absentBaseline(String generation) {
            return new VersionToken(generation, "BASELINE", 0, Presence.ABSENT);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record Dependency(
            DependencyType type,
            String producerTransactionId,
            ObjectIdentity object,
            VersionToken requiredVersion,
            String reason
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record Mutation(
            int protocolVersion,
            String transactionId,
            String sourceDatabaseId,
            String sourceIncarnation,
            String captureScopeId,
            String projectionGeneration,
            int schemaVersion,
            long mutationSequence,
            String mutationId,
            ObjectIdentity object,
            Operation operation,
            List<String> sourceEventIds,
            List<Long> sourceRecordSequences,
            List<SourcePosition> sourceEventPositions,
            VersionToken beforeObjectVersion,
            VersionToken expectedDestinationVersion,
            VersionToken newObjectVersion,
            List<Dependency> dependencies,
            String idempotencyKey,
            String payloadHash,
            JsonNode before,
            JsonNode after
    ) {
        public Mutation {
            if (protocolVersion != VERSION) throw new IllegalArgumentException("unsupported protocol version");
            require(transactionId, "transaction id");
            require(mutationId, "mutation id");
            require(idempotencyKey, "idempotency key");
            require(payloadHash, "payload hash");
            if (mutationSequence < 1) throw new IllegalArgumentException("mutation sequence must be positive");
            if (object == null || operation == null || expectedDestinationVersion == null || newObjectVersion == null) {
                throw new IllegalArgumentException("mutation identity, operation, and versions are required");
            }
            sourceEventIds = List.copyOf(sourceEventIds);
            sourceRecordSequences = List.copyOf(sourceRecordSequences);
            sourceEventPositions = List.copyOf(sourceEventPositions);
            dependencies = dependencies == null ? List.of() : List.copyOf(dependencies);
            if (operation == Operation.DELETE && after != null && !after.isNull()) {
                throw new IllegalArgumentException("DELETE cannot carry an after payload");
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public record ControlMessage(
            int protocolVersion,
            ControlType messageType,
            String transactionId,
            String sourceDatabaseId,
            String sourceIncarnation,
            String captureScopeId,
            String nativeTransactionId,
            SourceStatus sourceStatus,
            String sourceCommitPositionKind,
            String sourceCommitPosition,
            long sourceCommitSequence,
            String projectionGeneration,
            int projectionVersion,
            int schemaVersion,
            String correlationId,
            int capturedSourceEventCount,
            Map<String, Integer> sourceCollectionCounts,
            int expectedMutationCount,
            String mutationSetHash,
            String manifestHash,
            long firstSeenEpochMs,
            long deadlineEpochMs,
            String reason
    ) {
        public ControlMessage {
            if (protocolVersion != VERSION) throw new IllegalArgumentException("unsupported protocol version");
            require(transactionId, "transaction id");
            require(sourceDatabaseId, "source database id");
            require(sourceIncarnation, "source incarnation");
            require(captureScopeId, "capture scope id");
            require(projectionGeneration, "projection generation");
            if (capturedSourceEventCount < 0 || expectedMutationCount < 0) {
                throw new IllegalArgumentException("negative event count");
            }
            sourceCollectionCounts = sourceCollectionCounts == null ? Map.of() : Map.copyOf(sourceCollectionCounts);
            if (messageType == ControlType.MANIFEST) {
                require(mutationSetHash, "mutation set hash");
                require(manifestHash, "manifest hash");
            }
        }
    }

    static void require(String value, String label) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(label + " is required");
    }
}
