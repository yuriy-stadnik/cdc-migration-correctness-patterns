package com.example.cdc.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import static com.example.cdc.protocol.ProtocolV2.*;

/** Canonical encoding. The manifest has one integrity hash over complete mutation descriptors. */
public final class ProtocolV2Codec {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .findAndRegisterModules()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .enable(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);

    private ProtocolV2Codec() {}
    public static ObjectMapper mapper() { return MAPPER; }

    public static String json(Object value) {
        try { return MAPPER.writeValueAsString(canonical(MAPPER.valueToTree(value))); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("cannot encode protocol record", e); }
    }

    private static String sha256(Object value) {
        try {
            return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(json(value).getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    private static JsonNode canonical(JsonNode node) {
        if (node == null) return MAPPER.nullNode();
        if (node.isObject()) {
            ObjectNode sorted = MAPPER.createObjectNode();
            TreeMap<String, JsonNode> entries = new TreeMap<>();
            node.fields().forEachRemaining(e -> entries.put(e.getKey(), e.getValue()));
            entries.forEach((key, value) -> sorted.set(key, canonical(value)));
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode result = MAPPER.createArrayNode();
            node.forEach(value -> result.add(canonical(value)));
            return result;
        }
        if (node.isFloatingPointNumber()) return DecimalNode.valueOf(node.decimalValue().stripTrailingZeros());
        return node;
    }

    private static <T> T parse(String json, Class<T> type) {
        try { return MAPPER.readValue(json, type); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("invalid " + type.getSimpleName(), e); }
    }

    public static Mutation readMutation(String topic, String json) {
        if (!MUTATION_TOPIC.equals(topic)) throw new IllegalArgumentException("wrong mutation topic");
        Mutation mutation = parse(json, Mutation.class); verifyMutation(mutation); return mutation;
    }
    public static Manifest readManifest(String topic, String json) {
        if (!CONTROL_TOPIC.equals(topic)) throw new IllegalArgumentException("wrong control topic");
        Manifest manifest = parse(json, Manifest.class); verifyManifestEnvelope(manifest); return manifest;
    }
    public static String transactionId(String databaseId, String incarnation, String sourceUuid) {
        ProtocolV2.require(databaseId, "database id"); ProtocolV2.require(incarnation, "source incarnation");
        try {
            if (!UUID.fromString(sourceUuid).toString().equals(sourceUuid))
                throw new IllegalArgumentException("noncanonical source transaction UUID");
        } catch (RuntimeException e) { throw new IllegalArgumentException("source transaction UUID required", e); }
        return "source-v2:" + json(List.of(databaseId, incarnation, sourceUuid));
    }

    private static JsonNode mutationDescriptor(Mutation mutation) {
        ObjectNode descriptor = MAPPER.valueToTree(mutation);
        descriptor.remove(List.of("mutationId", "idempotencyKey"));
        ArrayNode dependencies = MAPPER.createArrayNode();
        mutation.dependencies().stream().map(ProtocolV2Codec::json).sorted().forEach(value -> {
            try { dependencies.add(MAPPER.readTree(value)); }
            catch (JsonProcessingException e) { throw new IllegalArgumentException(e); }
        });
        descriptor.set("dependencies", dependencies);
        return canonical(descriptor);
    }

    public static String mutationId(Mutation mutation) {
        return mutation.transactionId() + ":" + mutation.mutationSequence();
    }
    public static String idempotencyKey(Mutation mutation) {
        return mutation.projectionGeneration() + ":" + mutationId(mutation);
    }

    public static Mutation signMutation(Mutation draft) {
        Mutation base = new Mutation(VERSION, SCHEMA_VERSION, RecordType.MUTATION,
                draft.transactionId(), draft.sourceDatabaseId(), draft.sourceIncarnation(),
                draft.captureScopeId(), draft.projectionGeneration(), draft.baselineId(),
                draft.mutationSequence(), "PENDING", "PENDING", draft.owner(), draft.object(),
                draft.operation(), draft.expected(), draft.result(), draft.dependencies(), draft.before(), draft.after());
        Mutation signed = new Mutation(VERSION, SCHEMA_VERSION, RecordType.MUTATION,
                base.transactionId(), base.sourceDatabaseId(), base.sourceIncarnation(),
                base.captureScopeId(), base.projectionGeneration(), base.baselineId(), base.mutationSequence(),
                mutationId(base), idempotencyKey(base), base.owner(), base.object(), base.operation(),
                base.expected(), base.result(), base.dependencies(), base.before(), base.after());
        verifyMutation(signed); return signed;
    }

    public static void verifyMutation(Mutation mutation) {
        if (!mutationId(mutation).equals(mutation.mutationId())
                || !idempotencyKey(mutation).equals(mutation.idempotencyKey()))
            throw new IllegalArgumentException("mutation identity mismatch");
        verifyPayload(mutation.before(), mutation.object(), mutation.expected().state());
        verifyPayload(mutation.after(), mutation.object(), mutation.result());
        if (mutation.expected().state().presence() == Presence.PRESENT
                && (mutation.before() == null || mutation.before().isNull()))
            throw new IllegalArgumentException("present predecessor needs before payload");
    }

    private static void verifyPayload(JsonNode payload, ObjectIdentity object, OwnerState state) {
        if (payload == null || payload.isNull()) return;
        if (!payload.isObject()) throw new IllegalArgumentException("row payload must be an object");
        JsonNode version = payload.get("version");
        if (version != null && (!version.isIntegralNumber() || !version.canConvertToLong()
                || state.version() == null || version.longValue() != state.version()))
            throw new IllegalArgumentException("payload version disagrees with owner state");
        object.primaryKey().forEach((key, typed) -> {
            JsonNode value = payload.get(key);
            if (value == null || value.isNull()) throw new IllegalArgumentException("payload primary key missing " + key);
            if (typed.type().equals("int64")) {
                if (!value.isIntegralNumber() || !value.canConvertToLong()
                        || value.longValue() != Long.parseLong(typed.value()))
                    throw new IllegalArgumentException("payload primary key mismatch " + key);
            } else if (!value.isTextual() || !value.textValue().equals(typed.value()))
                throw new IllegalArgumentException("payload primary key mismatch " + key);
        });
    }

    public static String mutationSetHash(List<Mutation> mutations) {
        return sha256(mutations.stream().sorted(Comparator.comparingLong(Mutation::mutationSequence))
                .map(ProtocolV2Codec::mutationDescriptor).toList());
    }

    public static Manifest signManifest(Manifest draft, List<Mutation> mutations) {
        List<OwnerTransition> ordered = draft.sourceTransitions().stream()
                .sorted(Comparator.comparing(t -> json(t.owner()))).toList();
        Manifest signed = new Manifest(VERSION, SCHEMA_VERSION, RecordType.MANIFEST,
                draft.transactionId(), draft.sourceDatabaseId(), draft.sourceIncarnation(),
                draft.captureScopeId(), draft.projectionGeneration(), draft.baselineId(), draft.boundary(),
                draft.sourceTransitionCount(), draft.destinationDescriptorCount(), ordered, mutationSetHash(mutations));
        verifyManifest(signed, mutations); return signed;
    }

    private static void verifyManifestEnvelope(Manifest manifest) {
        if (!manifest.transactionId().equals(transactionId(manifest.sourceDatabaseId(),
                manifest.sourceIncarnation(), manifest.boundary().sourceTransactionUuid()))
                || manifest.sourceTransitionCount() != manifest.sourceTransitions().size())
            throw new IllegalArgumentException("manifest identity/count mismatch");
    }

    public static void verifyManifest(Manifest manifest, List<Mutation> mutations) {
        verifyManifestEnvelope(manifest);
        if (manifest.destinationDescriptorCount() != mutations.size()
                || !mutationSetHash(mutations).equals(manifest.mutationSetHash()))
            throw new IllegalArgumentException("manifest mutation set mismatch");
        Map<String, OwnerTransition> transitions = new TreeMap<>(); Set<Long> ordinals = new HashSet<>();
        for (OwnerTransition transition : manifest.sourceTransitions()) {
            if (!transition.transactionId().equals(manifest.transactionId())
                    || !transition.owner().sourceDatabaseId().equals(manifest.sourceDatabaseId())
                    || !transition.owner().sourceIncarnation().equals(manifest.sourceIncarnation())
                    || transitions.put(json(transition.owner()), transition) != null)
                throw new IllegalArgumentException("foreign or duplicate source transition");
            transition.revisions().forEach(r -> { if (!ordinals.add(r.ordinal()))
                throw new IllegalArgumentException("duplicate source journal ordinal"); });
        }
        if (ordinals.size() != manifest.boundary().journalCount())
            throw new IllegalArgumentException("source journal count mismatch");
        for (long i = 1; i <= manifest.boundary().journalCount(); i++)
            if (!ordinals.contains(i)) throw new IllegalArgumentException("source journal ordinal gap");
        Set<Long> sequences = new HashSet<>(); Set<String> objects = new HashSet<>(); Set<String> emitted = new HashSet<>();
        for (Mutation mutation : mutations) {
            verifyMutation(mutation);
            if (!mutation.transactionId().equals(manifest.transactionId())
                    || !mutation.sourceDatabaseId().equals(manifest.sourceDatabaseId())
                    || !mutation.sourceIncarnation().equals(manifest.sourceIncarnation())
                    || !mutation.captureScopeId().equals(manifest.captureScopeId())
                    || !mutation.projectionGeneration().equals(manifest.projectionGeneration())
                    || !mutation.baselineId().equals(manifest.baselineId()))
                throw new IllegalArgumentException("mixed transaction/source/generation");
            OwnerTransition transition = transitions.get(json(mutation.owner()));
            if (transition == null || !transition.expected().equals(mutation.expected())
                    || !transition.result().equals(mutation.result())
                    || !json(transition.before()).equals(json(mutation.before()))
                    || !json(transition.after()).equals(json(mutation.after())))
                throw new IllegalArgumentException("mutation differs from source transition");
            emitted.add(json(mutation.owner()));
            if (!sequences.add(mutation.mutationSequence()) || !objects.add(json(mutation.object())))
                throw new IllegalArgumentException("duplicate mutation sequence or destination key");
        }
        for (long i = 1; i <= mutations.size(); i++) if (!sequences.contains(i))
            throw new IllegalArgumentException("mutation sequence gap");
        if (!transitions.keySet().equals(emitted))
            throw new IllegalArgumentException("each source row must emit exactly one destination descriptor");
    }
}
