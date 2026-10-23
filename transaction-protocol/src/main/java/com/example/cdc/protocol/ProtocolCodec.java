package com.example.cdc.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class ProtocolCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .findAndRegisterModules()
            .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    private ProtocolCodec() {}

    public static ObjectMapper mapper() { return MAPPER; }

    public static String json(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot encode protocol record", e);
        }
    }

    public static <T> T read(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("invalid " + type.getSimpleName() + " record", e);
        }
    }

    public static String sha256(Object value) {
        return sha256Bytes(json(value).getBytes(StandardCharsets.UTF_8));
    }

    public static String sha256Bytes(byte[] bytes) {
        try {
            return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String canonicalObjectKey(Protocol.ObjectIdentity identity) {
        return identity.entity() + ":" + json(new TreeMap<>(identity.primaryKey()));
    }

    public static String canonicalTransactionId(String databaseId, String incarnation, String scope, String nativeId) {
        Protocol.require(databaseId, "database id");
        Protocol.require(incarnation, "incarnation");
        Protocol.require(scope, "scope");
        Protocol.require(nativeId, "native transaction id");
        return databaseId + "/" + incarnation + "/" + scope + "/" + nativeId;
    }

    /** Debezium PostgreSQL 2.7 may suffix the native xid with a changing LSN. */
    public static String normalizePostgresTransactionId(String debeziumId) {
        Protocol.require(debeziumId, "Debezium transaction id");
        int separator = debeziumId.indexOf(':');
        return separator < 0 ? debeziumId : debeziumId.substring(0, separator);
    }

    public static String mutationSetHash(List<Protocol.Mutation> mutations) {
        return sha256(mutations.stream()
                .sorted(java.util.Comparator.comparingLong(Protocol.Mutation::mutationSequence))
                .map(m -> Map.of("sequence", m.mutationSequence(), "id", m.mutationId(), "payload", m.payloadHash()))
                .toList());
    }

    public static String payloadHash(JsonNode before, JsonNode after) {
        return sha256(Map.of(
                "before", before == null ? MAPPER.nullNode() : before,
                "after", after == null ? MAPPER.nullNode() : after));
    }
}
