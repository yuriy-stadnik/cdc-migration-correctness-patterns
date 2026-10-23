package com.example.cdc.protocol;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolCodecTest {
    @Test
    void canonicalObjectIdentityDoesNotDependOnMapInsertionOrder() {
        var first = new LinkedHashMap<String, Protocol.TypedValue>();
        first.put("b", new Protocol.TypedValue("text", "2"));
        first.put("a", new Protocol.TypedValue("int64", "1"));
        var second = new LinkedHashMap<String, Protocol.TypedValue>();
        second.put("a", new Protocol.TypedValue("int64", "1"));
        second.put("b", new Protocol.TypedValue("text", "2"));

        assertEquals(
                ProtocolCodec.canonicalObjectKey(new Protocol.ObjectIdentity("x", first)),
                ProtocolCodec.canonicalObjectKey(new Protocol.ObjectIdentity("x", second)));
    }

    @Test
    void postgresAdapterRemovesOnlyTheLsnSuffix() {
        assertEquals("9001", ProtocolCodec.normalizePostgresTransactionId("9001:0A/BEEF"));
        assertEquals("9001", ProtocolCodec.normalizePostgresTransactionId("9001"));
    }

    @Test
    void versionsAreNotOffsetsAndRoundTripLosslessly() {
        var version = new Protocol.VersionToken("g1", "source/inc/scope/tx1", 3, Protocol.Presence.PRESENT);
        assertEquals(version, ProtocolCodec.read(ProtocolCodec.json(version), Protocol.VersionToken.class));
        assertThrows(IllegalArgumentException.class,
                () -> new Protocol.VersionToken("g1", "tx", -1, Protocol.Presence.PRESENT));
    }

    @Test
    void typedKeysAvoidDelimiterCollisions() {
        var a = new Protocol.ObjectIdentity("entity", Map.of("id", new Protocol.TypedValue("text", "a|b")));
        var b = new Protocol.ObjectIdentity("entity", Map.of("id", new Protocol.TypedValue("text", "a"),
                "suffix", new Protocol.TypedValue("text", "b")));
        assertNotEquals(ProtocolCodec.canonicalObjectKey(a), ProtocolCodec.canonicalObjectKey(b));
    }
}
