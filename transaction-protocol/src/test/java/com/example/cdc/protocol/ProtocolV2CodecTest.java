package com.example.cdc.protocol;

import com.example.cdc.protocol.ProtocolV2.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.example.cdc.protocol.ProtocolV2.*;
import static org.junit.jupiter.api.Assertions.*;

class ProtocolV2CodecTest {
    private static final String UUID = "123e4567-e89b-12d3-a456-426614174000";
    private static final String DB = "source";
    private static final String INC = "inc-1";
    private static final String SCOPE = "scope";
    private static final OwnerState ABSENT = new OwnerState(Presence.ABSENT, null);
    private static final OwnerState PRESENT = new OwnerState(Presence.PRESENT, 1L);

    private static ObjectNode customer(long version) {
        ObjectNode row = ProtocolV2Codec.mapper().createObjectNode();
        return row.put("id", 42).put("first_name", "Ada").put("last_name", "Lovelace").put("version", version);
    }

    private static OwnerIdentity owner(String collection, Map<String, TypedValue> key) {
        return new OwnerIdentity(DB, INC, collection, key);
    }

    private record Fixture(OwnerTransition transition, Mutation mutation, Manifest manifest) {}

    private static Fixture fixture() {
        OwnerIdentity owner = owner("inventory.customers", Map.of("id", new TypedValue("int64", "42")));
        ObjectIdentity object = new ObjectIdentity("client.customers", owner.primaryKey());
        String tx = ProtocolV2Codec.transactionId(DB, INC, UUID);
        Predecessor predecessor = new Predecessor(ABSENT, new ProducerReference(ProducerType.BASELINE, EMPTY_BASELINE_ID));
        OwnerTransition transition = new OwnerTransition(tx, owner, predecessor, PRESENT,
                List.of(new JournalRevision(1, ABSENT, PRESENT)), null, customer(1));
        Mutation mutation = ProtocolV2Codec.signMutation(new Mutation(VERSION, SCHEMA_VERSION, RecordType.MUTATION,
                tx, DB, INC, SCOPE, GENERATION, EMPTY_BASELINE_ID, 1, "pending", "pending",
                owner, object, Operation.INSERT, predecessor, PRESENT, List.of(), null, customer(1)));
        SourceBoundary boundary = new SourceBoundary(UUID, "7", "0/1", "0/2", 1, 3,
                Map.of("inventory.customers", 1, "source_cdc.transaction_context", 1,
                        "source_cdc.revision_journal", 1));
        Manifest manifest = ProtocolV2Codec.signManifest(new Manifest(VERSION, SCHEMA_VERSION,
                RecordType.MANIFEST, tx, DB, INC, SCOPE, GENERATION, EMPTY_BASELINE_ID, boundary,
                1, 1, List.of(transition), "pending"), List.of(mutation));
        return new Fixture(transition, mutation, manifest);
    }

    @Test void roundTripsAndVerifiesTheOnlyWireContract() {
        Fixture fixture = fixture();
        Mutation mutation = ProtocolV2Codec.readMutation(MUTATION_TOPIC, ProtocolV2Codec.json(fixture.mutation));
        Manifest manifest = ProtocolV2Codec.readManifest(CONTROL_TOPIC, ProtocolV2Codec.json(fixture.manifest));
        ProtocolV2Codec.verifyManifest(manifest, List.of(mutation));
        assertEquals(ProtocolV2Codec.json(fixture.mutation), ProtocolV2Codec.json(mutation));
        assertEquals(ProtocolV2Codec.json(fixture.manifest), ProtocolV2Codec.json(manifest));
    }

    @Test void oneManifestHashBindsTheCompleteMutationDescriptor() {
        Fixture fixture = fixture();
        Mutation base = fixture.mutation;
        OwnerState deleted = new OwnerState(Presence.DELETED, 2L);
        Dependency dependency = new Dependency(DependencyType.EXISTS, base.owner(), PRESENT,
                new ProducerReference(ProducerType.TRANSACTION, "prior"), "ordered dependency");
        Mutation altered = ProtocolV2Codec.signMutation(new Mutation(VERSION, SCHEMA_VERSION, RecordType.MUTATION,
                base.transactionId(), DB, INC, SCOPE, GENERATION, EMPTY_BASELINE_ID, 1, "pending", "pending",
                base.owner(), base.object(), Operation.INSERT, base.expected(), base.result(),
                List.of(dependency), base.before(), base.after()));
        assertNotEquals(ProtocolV2Codec.mutationSetHash(List.of(base)),
                ProtocolV2Codec.mutationSetHash(List.of(altered)));
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolV2Codec.verifyManifest(fixture.manifest, List.of(altered)));
        assertEquals(Presence.DELETED, deleted.presence());
    }

    @Test void removedTablesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> owner("inventory.addresses", Map.of(
                "customer_id", new TypedValue("int64", "42"),
                "address_type", new TypedValue("text", "HOME"))));
    }

    @Test void onlyTheFixedEmptyBootstrapCanProveAbsence() {
        assertDoesNotThrow(() -> new Predecessor(ABSENT,
                new ProducerReference(ProducerType.BASELINE, EMPTY_BASELINE_ID)));
        assertThrows(IllegalArgumentException.class, () -> new Predecessor(ABSENT,
                new ProducerReference(ProducerType.BASELINE, "dynamic-baseline")));
        assertThrows(IllegalArgumentException.class, () -> new Predecessor(PRESENT,
                new ProducerReference(ProducerType.BASELINE, EMPTY_BASELINE_ID)));
    }
}
