package com.example.cdc.projector;

import com.example.cdc.protocol.Protocol;
import com.example.cdc.protocol.ProtocolCodec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InventoryProjectionPlannerTest {
    @Test
    void fansOutOneAccountChangeAndAdvancesDeterministicVersions() throws Exception {
        ProjectorState state = new ProjectorState();
        ProjectorState.Assembly tx = assembly("tx-1", 10);
        ProjectorState.CapturedChange change = new ProjectorState.CapturedChange();
        change.collection = "inventory.accounts";
        change.sequence = 1;
        change.sourceEventId = "event-1";
        change.sourcePosition = "0/1";
        change.operation = "c";
        change.after = ProtocolCodec.mapper().readTree("""
                {"account_id":1,"customer_id":42,"street":"Main","city":"X","state":"NJ",
                 "zip_code":"00000","home_phone":"111","work_phone":null,"mobile_phone":"333"}
                """);
        tx.changes.put(1L, change);

        var result = new InventoryProjectionPlanner().plan(state, tx, "db", "inc", "scope", Protocol.SourceStatus.COMMITTED);

        assertEquals(3, result.mutations().size());
        assertEquals(3, result.manifest().expectedMutationCount());
        assertTrue(result.mutations().stream().allMatch(m -> m.newObjectVersion().producerTransactionId().equals("db/inc/scope/tx-1")));
        assertEquals(result.manifest().mutationSetHash(), ProtocolCodec.mutationSetHash(result.mutations()));
    }

    @Test
    void orderIdentityUsesDestinationPrimaryKeyName() throws Exception {
        var tx=assembly("key-test",12); tx.changes.put(1L,order(1,100,"P","10.00"));
        var planned=new InventoryProjectionPlanner().plan(new ProjectorState(),tx,"db","inc","scope",Protocol.SourceStatus.COMMITTED);
        var order=planned.mutations().stream().filter(m->m.object().entity().equals("operational.orders")).findFirst().orElseThrow();
        assertEquals("100",order.object().primaryKey().get("id").value());
        assertTrue(planned.mutations().stream().filter(m->m.object().entity().equals("operational.order_items"))
                .allMatch(m->m.dependencies().isEmpty()),"same-transaction parents do not become external dependencies");
    }

    @Test
    void conflictingContributionsQuarantineInsteadOfChoosingAWinner() throws Exception {
        ProjectorState state = new ProjectorState();
        ProjectorState.Assembly tx = assembly("tx-2", 11);
        tx.changes.put(1L, order(1, 100, "P", "10.00"));
        tx.changes.put(2L, order(2, 101, "P", "20.00"));
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> new InventoryProjectionPlanner().plan(state, tx, "db", "inc", "scope", Protocol.SourceStatus.COMMITTED));
        assertTrue(error.getMessage().contains("conflicting projected contributions"));
        assertTrue(state.objectVersions.isEmpty(), "failed planning must not advance versions");
    }

    private static ProjectorState.Assembly assembly(String nativeId, long offset) {
        ProjectorState.Assembly tx = new ProjectorState.Assembly();
        tx.nativeTransactionId = nativeId;
        tx.canonicalTransactionId = "db/inc/scope/" + nativeId;
        tx.firstSeenEpochMs = 1;
        tx.endOffset = offset;
        tx.commitPosition = "0/" + offset;
        return tx;
    }

    private static ProjectorState.CapturedChange order(long seq, long orderId, String product, String price) throws Exception {
        ProjectorState.CapturedChange change = new ProjectorState.CapturedChange();
        change.collection = "inventory.orders_flat";
        change.sequence = seq;
        change.sourceEventId = "event-" + seq;
        change.sourcePosition = "0/" + seq;
        change.operation = "c";
        change.after = ProtocolCodec.mapper().readTree("{\"order_id\":" + orderId
                + ",\"customer_id\":1,\"product_name\":\"" + product
                + "\",\"product_category\":\"C\",\"unit_price\":\"" + price
                + "\",\"quantity\":1}");
        return change;
    }
}
