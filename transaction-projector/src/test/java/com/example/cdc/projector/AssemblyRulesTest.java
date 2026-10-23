package com.example.cdc.projector;

import com.example.cdc.protocol.ProtocolCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AssemblyRulesTest {
    @Test
    void requiresBothBoundariesExactCountAndContiguousSequences() {
        var tx = assembly(3);
        tx.beginSeen = true; tx.endSeen = true;
        AssemblyRules.add(tx, change(1), "h1");
        AssemblyRules.add(tx, change(3), "h3");
        assertFalse(AssemblyRules.complete(tx));
        assertEquals(List.of(2L), AssemblyRules.missingSequences(tx));
        AssemblyRules.add(tx, change(2), "h2");
        assertTrue(AssemblyRules.complete(tx));
    }

    @Test
    void identicalDuplicateDoesNotInflateCountButConflictFails() {
        var tx = assembly(1);
        assertEquals(AssemblyRules.FragmentResult.INSERTED, AssemblyRules.add(tx, change(1), "same"));
        assertEquals(AssemblyRules.FragmentResult.IDENTICAL_DUPLICATE, AssemblyRules.add(tx, change(1), "same"));
        assertEquals(1, tx.changes.size());
        assertThrows(IllegalArgumentException.class, () -> AssemblyRules.add(tx, change(1), "different"));
    }

    @Test
    void checkpointJsonRestoresAssemblyAndProjectedVersions() {
        var state = new ProjectorState();
        var tx = assembly(1); tx.beginSeen = true; tx.endSeen = true;
        AssemblyRules.add(tx, change(1), "hash"); state.assemblies.put("native", tx);
        var restored = ProtocolCodec.read(ProtocolCodec.json(state), ProjectorState.class);
        assertTrue(AssemblyRules.complete(restored.assemblies.get("native")));
        assertEquals("hash", restored.assemblies.get("native").changeHashes.get(1L));
    }

    private static ProjectorState.Assembly assembly(int count) { var tx=new ProjectorState.Assembly();tx.expectedEventCount=count;tx.expectedCollectionCounts=java.util.Map.of("inventory.customers",count);return tx; }
    private static ProjectorState.CapturedChange change(long sequence) { var c=new ProjectorState.CapturedChange();c.sequence=sequence;c.collection="inventory.customers";c.sourceEventId="e"+sequence;c.sourcePosition="0/"+sequence;return c; }
}
