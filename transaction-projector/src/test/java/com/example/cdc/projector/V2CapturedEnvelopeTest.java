package com.example.cdc.projector;

import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import static com.example.cdc.protocol.ProtocolV2.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Opt-in real PostgreSQL/Debezium envelope test, generated only on disposable infrastructure. */
class V2CapturedEnvelopeTest {
    @Test void realDebeziumTransactionsPlanAndReplay() throws Exception {
        String fixture = System.getenv("V2_CAPTURE_FIXTURE");
        assumeTrue(fixture != null, "run local/tests/test-projector-v2-capture.py and set V2_CAPTURE_FIXTURE");
        List<JsonNode> rows = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of(fixture))) rows.add(V2Capture.parse(line));
        V2Projector projector = new V2Projector("db", "inc", "scope");
        V2ProjectorState state = new V2ProjectorState();
        List<OutputRecord> output = new ArrayList<>();
        for (JsonNode row : rows) output.addAll(projector.accept(state,V2ProjectorTest.record(CAPTURE_TOPIC,row)));
        assertTrue(state.quarantine.isEmpty(), state.quarantine.toString());
        assertTrue(state.pending.isEmpty(), state.waiting.toString());
        assertEquals(5, V2ProjectorTest.manifests(output).size());
        assertTrue(V2ProjectorTest.mutations(output).stream().noneMatch(m -> m.after() != null && m.after().toString().contains("ROLLBACK")));
        for (var manifest : V2ProjectorTest.manifests(output)) {
            ProtocolV2Codec.verifyManifest(manifest, V2ProjectorTest.mutations(output).stream()
                    .filter(m -> m.transactionId().equals(manifest.transactionId())).toList());
        }
        // Checkpoint serialization and replay using the actual immutable capture evidence.
        state = ProtocolV2Codec.mapper().readValue(ProtocolV2Codec.json(state), V2ProjectorState.class);
        Collections.reverse(rows);
        for (JsonNode row : rows) assertTrue(projector.accept(state,V2ProjectorTest.record(CAPTURE_TOPIC,row)).isEmpty());
        assertNull(state.haltedReason);
        // Starting with a fresh checkpoint, shuffled delivery must produce the same immutable descriptors.
        V2ProjectorState shuffled = new V2ProjectorState();
        Collections.shuffle(rows,new Random(17));
        List<OutputRecord> reordered = new ArrayList<>();
        for (JsonNode row : rows) reordered.addAll(projector.accept(shuffled,V2ProjectorTest.record(CAPTURE_TOPIC,row)));
        assertTrue(shuffled.quarantine.isEmpty(),shuffled.quarantine.toString());
        assertEquals(new HashSet<>(output),new HashSet<>(reordered));
    }
}
