package com.example.cdc.projector;

import com.example.cdc.protocol.ProtocolV2.*;
import com.example.cdc.protocol.ProtocolV2Codec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.*;
import static com.example.cdc.protocol.ProtocolV2.*;

/** Pinned PostgreSQL/Debezium adapter. Source log intervals qualify native XIDs, never Kafka offsets. */
final class V2Capture {
    static final Set<String> TABLES = Set.of("inventory.customers", "inventory.products", "inventory.orders", "inventory.order_items",
            "source_cdc.transaction_context", "source_cdc.revision_journal");
    static final int MAX_EVENTS = 100_000;
    private final String database, incarnation;
    V2Capture(String database, String incarnation) { this.database = database; this.incarnation = incarnation; }

    static JsonNode parse(String value) {
        try { return ProtocolV2Codec.mapper().readTree(value); }
        catch (Exception e) { throw new IllegalArgumentException("invalid capture JSON", e); }
    }
    static String text(JsonNode node, String field) {
        JsonNode n = node.get(field);
        if (n == null || n.isNull() || !n.isValueNode() || n.asText().isBlank())
            throw new IllegalArgumentException("missing capture field " + field);
        return n.asText();
    }
    static long number(JsonNode node, String field) {
        JsonNode n = node.get(field);
        if (n == null || !n.isIntegralNumber() || !n.canConvertToLong())
            throw new IllegalArgumentException("non-integral capture field " + field);
        return n.longValue();
    }
    static BigInteger lsn(String value) {
        if (value.contains("/")) {
            String[] parts = value.split("/", -1);
            if (parts.length != 2) throw new IllegalArgumentException("invalid LSN");
            return new BigInteger(parts[0], 16).shiftLeft(32).add(new BigInteger(parts[1], 16));
        }
        BigInteger result = new BigInteger(value);
        if (result.signum() < 0) throw new IllegalArgumentException("negative LSN");
        return result;
    }
    static boolean boundary(JsonNode root) { return root.has("status") && root.has("id"); }
    static String rawId(JsonNode root) { return text(boundary(root) ? root : root.path("transaction"), "id"); }
    static String xid(JsonNode root) {
        String id = rawId(root);
        int i = id.indexOf(':');
        if (i < 1) throw new IllegalArgumentException("transaction occurrence requires xid:LSN evidence");
        String xid = id.substring(0, i);
        long value = Long.parseLong(xid);
        if (value < 0 || value > 0xffffffffL) throw new IllegalArgumentException("invalid native XID");
        return xid;
    }
    static BigInteger position(JsonNode root) {
        String id = rawId(root);
        xid(root);
        return lsn(id.substring(id.indexOf(':') + 1));
    }
    static String collection(JsonNode root) {
        return text(root.path("source"), "schema") + "." + text(root.path("source"), "table");
    }
    static String observationKey(JsonNode root) {
        return xid(root) + ":" + position(root) + ":"
                + (boundary(root) ? text(root, "status") : number(root.path("transaction"), "total_order"));
    }
    static JsonNode immutable(JsonNode root) {
        ObjectNode copy = root.deepCopy();
        copy.remove(List.of("ts_ms", "ts_us", "ts_ns")); // connector receive time is not source identity
        return copy;
    }

    V2ProjectorState.Pending assemble(JsonNode begin, JsonNode end, List<JsonNode> rows) {
        long expected = number(end, "event_count");
        if (expected < 0 || expected > MAX_EVENTS) throw new IllegalArgumentException("capture transaction size bound");
        if (rows.size() != expected) return null;
        TreeMap<Long, JsonNode> ordered = new TreeMap<>();
        Map<String, Integer> actual = new TreeMap<>(), declared = new TreeMap<>();
        for (JsonNode row : rows) {
            String collection = collection(row);
            if (!TABLES.contains(collection)) throw new IllegalArgumentException("unsupported captured table " + collection);
            long sequence = number(row.path("transaction"), "total_order");
            if (sequence < 1 || ordered.put(sequence, row) != null)
                throw new IllegalArgumentException("duplicate or invalid capture sequence");
            actual.merge(collection, 1, Integer::sum);
            if (row.path("source").hasNonNull("txId")
                    && !text(row.path("source"), "txId").equals(xid(end)))
                throw new IllegalArgumentException("source XID disagrees with envelope");
            if (!lsn(text(row.path("source"), "lsn")).equals(position(row)))
                throw new IllegalArgumentException("source LSN disagrees with envelope");
        }
        for (long i = 1; i <= expected; i++) if (!ordered.containsKey(i)) return null;
        if (!end.path("data_collections").isArray()) throw new IllegalArgumentException("missing collection counts");
        for (JsonNode c : end.path("data_collections")) {
            String name = text(c, "data_collection");
            long count = number(c, "event_count");
            if (count < 0 || count > MAX_EVENTS || declared.put(name, (int) count) != null)
                throw new IllegalArgumentException("invalid collection counts");
        }
        if (!actual.equals(declared)) throw new IllegalArgumentException("capture collection counts disagree");
        Map<String, Long> collectionOrder = new HashMap<>();
        BigInteger priorLsn = position(begin);
        for (JsonNode row : ordered.values()) {
            if (position(row).compareTo(priorLsn) < 0) throw new IllegalArgumentException("capture order contradicts source LSN");
            priorLsn = position(row);
            long n = collectionOrder.merge(collection(row), 1L, Long::sum);
            if (row.path("transaction").has("data_collection_order")
                    && number(row.path("transaction"), "data_collection_order") != n)
                throw new IllegalArgumentException("collection order gap");
        }
        JsonNode context = null;
        String uuid = null, fullXid = null;
        TreeMap<Long, JsonNode> journal = new TreeMap<>();
        for (JsonNode row : ordered.values()) {
            if (collection(row).equals("source_cdc.transaction_context")) {
                JsonNode after = row.path("after");
                if (after.isNull() || !after.isObject()) throw new IllegalArgumentException("source context deleted");
                String id = text(after, "source_transaction_id");
                String full = text(after, "full_xid");
                if (uuid != null && (!uuid.equals(id) || !fullXid.equals(full)))
                    throw new IllegalArgumentException("ambiguous reused-XID occurrence");
                uuid = id; fullXid = full; context = after;
            }
            if (collection(row).equals("source_cdc.revision_journal")) {
                if (!text(row, "op").equals("c")) throw new IllegalArgumentException("journal must be append-only");
                JsonNode entry = row.path("after");
                if (journal.put(number(entry, "ordinal"), entry) != null)
                    throw new IllegalArgumentException("duplicate journal ordinal");
            }
        }
        if (context == null) throw new IllegalArgumentException("committed source context missing");
        if (!new BigInteger(fullXid).and(BigInteger.valueOf(0xffffffffL)).toString().equals(xid(end)))
            throw new IllegalArgumentException("source full XID does not match occurrence");
        if (number(context, "journal_count") != journal.size())
            throw new IllegalArgumentException("source journal count mismatch");
        for (long i = 1; i <= journal.size(); i++) if (!journal.containsKey(i))
            throw new IllegalArgumentException("source journal gap");
        String tx = ProtocolV2Codec.transactionId(database, incarnation, uuid);
        V2ProjectorState.Pending pending = new V2ProjectorState.Pending();
        pending.transactionId = tx;
        pending.captureEvidence = ProtocolV2Codec.json(List.of(immutable(begin), immutable(end), ordered.values()));
        pending.boundary = new SourceBoundary(uuid, xid(end), position(begin).toString(), position(end).toString(),
                journal.size(), rows.size(), actual);
        Map<String, V2ProjectorState.Net> nets = new TreeMap<>();
        Set<String> beforeEvidence = new HashSet<>(), afterEvidence = new HashSet<>();
        for (JsonNode entry : journal.values()) {
            if (!uuid.equals(text(entry, "source_transaction_id"))) throw new IllegalArgumentException("journal UUID mismatch");
            OwnerIdentity owner = owner(text(entry, "collection"), jsonValue(entry.get("owner_key")));
            OwnerState before = state(entry, "before"), after = state(entry, "after");
            JsonNode beforeImage = image(owner, jsonValue(entry.get("before_image")), before);
            JsonNode afterImage = image(owner, jsonValue(entry.get("after_image")), after);
            var revision = new JournalRevision(number(entry, "ordinal"), before, after);
            String key = ProtocolV2Codec.json(owner);
            V2ProjectorState.Net net = nets.get(key);
            if (net == null) {
                net = new V2ProjectorState.Net(); net.owner = owner; net.beforeState = before; net.before = beforeImage;
                nets.put(key, net);
            } else if (!net.result.equals(before) || !same(net.after, beforeImage)) {
                throw new IllegalArgumentException("discontinuous source owner chain");
            }
            net.result = after; net.after = afterImage; net.revisions.add(revision);
            if (before.presence() != Presence.ABSENT) beforeEvidence.add(evidence(owner, before, beforeImage));
            afterEvidence.add(evidence(owner, after, afterImage));
        }
        Set<String> seenBefore = new HashSet<>(), seenAfter = new HashSet<>();
        for (JsonNode row : ordered.values()) {
            String c = collection(row);
            if (c.equals("source_cdc.revision_journal") || c.equals("source_cdc.transaction_context")) continue;
            if (!List.of("c", "u", "d").contains(text(row, "op"))) throw new IllegalArgumentException("unsupported row operation");
            for (String side : List.of("before", "after")) {
                JsonNode raw = row.get(side);
                if (raw == null || raw.isNull()) continue;
                OwnerIdentity owner = ownerFromRow(c, raw);
                OwnerState s = new OwnerState(raw.path("is_deleted").asBoolean(false) ? Presence.DELETED : Presence.PRESENT,
                        number(raw, "version"));
                JsonNode img = image(owner, raw, s);
                String ev = evidence(owner, s, img);
                Set<String> wanted = side.equals("before") ? beforeEvidence : afterEvidence;
                if (!wanted.contains(ev)) throw new IllegalArgumentException("row has no matching source journal " + c + " " + side);
                (side.equals("before") ? seenBefore : seenAfter).add(ev);
            }
        }
        for (V2ProjectorState.Net net : nets.values()) for (JournalRevision revision : net.revisions) {
            // Physical deletes have old-row evidence; the deletion revision itself is source journal evidence.
            if (revision.after().presence() == Presence.DELETED && net.owner.collection().startsWith("inventory.")) continue;
            boolean found = seenAfter.stream().anyMatch(e -> e.startsWith(evidencePrefix(net.owner, revision.after())));
            if (!found) throw new IllegalArgumentException("journal revision lacks captured row");
        }
        // Every PRESENT journal before image must be corroborated. Retained projection tombstones also have row images.
        for (String evidence : beforeEvidence) {
            if (!seenBefore.contains(evidence) && !evidence.contains("\"presence\":\"DELETED\""))
                throw new IllegalArgumentException("journal before image lacks captured row");
        }
        pending.nets.addAll(nets.values());
        return pending;
    }

    static JsonNode jsonValue(JsonNode node) { return node != null && node.isTextual() ? parse(node.textValue()) : node; }
    static OwnerState state(JsonNode entry, String side) {
        Presence presence = Presence.valueOf(text(entry, side + "_presence"));
        return new OwnerState(presence, presence == Presence.ABSENT ? null : number(entry, side + "_version"));
    }
    OwnerIdentity owner(String collection, JsonNode key) {
        Map<String, TypedValue> values = new TreeMap<>();
        if (key == null || !key.isObject()) throw new IllegalArgumentException("invalid source owner key");
        key.fields().forEachRemaining(e -> values.put(e.getKey(), new TypedValue(
                Set.of("id", "customer_id", "order_id").contains(e.getKey()) ? "int64" : "text", e.getValue().asText())));
        return new OwnerIdentity(database, incarnation, collection, values);
    }
    OwnerIdentity ownerFromRow(String collection, JsonNode row) {
        List<String> keys = switch (collection) {
            case "inventory.customers", "inventory.orders" -> List.of("id");
            case "inventory.order_items" -> List.of("order_id", "product_name");
            case "inventory.products" -> List.of("name");
            default -> throw new IllegalArgumentException("unsupported row collection");
        };
        ObjectNode key = ProtocolV2Codec.mapper().createObjectNode();
        keys.forEach(k -> key.set(k, row.get(k)));
        return owner(collection, key);
    }
    static JsonNode image(OwnerIdentity owner, JsonNode image, OwnerState state) {
        if (state.presence() != Presence.PRESENT) return null;
        if (image == null || image.isNull()) throw new IllegalArgumentException("present journal image missing");
        ObjectNode result = image.deepCopy();
        if (result.has("version") && number(result, "version") != state.version())
            throw new IllegalArgumentException("source image version mismatch");
        result.put("version", state.version());
        for (String field : List.of("created_at", "updated_at", "order_date")) {
            if (result.hasNonNull(field)) result.put(field, OffsetDateTime.parse(result.get(field).asText()).toInstant().toString());
        }
        for (String field : List.of("balance", "unit_price", "total_price", "current_price", "price_at_purchase")) {
            if (result.hasNonNull(field)) result.put(field, new BigDecimal(result.get(field).asText()));
        }
        return result;
    }
    static boolean same(JsonNode a, JsonNode b) { return ProtocolV2Codec.json(a).equals(ProtocolV2Codec.json(b)); }
    static String evidencePrefix(OwnerIdentity owner, OwnerState state) {
        return ProtocolV2Codec.json(owner) + "|" + ProtocolV2Codec.json(state) + "|";
    }
    static String evidence(OwnerIdentity owner, OwnerState state, JsonNode image) {
        return evidencePrefix(owner, state) + ProtocolV2Codec.json(image);
    }
}
