package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void idempotencyKeyUsesSourceTransactionOrderTopicAndBusinessKey() throws Exception {
        JsonNode payload = MAPPER.readTree("""
            {
              "source_tx_id": "tx-3",
              "source_tx_total_order": "12"
            }
        """);

        assertEquals(
            "tx-3|12|operational.order_items|order_id=99|product_name=Keyboard",
            Handler.idempotencyKey(
                payload,
                "operational.order_items",
                Handler.businessKey("order_id", 99, "product_name", "Keyboard")
            )
        );
    }

    @Test
    void idempotencyKeyUsesUpstreamKeyAndNoTxFallbackForSnapshotRecords() throws Exception {
        JsonNode payload = MAPPER.readTree("""
            {
              "idempotency_key": "no-tx|-1|client.customers|1"
            }
        """);

        assertEquals(
            "no-tx|-1|client.customers|1",
            Handler.idempotencyKey(payload, "client.customers", "1")
        );
        assertEquals("no-tx", Handler.processedSourceTxId(MAPPER.createObjectNode()));
    }

    @Test
    void parseForeignKeyNameAndDependencySides() {
        SQLException fkError = new SQLException(
            "violates foreign key constraint \"fk__operational.order_items__operational.products\"",
            "23503"
        );

        Handler.ChildDependency dependency = Handler.resolveDependency(
            "operational.order_items",
            fkError,
            new Handler.ChildDependency("fallback", "a", "b", "c", "d")
        );

        assertEquals("fk__operational.order_items__operational.products", dependency.fkName());
        assertEquals("operational", dependency.childSchema());
        assertEquals("order_items", dependency.childTable());
        assertEquals("operational", dependency.parentSchema());
        assertEquals("products", dependency.parentTable());
    }

    @Test
    void requiredTextRejectsMissingFields() {
        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> Handler.requiredText(MAPPER.createObjectNode().put("name", " "), "name")
        );

        assertEquals("Missing required field: name", error.getMessage());
    }
}
