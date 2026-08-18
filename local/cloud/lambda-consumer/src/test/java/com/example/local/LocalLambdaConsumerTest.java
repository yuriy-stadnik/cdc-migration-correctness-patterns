package com.example.local;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocalLambdaConsumerTest {

    @Test
    void upsertCustomerBindsPayloadAndSourceMetadata() throws Exception {
        Connection clientConn = mock(Connection.class);
        Connection operationalConn = mock(Connection.class);
        PreparedStatement processedPs = mock(PreparedStatement.class);
        PreparedStatement businessPs = mock(PreparedStatement.class);
        PreparedStatement pendingSelectPs = mock(PreparedStatement.class);
        ResultSet pendingRows = mock(ResultSet.class);
        Savepoint savepoint = mock(Savepoint.class);
        when(clientConn.getAutoCommit()).thenReturn(true);
        when(clientConn.setSavepoint(anyString())).thenReturn(savepoint);
        when(clientConn.prepareStatement(anyString())).thenReturn(processedPs, businessPs, pendingSelectPs);
        when(processedPs.executeUpdate()).thenReturn(1);
        when(pendingSelectPs.executeQuery()).thenReturn(pendingRows);
        when(pendingRows.next()).thenReturn(false);

        Map<String, Object> payload = Map.ofEntries(
                Map.entry("id", "42"),
                Map.entry("first_name", "Ada"),
                Map.entry("last_name", "Lovelace"),
                Map.entry("email", "ada@example.com"),
                Map.entry("status", "active"),
                Map.entry("created_at", "2026-07-31T10:15:30Z"),
                Map.entry("updated_at", "2026-07-31T10:16:30Z"),
                Map.entry("source_record_type", "u"),
                Map.entry("source_ts_ms", "1785492930000"),
                Map.entry("source_tx_id", "tx-1"),
                Map.entry("idempotency_key", "tx-1|3|client.customers|42"),
                Map.entry("source_tx_total_order", 3),
                Map.entry("source_tx_data_collection_order", 2)
        );

        LocalLambdaConsumer.upsert(clientConn, operationalConn, "client.customers", 0, 100L, payload);

        verify(processedPs).setString(1, "tx-1|3|client.customers|42");
        verify(processedPs).setString(2, "tx-1");
        verify(processedPs).setLong(3, 3L);
        verify(processedPs).setString(4, "client.customers");
        verify(processedPs).setString(5, "42");
        verify(processedPs).executeUpdate();
        verify(clientConn).releaseSavepoint(savepoint);
        verify(clientConn).commit();
        verify(clientConn).setAutoCommit(false);
        verify(clientConn).setAutoCommit(true);
        verify(operationalConn, never()).prepareStatement(anyString());
        verify(businessPs).setLong(1, 42L);
        verify(businessPs).setString(2, "Ada");
        verify(businessPs).setString(3, "Lovelace");
        verify(businessPs).setString(4, "ada@example.com");
        verify(businessPs).setString(5, "active");
        verify(businessPs).setString(6, "2026-07-31T10:15:30Z");
        verify(businessPs).setString(7, "2026-07-31T10:16:30Z");
        verify(businessPs).setString(8, "u");
        verify(businessPs).setLong(9, 1785492930000L);
        verify(businessPs).setString(10, "tx-1");
        verify(businessPs).setLong(11, 3L);
        verify(businessPs).setLong(12, 2L);
        verify(businessPs).setString(13, "tx-1|3|client.customers|42");
        verify(businessPs).executeUpdate();
    }

    @Test
    void duplicateProcessedEventSkipsBusinessWrite() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement processedPs = mock(PreparedStatement.class);
        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.prepareStatement(anyString())).thenReturn(processedPs);
        when(processedPs.executeUpdate()).thenReturn(0);

        Map<String, Object> payload = Map.of(
                "source_tx_id", "tx-duplicate",
                "source_tx_total_order", 9
        );

        LocalLambdaConsumer.processBusinessEvent(
                conn,
                "client.customers",
                "42",
                payload,
                () -> {
                    throw new AssertionError("business write should not run for duplicate idempotency key");
                }
        );

        verify(processedPs).setString(1, "tx-duplicate|9|client.customers|42");
        verify(processedPs).executeUpdate();
        verify(conn).commit();
    }

    @Test
    void foreignKeyViolationIsBufferedWithoutFailingTransaction() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement processedPs = mock(PreparedStatement.class);
        PreparedStatement pendingPs = mock(PreparedStatement.class);
        Savepoint savepoint = mock(Savepoint.class);
        SQLException fkError = new SQLException(
                "ERROR: insert or update on table \"addresses\" violates foreign key constraint \"fk__client.addresses__client.customers\"",
                "23503"
        );
        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.setSavepoint(anyString())).thenReturn(savepoint);
        when(conn.prepareStatement(anyString())).thenReturn(processedPs, pendingPs);
        when(processedPs.executeUpdate()).thenReturn(1);

        Map<String, Object> payload = Map.of(
                "customer_id", 42,
                "address_type", "HOME",
                "source_tx_id", "tx-4",
                "source_tx_total_order", 5,
                "idempotency_key", "tx-4|5|client.addresses|customer_id=42|address_type=HOME"
        );

        LocalLambdaConsumer.processBusinessEvent(
                conn,
                "client.addresses",
                "customer_id=42|address_type=HOME",
                payload,
                new LocalLambdaConsumer.ChildDependency(
                        "fk__client.addresses__client.customers",
                        "client",
                        "addresses",
                        "client",
                        "customers"
                ),
                () -> {
                    throw fkError;
                },
                null
        );

        verify(conn).rollback(savepoint);
        verify(pendingPs).setString(1, "tx-4|5|client.addresses|customer_id=42|address_type=HOME");
        verify(pendingPs).setString(2, "fk__client.addresses__client.customers");
        verify(pendingPs).setString(3, "client");
        verify(pendingPs).setString(4, "addresses");
        verify(pendingPs).setString(5, "client");
        verify(pendingPs).setString(6, "customers");
        verify(pendingPs).executeUpdate();
        verify(conn).commit();
    }

    @Test
    void parseForeignKeyNameAndDependencySides() {
        SQLException fkError = new SQLException(
                "violates foreign key constraint \"fk__operational.order_items__operational.products\"",
                "23503"
        );

        LocalLambdaConsumer.ChildDependency dependency = LocalLambdaConsumer.resolveDependency(
                "operational.order_items",
                fkError,
                new LocalLambdaConsumer.ChildDependency("fallback", "a", "b", "c", "d")
        );

        assertEquals("fk__operational.order_items__operational.products", dependency.fkName());
        assertEquals("operational", dependency.childSchema());
        assertEquals("order_items", dependency.childTable());
        assertEquals("operational", dependency.parentSchema());
        assertEquals("products", dependency.parentTable());
    }

    @Test
    void idempotencyKeyUsesSourceTransactionOrderTopicAndBusinessKey() {
        Map<String, Object> payload = Map.of(
                "source_tx_id", "tx-3",
                "source_tx_total_order", "12"
        );

        assertEquals(
                "tx-3|12|operational.order_items|order_id=99|product_name=Keyboard",
                LocalLambdaConsumer.idempotencyKey(
                        payload,
                        "operational.order_items",
                        LocalLambdaConsumer.businessKey("order_id", 99, "product_name", "Keyboard")
                )
        );
    }

    @Test
    void idempotencyKeyUsesUpstreamKeyAndNoTxFallbackForSnapshotRecords() {
        assertEquals(
                "no-tx|-1|client.customers|1",
                LocalLambdaConsumer.idempotencyKey(
                        Map.of("idempotency_key", "no-tx|-1|client.customers|1"),
                        "client.customers",
                        "1"
                )
        );
        assertEquals("no-tx", LocalLambdaConsumer.processedSourceTxId(Map.of()));
    }

    @Test
    void upsertTransactionMetadataWritesBothDatabasesAndDefaultsMissingCollections() throws Exception {
        Connection clientConn = mock(Connection.class);
        Connection operationalConn = mock(Connection.class);
        PreparedStatement clientPs = mock(PreparedStatement.class);
        PreparedStatement operationalPs = mock(PreparedStatement.class);
        when(clientConn.prepareStatement(anyString())).thenReturn(clientPs);
        when(operationalConn.prepareStatement(anyString())).thenReturn(operationalPs);

        Map<String, Object> payload = Map.of(
                "id", "tx-2",
                "status", "BEGIN",
                "event_count", "",
                "ts_ms", 1785492930123L
        );

        LocalLambdaConsumer.upsert(clientConn, operationalConn, "pg1.transaction", 1, 10L, payload);

        for (PreparedStatement ps : List.of(clientPs, operationalPs)) {
            verify(ps).setString(1, "tx-2");
            verify(ps).setString(2, "BEGIN");
            verify(ps).setObject(3, null);
            verify(ps).setString(4, "[]");
            verify(ps).setLong(5, 1785492930123L);
            verify(ps).executeUpdate();
        }
    }

    @Test
    void unknownClientTopicInsertsRawEventIntoClientEventsTable() throws Exception {
        Connection clientConn = mock(Connection.class);
        Connection operationalConn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        when(clientConn.prepareStatement(sql.capture())).thenReturn(ps);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", 7);
        payload.put("name", "sample");

        LocalLambdaConsumer.upsert(
                clientConn,
                operationalConn,
                "client.unmapped",
                2,
                99L,
                payload
        );

        assertTrue(sql.getValue().contains("INSERT INTO client.events"));
        verify(operationalConn, never()).prepareStatement(anyString());
        verify(ps).setString(1, "client.unmapped");
        verify(ps).setInt(2, 2);
        verify(ps).setLong(3, 99L);
        verify(ps).setString(4, "{\"id\":7,\"name\":\"sample\"}");
        verify(ps).executeUpdate();
    }

    @Test
    void unknownOperationalTopicInsertsRawEventIntoOperationalEventsTable() throws Exception {
        Connection clientConn = mock(Connection.class);
        Connection operationalConn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        when(operationalConn.prepareStatement(sql.capture())).thenReturn(ps);

        LocalLambdaConsumer.upsert(
                clientConn,
                operationalConn,
                "inventory.unmapped",
                3,
                101L,
                Map.of("id", 8)
        );

        assertTrue(sql.getValue().contains("INSERT INTO operational.events"));
        verify(clientConn, never()).prepareStatement(anyString());
        verify(ps).setString(1, "inventory.unmapped");
        verify(ps).setInt(2, 3);
        verify(ps).setLong(3, 101L);
        verify(ps).setString(4, "{\"id\":8}");
        verify(ps).executeUpdate();
    }

    @Test
    void payloadConvertersRejectMissingRequiredFieldsAndParseOptionalValues() {
        Map<String, Object> payload = Map.of(
                "long_as_number", 12L,
                "long_as_text", "13",
                "int_as_text", "14",
                "decimal_as_text", "19.95",
                "blank_long", " "
        );

        assertEquals(12L, LocalLambdaConsumer.requiredLong(payload, "long_as_number"));
        assertEquals(13L, LocalLambdaConsumer.requiredLong(payload, "long_as_text"));
        assertEquals(14, LocalLambdaConsumer.requiredInt(payload, "int_as_text"));
        assertEquals(new BigDecimal("19.95"), LocalLambdaConsumer.optionalDecimal(payload, "decimal_as_text"));
        assertEquals(null, LocalLambdaConsumer.optionalLong(payload, "blank_long"));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> LocalLambdaConsumer.requiredText(Map.of("name", " "), "name")
        );
        assertEquals("Missing required field: name", error.getMessage());
    }
}
