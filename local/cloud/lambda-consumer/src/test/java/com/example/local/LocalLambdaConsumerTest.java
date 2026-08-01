package com.example.local;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
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
        PreparedStatement ps = mock(PreparedStatement.class);
        when(clientConn.prepareStatement(anyString())).thenReturn(ps);

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
                Map.entry("source_tx_total_order", 3),
                Map.entry("source_tx_data_collection_order", 2)
        );

        LocalLambdaConsumer.upsert(clientConn, operationalConn, "client.customers", 0, 100L, payload);

        verify(clientConn).prepareStatement(anyString());
        verify(operationalConn, never()).prepareStatement(anyString());
        verify(ps).setLong(1, 42L);
        verify(ps).setString(2, "Ada");
        verify(ps).setString(3, "Lovelace");
        verify(ps).setString(4, "ada@example.com");
        verify(ps).setString(5, "active");
        verify(ps).setString(6, "2026-07-31T10:15:30Z");
        verify(ps).setString(7, "2026-07-31T10:16:30Z");
        verify(ps).setString(8, "u");
        verify(ps).setLong(9, 1785492930000L);
        verify(ps).setString(10, "tx-1");
        verify(ps).setLong(11, 3L);
        verify(ps).setLong(12, 2L);
        verify(ps).executeUpdate();
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

        LocalLambdaConsumer.upsert(
                clientConn,
                operationalConn,
                "client.unmapped",
                2,
                99L,
                Map.of("id", 7, "name", "sample")
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
