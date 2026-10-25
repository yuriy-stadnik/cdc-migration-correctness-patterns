package com.example.cdc.apply;

import java.sql.SQLException;
import java.util.Optional;

/** Public v2-only ingestion and atomic apply facade used by Lambda and the local worker. */
public final class TransactionApplyService {
    public enum StageResult { STAGED, DUPLICATE, QUARANTINED }
    public record ApplyResult(String transactionId, boolean applied, String detail) {}

    private final V2TransactionApplyService service;

    public TransactionApplyService(ConnectionProvider connections, String workerId) {
        this.service = new V2TransactionApplyService(connections);
    }

    public StageResult stage(String topic, int partition, long offset, String json) throws SQLException {
        return service.stage(topic, json);
    }

    public Optional<ApplyResult> applyNext() throws SQLException {
        return service.applyNext();
    }

    public int drain(int maxTransactions) throws SQLException {
        int applied = 0;
        for (int i = 0; i < maxTransactions; i++) {
            Optional<ApplyResult> result = applyNext();
            if (result.isEmpty()) break;
            if (result.get().applied()) applied++;
        }
        return applied;
    }
}
