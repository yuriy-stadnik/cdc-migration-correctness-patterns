package com.example.cdc.projector;

import java.util.ArrayList;
import java.util.List;

/** Pure assembly invariants, separated so duplicate/missing/restart behavior is directly testable. */
public final class AssemblyRules {
    private AssemblyRules() {}

    public enum FragmentResult { INSERTED, IDENTICAL_DUPLICATE }

    public static FragmentResult add(ProjectorState.Assembly tx, ProjectorState.CapturedChange change, String hash) {
        String prior = tx.changeHashes.putIfAbsent(change.sequence, hash);
        if (prior != null) {
            if (!prior.equals(hash)) throw new IllegalArgumentException("conflicting duplicate source fragment");
            return FragmentResult.IDENTICAL_DUPLICATE;
        }
        tx.changes.put(change.sequence, change);
        return FragmentResult.INSERTED;
    }

    public static boolean complete(ProjectorState.Assembly tx) {
        return tx.beginSeen && tx.endSeen && tx.expectedEventCount >= 0
                && tx.changes.size() == tx.expectedEventCount && missingSequences(tx).isEmpty()
                && collectionCountsMatch(tx);
    }

    private static boolean collectionCountsMatch(ProjectorState.Assembly tx) {
        if (tx.expectedCollectionCounts == null) return false;
        java.util.Map<String, Integer> actual = new java.util.HashMap<>();
        tx.changes.values().forEach(c -> actual.merge(c.collection, 1, Integer::sum));
        return actual.equals(tx.expectedCollectionCounts);
    }

    public static List<Long> missingSequences(ProjectorState.Assembly tx) {
        List<Long> missing = new ArrayList<>();
        if (tx.expectedEventCount < 0) return missing;
        for (long sequence = 1; sequence <= tx.expectedEventCount; sequence++) {
            if (!tx.changes.containsKey(sequence)) missing.add(sequence);
        }
        return List.copyOf(missing);
    }
}
