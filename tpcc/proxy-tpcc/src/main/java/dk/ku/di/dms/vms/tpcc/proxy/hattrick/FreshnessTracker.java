package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Client-side freshness score computation.
 *
 * <h3>HATtrick Section 4.2 — the algorithm</h3>
 * <pre>
 * f_Aq = max(0,  t_query_start  −  commit_time_of_first_not_seen_txn)
 * </pre>
 * where "first not seen" for client j = the first transaction from client j
 * whose txnnum > returned_txnnum_j AND whose commitTime < t_query_start.
 *
 * <h3>How this class is used</h3>
 * <ol>
 *   <li>T-clients call {@link #recordCommit} after every committed transaction.</li>
 *   <li>A-clients call {@link #score} after every query to compute and store
 *       the freshness score for that query.</li>
 *   <li>At experiment end, call {@link #p99FreshnessSeconds} and
 *       {@link #averageFreshnessSeconds} to aggregate results.</li>
 * </ol>
 *
 * All methods are thread-safe. The commit log is kept in per-client lists
 * sorted by txnnum (which is already monotonic — no sorting needed).
 */
public final class FreshnessTracker {

    /**
     * Commit log: client_id → ordered list of CommitRecords.
     * CommitRecords are appended in txnnum order (naturally monotonic).
     */
    private final Map<Integer, List<CommitRecord>> commitLog = new ConcurrentHashMap<>();

    /** All scored query records (for aggregation at experiment end). */
    private final List<QueryRecord> scoredQueries = new CopyOnWriteArrayList<>();

    private final int numTClients;

    public FreshnessTracker(int numTClients) {
        this.numTClients = numTClients;
        for (int j = 1; j <= numTClients; j++) {
            commitLog.put(j, new CopyOnWriteArrayList<>());
        }
    }

    /**
     * Called by a T-client immediately after receiving the commit acknowledgement.
     * The commit time is measured on the client side (System.nanoTime()) to avoid
     * distributed clock synchronisation issues.
     */
    public void recordCommit(CommitRecord record) {
        commitLog.get(record.client_id).add(record);
    }

    /**
     * Computes the freshness score for a completed query and stores it.
     *
     * @param qr    the query record (returnedTxnnums and queryStartNs must be set)
     * @return      freshness score in seconds (0 = perfect freshness)
     */
    public double score(QueryRecord qr) {
        double maxStaleness = 0.0;

        for (int j = 1; j <= numTClients; j++) {
            long seenTxnnum = qr.returnedTxnnums[j - 1];
            double stale = stalenessForClient(j, seenTxnnum, qr.queryStartNs);
            if (stale > maxStaleness) {
                maxStaleness = stale;
            }
        }

        qr.freshnessScore = maxStaleness;
        scoredQueries.add(qr);
        return maxStaleness;
    }

    /**
     * For client j: find the first txnnum > seenTxnnum that committed
     * before queryStartNs. If such a transaction exists, it was missed —
     * staleness = (queryStartNs - commitTimeNs) / 1e9 seconds.
     */
    private double stalenessForClient(int clientId, long seenTxnnum, long queryStartNs) {
        List<CommitRecord> log = commitLog.get(clientId);
        if (log == null) return 0.0;

        for (CommitRecord cr : log) {
            // First txnnum greater than what the snapshot saw
            if (cr.txnnum > seenTxnnum) {
                // Did it commit before the query started?
                if (cr.commitTimeNs < queryStartNs) {
                    // The query missed this transaction — compute staleness
                    return Math.max(0.0, (queryStartNs - cr.commitTimeNs) / 1e9);
                }
                // It committed after the query started — not expected to be seen
                return 0.0;
            }
        }
        // No transaction beyond seenTxnnum found — perfectly fresh
        return 0.0;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Aggregation methods
    // ─────────────────────────────────────────────────────────────────────────

    public double averageFreshnessSeconds() {
        return scoredQueries.stream()
                .mapToDouble(q -> q.freshnessScore)
                .average()
                .orElse(0.0);
    }

    public double p99FreshnessSeconds() {
        List<Double> scores = new ArrayList<>();
        for (QueryRecord q : scoredQueries) scores.add(q.freshnessScore);
        if (scores.isEmpty()) return 0.0;
        Collections.sort(scores);
        int idx = (int) Math.ceil(0.99 * scores.size()) - 1;
        return scores.get(Math.max(0, idx));
    }

    public double maxFreshnessSeconds() {
        return scoredQueries.stream()
                .mapToDouble(q -> q.freshnessScore)
                .max()
                .orElse(0.0);
    }

    public int queryCount() {
        return scoredQueries.size();
    }

    public List<QueryRecord> getScoredQueries() {
        return Collections.unmodifiableList(scoredQueries);
    }

    /** Returns the txnnum for client j from the latest commit record seen. */
    public long latestTxnnumForClient(int clientId) {
        List<CommitRecord> log = commitLog.get(clientId);
        if (log == null || log.isEmpty()) return 0L;
        return log.get(log.size() - 1).txnnum;
    }

    public void printSummary(String label) {
        System.out.printf(
                "[Freshness] %s  queries=%d  avg=%.3fs  p99=%.3fs  max=%.3fs%n",
                label,
                queryCount(),
                averageFreshnessSeconds(),
                p99FreshnessSeconds(),
                maxFreshnessSeconds()
        );
    }
}