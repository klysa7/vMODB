package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

/**
 * HATtrick Phase 1 — COUNT queries on existing TPC-C tables.
 *
 * <p>Each query cross-joins all FRESHNESS_j tables (one per T-client).
 * Because the query runs under snapshot isolation, the returned txnnum
 * values reflect exactly which transactions were visible in that snapshot.
 *
 * <p>The COUNT queries validate the entire freshness pipeline before
 * any SSB schema work (LINEORDER / DATE) is begun.
 *
 * <h3>What each query measures</h3>
 * <ul>
 *   <li>CA1: orders table — written by New Order (grows on every transaction)</li>
 *   <li>CA2: order_line  — written by New Order (grows fastest, ~10× orders)</li>
 *   <li>CA3: history     — written by Payment (~48% of all transactions)</li>
 * </ul>
 *
 * <h3>Expected result shape</h3>
 * <pre>
 *   CA1 → 1 row: (count_val, txnnum_1, txnnum_2)
 *   CA2 → 1 row: (count_val, txnnum_1, txnnum_2)
 *   CA3 → 1 row: (count_val, txnnum_1, txnnum_2)
 * </pre>
 */
public final class HATtrickQueries {

    private HATtrickQueries() {}

    // ─────────────────────────────────────────────────────────────────────────
    // SQL builders — numTClients determines how many FRESHNESS joins are added
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * CA1: COUNT orders.
     * Freshness is determined by New Order transactions — orders grows on every
     * New Order commit. If the snapshot is stale, the count is lower than the
     * true committed count.
     */
    public static String ca1Orders(int numTClients) {
        return buildCountQuery("orders", numTClients);
    }

    /**
     * CA2: COUNT order_line.
     * This is the fastest-growing table (~10 rows per New Order).
     * Provides the strongest freshness signal for New Order transactions.
     */
    public static String ca2OrderLine(int numTClients) {
        return buildCountQuery("order_line", numTClients);
    }

    /**
     * CA3: COUNT history.
     * Written by Payment transactions only. Provides freshness signal
     * specifically for the Payment path.
     */
    public static String ca3History(int numTClients) {
        return buildCountQuery("history", numTClients);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Internal builder
    // ─────────────────────────────────────────────────────────────────────────

    private static String buildCountQuery(String tableName, int numTClients) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*)");

        // FRESHNESS columns
        for (int j = 1; j <= numTClients; j++) {
            sb.append(", f").append(j).append(".txnnum AS txnnum_").append(j);
        }

        sb.append(" FROM ").append(tableName);

        // FRESHNESS joins
        for (int j = 1; j <= numTClients; j++) {
            sb.append(", freshness f").append(j)
                    .append(" /* client_id = ").append(j).append(" */");
        }

        // WHERE clause to pin each alias to its client_id row
        sb.append(" WHERE ");
        for (int j = 1; j <= numTClients; j++) {
            if (j > 1) sb.append(" AND ");
            sb.append("f").append(j).append(".client_id = ").append(j);
        }

        return sb.toString();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Hardcoded 2-client versions for quick reference
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Hardcoded CA1 for 2 T-clients — useful for unit tests / debugging.
     * <pre>
     * SELECT COUNT(*), f1.txnnum AS txnnum_1, f2.txnnum AS txnnum_2
     * FROM orders,
     *      freshness f1,
     *      freshness f2
     * WHERE f1.client_id = 1
     *   AND f2.client_id = 2
     * </pre>
     */
    public static final String CA1_TWO_CLIENTS =
            "SELECT COUNT(*), f1.txnnum AS txnnum_1, f2.txnnum AS txnnum_2 " +
                    "FROM orders, freshness f1, freshness f2 " +
                    "WHERE f1.client_id = 1 AND f2.client_id = 2";

    public static final String CA2_TWO_CLIENTS =
            "SELECT COUNT(*), f1.txnnum AS txnnum_1, f2.txnnum AS txnnum_2 " +
                    "FROM order_line, freshness f1, freshness f2 " +
                    "WHERE f1.client_id = 1 AND f2.client_id = 2";

    public static final String CA3_TWO_CLIENTS =
            "SELECT COUNT(*), f1.txnnum AS txnnum_1, f2.txnnum AS txnnum_2 " +
                    "FROM history, freshness f1, freshness f2 " +
                    "WHERE f1.client_id = 1 AND f2.client_id = 2";
}