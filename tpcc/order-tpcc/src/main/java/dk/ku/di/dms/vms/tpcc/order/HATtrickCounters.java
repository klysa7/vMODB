package dk.ku.di.dms.vms.tpcc.order;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-free counters shared between OrderService (writer) and
 * OrderHttpHandler (reader) so that A-client COUNT queries never
 * need to call beginTransaction() or touch the VMS index machinery.
 *
 * ordersCount    — incremented each time an Order row is inserted
 * orderLineCount — incremented each time an OrderLine row is inserted
 * historyCount   — incremented each time a History row is inserted
 * freshnessCache — freshnessCache[j] mirrors the latest txnnum committed
 *                  by T-client j (same value stored in the FRESHNESS table,
 *                  but readable without a transaction context)
 *
 * Initialised once in OrderHttpHandler.put() after population finishes.
 */
public final class HATtrickCounters {

    private HATtrickCounters() {}

    // ── row counters (populated during data-load, then incremented live) ──
    public static final AtomicLong ordersCount    = new AtomicLong(0);
    public static final AtomicLong orderLineCount = new AtomicLong(0);
    public static final AtomicLong historyCount   = new AtomicLong(0);

    // ── per-T-client freshness cache ──────────────────────────────────────
    // Index 0 unused; client IDs start at 1.
    // Sized for up to 64 T-clients; more than enough for any experiment.
    private static final int MAX_CLIENTS = 64;
    private static final AtomicLong[] freshnessCache =
            new AtomicLong[MAX_CLIENTS + 1];

    static {
        for (int i = 0; i <= MAX_CLIENTS; i++) {
            freshnessCache[i] = new AtomicLong(0);
        }
    }

    /** Called by OrderService after each transaction commits. */
    public static void setFreshness(int clientId, long txnnum) {
        if (clientId >= 1 && clientId <= MAX_CLIENTS) {
            freshnessCache[clientId].set(txnnum);
        }
    }

    /** Called by OrderHttpHandler inside executeCountQuery(). */
    public static long getFreshness(int clientId) {
        if (clientId >= 1 && clientId <= MAX_CLIENTS) {
            return freshnessCache[clientId].get();
        }
        return 0L;
    }
}