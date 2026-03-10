package dk.ku.di.dms.vms.tpcc.order;

import java.util.concurrent.atomic.AtomicLong;

public final class HATtrickCounters {

    private HATtrickCounters() {}

    public static final AtomicLong ordersCount    = new AtomicLong(0);
    public static final AtomicLong orderLineCount = new AtomicLong(0);
    public static final AtomicLong historyCount   = new AtomicLong(0);

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

    /** Atomically increments and returns the new txnnum for this client. */
    public static long incrementFreshness(int clientId) {
        if (clientId >= 1 && clientId <= MAX_CLIENTS) {
            return freshnessCache[clientId].incrementAndGet();
        }
        return 0L;
    }

    /** Called by OrderHttpHandler inside executeCountQuery(). */
    public static long getFreshness(int clientId) {
        if (clientId >= 1 && clientId <= MAX_CLIENTS) {
            return freshnessCache[clientId].get();
        }
        return 0L;
    }
}