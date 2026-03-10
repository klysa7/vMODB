package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

/**
 * Records the result of a single analytical query execution.
 *
 * <pre>
 * queryType      — CA1 / CA2 / CA3 (identifies the COUNT query)
 * queryStartNs   — System.nanoTime() immediately before sending the SQL
 * queryEndNs     — System.nanoTime() immediately after the result arrived
 * returnedTxnnums — txnnum values returned in FRESHNESS columns (one per T-client)
 * resultCount    — the COUNT(*) value returned by the query
 * freshnessScore — computed after the fact by FreshnessTracker (seconds, 0 = perfect)
 * </pre>
 *
 * The freshness score is not set at construction time — it is filled in by
 * {@link FreshnessTracker#score(QueryRecord)} after the query completes.
 */
public final class QueryRecord {

    public enum QueryType { CA1_ORDERS, CA2_ORDER_LINE, CA3_HISTORY }

    public final QueryType queryType;
    public final long      queryStartNs;
    public final long      queryEndNs;
    /** returnedTxnnums[j-1] = txnnum seen from T-client j */
    public final long[]    returnedTxnnums;
    public final long      resultCount;

    /** Filled in by FreshnessTracker after scoring. Unit: seconds. */
    public double freshnessScore = 0.0;

    public QueryRecord(QueryType queryType,
                       long queryStartNs,
                       long queryEndNs,
                       long[] returnedTxnnums,
                       long resultCount) {
        this.queryType       = queryType;
        this.queryStartNs    = queryStartNs;
        this.queryEndNs      = queryEndNs;
        this.returnedTxnnums = returnedTxnnums;
        this.resultCount     = resultCount;
    }

    public double latencySeconds() {
        return (queryEndNs - queryStartNs) / 1e9;
    }
}