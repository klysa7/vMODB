package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

/**
 * A single data point on the throughput frontier.
 *
 * <p>One ExperimentPoint is produced per experiment run in the HATtrick
 * saturation method. The full frontier is built from a 6×6 grid of these.
 *
 * <pre>
 * tau          — number of T-client threads fixed for this run
 * alpha        — number of A-client threads fixed for this run
 * tps          — observed T-throughput (transactions/second)
 * qps          — observed A-throughput (queries/second)
 * avgFreshness — average freshness score in seconds (0 = perfect)
 * p99Freshness — 99th-percentile freshness score in seconds
 * durationSec  — how long the measurement window ran
 * </pre>
 */
public final class ExperimentPoint {

    public final int    tau;
    public final int    alpha;
    public final double tps;
    public final double qps;
    public final double avgFreshness;
    public final double p99Freshness;
    public final double durationSec;

    public ExperimentPoint(int tau, int alpha,
                           double tps, double qps,
                           double avgFreshness, double p99Freshness,
                           double durationSec) {
        this.tau          = tau;
        this.alpha        = alpha;
        this.tps          = tps;
        this.qps          = qps;
        this.avgFreshness = avgFreshness;
        this.p99Freshness = p99Freshness;
        this.durationSec  = durationSec;
    }

    /** CSV line: tau,alpha,tps,qps,avg_freshness_s,p99_freshness_s,duration_s */
    public String toCsv() {
        return String.format("%d,%d,%.3f,%.3f,%.6f,%.6f,%.1f",
                tau, alpha, tps, qps, avgFreshness, p99Freshness, durationSec);
    }

    public static String csvHeader() {
        return "tau,alpha,tps,qps,avg_freshness_s,p99_freshness_s,duration_s";
    }

    @Override
    public String toString() {
        return String.format(
                "ExperimentPoint[τ=%d α=%d | tps=%.1f qps=%.1f | fresh_avg=%.3fs p99=%.3fs]",
                tau, alpha, tps, qps, avgFreshness, p99Freshness);
    }
}