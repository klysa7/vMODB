package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.definition.key.composite.BaseComposite;

/**
 * AOO-6 FIX: Reusable probe key for updatesPerKeyMap lookups.
 *
 * PROBLEM:
 *   PrimaryIndex.scanSlotRangeFloatSum() calls readPkFromAddress() + buildRecordKey()
 *   for every active slot. For order_line with a 4-INT PK (ol_o_id, ol_d_id, ol_w_id,
 *   ol_number), buildRecordKey() allocates:
 *     - new Object[4]        (~32 bytes)
 *     - new QuadrupleCompositeKey (~48 bytes)
 *   Per scan: 300K slots × 80 bytes = ~22MB of immediately-discarded heap objects.
 *   These drive minor GC pauses under both OLTP and OLAP load.
 *
 * FIX:
 *   Allocate ONE MutableIntArrayKey per parallel scan worker at construction time.
 *   For each slot, update the internal int[] in-place, recompute hashCode(), and
 *   use it as a probe key for updatesPerKeyMap.get(). Zero heap allocation per row.
 *
 * CORRECTNESS:
 *   ConcurrentHashMap.get() calls probeKey.equals(storedKey). Our equals() handles
 *   all BaseComposite subtypes (Pair, Triple, Quadruple, N) by calling getValue(i)
 *   on the stored key. hashCode() replicates the exact polynomial used by all
 *   composite key classes: h = 31*h + values[i] (for INT columns, Integer.hashCode==value).
 *
 * SAFETY INVARIANT:
 *   This key MUST NEVER be passed to updatesPerKeyMap.put() or stored anywhere.
 *   It is a probe-only key. The keys stored in the map are the immutable keys
 *   inserted at OLTP write time (QuadrupleCompositeKey etc.).
 *   Only use for: updatesPerKeyMap.get(probeKey).
 *
 * USAGE (in scanSlotRangeFloatSum):
 *   MutableIntArrayKey probe = new MutableIntArrayKey(pkColumnCount);
 *   // per slot:
 *   probe.setValues(v0, v1, v2, v3);   // update in place
 *   OperationSetOfKey opSet = updatesPerKeyMap.get(probe);  // zero alloc
 *
 * Cite: QuestDB Discipline 1 Slide 1 — "No allocation on hot-path."
 *   Slide 4 — "The fastest allocation is the one that never happens."
 *   PDF AOO-6 — MutableIntArrayKey probe pattern.
 */
public final class MutableIntArrayKey implements IKey {

    // Mutable int values — updated in place for each slot.
    // Package-visible so PrimaryIndex (same project, different package) can
    // access via the update methods below without reflection.
    private final int[] vals;

    // Cached hash — recomputed on each setValues() call.
    // Stored so hashCode() is O(1) during the ConcurrentHashMap bucket probe.
    private int hash;

    public MutableIntArrayKey(int size) {
        this.vals = new int[size];
    }

    // ── Update methods — called once per slot in the scan hot loop ────────────

    /** Update all 4 values and recompute hash. For order_line (4-INT PK). */
    public void setValues(int v0, int v1, int v2, int v3) {
        this.vals[0] = v0;
        this.vals[1] = v1;
        this.vals[2] = v2;
        this.vals[3] = v3;
        // Replicate QuadrupleCompositeKey hash exactly:
        // Integer.hashCode(x) == x, so the polynomial reduces to:
        int h = 31 + v0;
        h = 31 * h + v1;
        h = 31 * h + v2;
        this.hash = 31 * h + v3;
    }

    /** Update 3 values and recompute hash. For 3-INT PK tables (if any). */
    public void setValues(int v0, int v1, int v2) {
        this.vals[0] = v0;
        this.vals[1] = v1;
        this.vals[2] = v2;
        // Replicate TripleCompositeKey hash exactly:
        int h = 31 + v0;
        h = 31 * h + v1;
        this.hash = 31 * h + v2;
    }

    /** Update 2 values and recompute hash. For 2-INT PK tables. */
    public void setValues(int v0, int v1) {
        this.vals[0] = v0;
        this.vals[1] = v1;
        // Replicate PairCompositeKey hash exactly:
        int h = 31 + v0;
        this.hash = 31 * h + v1;
    }

    // ── IKey contract ─────────────────────────────────────────────────────────

    @Override
    public int hashCode() {
        return this.hash;
    }

    @Override
    public int size() {
        return this.vals.length;
    }

    /**
     * ConcurrentHashMap calls probeKey.equals(storedKey).
     * storedKey is a QuadrupleCompositeKey / TripleCompositeKey / PairCompositeKey.
     * We compare via getValue(i) which was added to BaseComposite (AOO-6 change).
     *
     * Fast path: hash mismatch → false immediately (no field access needed).
     * Normal path: size check + per-field comparison via getValue(i).
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseComposite bc)) return false;
        if (!(o instanceof IKey otherKey)) return false;
        if (this.hash != bc.hashCode()) return false;
        if (this.vals.length != otherKey.size()) return false;
        for (int i = 0; i < this.vals.length; i++) {
            Object val = bc.getValue(i);
            if (val == null) return false;
            if (((Number) val).intValue() != this.vals[i]) return false;
        }
        return true;
    }
}