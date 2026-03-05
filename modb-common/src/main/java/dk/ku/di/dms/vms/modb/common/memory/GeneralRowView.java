package dk.ku.di.dms.vms.modb.common.memory;

import dk.ku.di.dms.vms.modb.common.schema.network.query.JoinRoutingData;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;

/**
 * Unified view over a single row regardless of whether it lives on the Java heap
 * (a broadcast-side {@code byte[]}) or in off-heap memory (a local VMS slot).
 *
 * <h3>Memory coordinate systems</h3>
 * <p>There are two coordinate systems in the codebase and {@code GeneralRowView}
 * handles both through the same arithmetic: every read is
 * {@code UNSAFE.get*(base, baseOffset + colOffset)}.
 *
 * <ul>
 *   <li><b>Heap byte[] (broadcast / build side)</b> — the array begins at the
 *       first data byte; there is no header.  {@code baseOffset} is set to
 *       {@code UNSAFE.arrayBaseOffset(byte[].class)} (= {@code BYTE_ARRAY_BASE}).
 *       Column offsets passed to {@link #extractKey} must be <em>data-relative</em>
 *       (0-based).  These come from {@code JoinRoutingData.remoteColOffsets}.</li>
 *
 *   <li><b>Off-heap slot (local / probe side)</b> — the slot begins with a
 *       5-byte header (1 active-flag byte + 4 hashed-PK bytes).  {@code baseOffset}
 *       is set to the raw slot address (= {@code iter.address()}).  Column offsets
 *       must be <em>slot-relative</em> (include {@code Schema.RECORD_HEADER = 5}).
 *       These come from {@code schema.columnOffset()} via
 *       {@code TransactionManager.resolveColumnOffsets()}.</li>
 * </ul>
 *
 * <h3>The headerOffset bug — and why it is not here</h3>
 * <p>A natural but incorrect implementation of {@link #wrap(byte[])} would be:
 * <pre>{@code
 *     this.baseOffset = BYTE_ARRAY_BASE + RECORD_HEADER;  // WRONG
 * }</pre>
 * That would skip 5 bytes inside the array, misaligning every column read by
 * exactly {@code RECORD_HEADER} bytes.  Broadcast rows carry <em>no</em> header,
 * so the correct form is simply {@code baseOffset = BYTE_ARRAY_BASE} — no addition.
 *
 * <h3>Thread safety</h3>
 * <p>{@code GeneralRowView} is <b>not thread-safe</b>.  Each thread should
 * obtain its own instance via the {@link #threadLocal()} factory, or allocate
 * a local instance per-call.
 */
public final class GeneralRowView {

    // -----------------------------------------------------------------------
    // Static constants
    // -----------------------------------------------------------------------

    /** Base offset of the first element of any {@code byte[]}. */
    private static final long BYTE_ARRAY_BASE = UNSAFE.arrayBaseOffset(byte[].class);

    // -----------------------------------------------------------------------
    // Per-thread singleton factory
    // -----------------------------------------------------------------------

    private static final ThreadLocal<GeneralRowView> THREAD_LOCAL =
            ThreadLocal.withInitial(GeneralRowView::new);

    /** Returns the thread-local {@code GeneralRowView} instance (reusable). */
    public static GeneralRowView threadLocal() {
        return THREAD_LOCAL.get();
    }

    // -----------------------------------------------------------------------
    // Instance state
    // -----------------------------------------------------------------------

    /**
     * Backing Java object.
     * <ul>
     *   <li>For heap mode: the {@code byte[]} instance.</li>
     *   <li>For off-heap mode: {@code null} (UNSAFE treats null as raw address).</li>
     * </ul>
     */
    private Object base;

    /**
     * Absolute base address used in all UNSAFE reads.
     * <ul>
     *   <li>Heap mode: {@code BYTE_ARRAY_BASE} — no header addition.</li>
     *   <li>Off-heap mode: {@code iter.address()} — the raw slot address.</li>
     * </ul>
     */
    private long baseOffset;

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /** Creates an uninitialised view; call {@link #wrap(byte[])} or
     *  {@link #wrap(long)} before use. */
    public GeneralRowView() {}

    // -----------------------------------------------------------------------
    // wrap() — two coordinate systems, same arithmetic
    // -----------------------------------------------------------------------

    /**
     * Points this view at a heap {@code byte[]} whose column data begins at
     * index 0 (no header).
     *
     * <p>Column offsets supplied to {@link #extractKey} (and the typed getters)
     * must be <em>data-relative</em> (0-based).
     *
     * <p><b>Correct</b>: {@code baseOffset = BYTE_ARRAY_BASE}.
     * Adding any header offset here would be a bug for broadcast rows.
     *
     * @param data the raw broadcast row payload
     */
    public void wrap(byte[] data) {
        this.base       = data;
        this.baseOffset = BYTE_ARRAY_BASE;  // ← no RECORD_HEADER addition: broadcast rows have no header
    }

    /**
     * Points this view at an off-heap slot starting at {@code slotAddress}.
     *
     * <p>Column offsets supplied to {@link #extractKey} (and the typed getters)
     * must be <em>slot-relative</em> (i.e. they already include
     * {@code Schema.RECORD_HEADER = 5}).  These come from
     * {@code schema.columnOffset()} without further adjustment.
     *
     * @param slotAddress the raw address returned by {@code IRecordIterator.address()}
     */
    public void wrap(long slotAddress) {
        this.base       = null;       // null tells UNSAFE to interpret baseOffset as raw address
        this.baseOffset = slotAddress;
    }

    // -----------------------------------------------------------------------
    // Typed column getters
    // -----------------------------------------------------------------------

    /** Reads a 4-byte int at {@code base + baseOffset + colByteOffset}. */
    public int getInt(int colByteOffset) {
        return UNSAFE.getInt(base, baseOffset + colByteOffset);
    }

    /** Reads an 8-byte long at {@code base + baseOffset + colByteOffset}. */
    public long getLong(int colByteOffset) {
        return UNSAFE.getLong(base, baseOffset + colByteOffset);
    }

    /** Reads a 4-byte float at {@code base + baseOffset + colByteOffset}. */
    public float getFloat(int colByteOffset) {
        return UNSAFE.getFloat(base, baseOffset + colByteOffset);
    }

    /** Reads an 8-byte double at {@code base + baseOffset + colByteOffset}. */
    public double getDouble(int colByteOffset) {
        return UNSAFE.getDouble(base, baseOffset + colByteOffset);
    }

    /** Reads a single byte at {@code base + baseOffset + colByteOffset}. */
    public byte getByte(int colByteOffset) {
        return UNSAFE.getByte(base, baseOffset + colByteOffset);
    }

    // -----------------------------------------------------------------------
    // Key extraction — the single source of type-dispatch truth
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Fast numeric key — avoids String allocation entirely
    // -----------------------------------------------------------------------

    /**
     * Returns {@code true} when all join columns can be packed into a single
     * {@code long} without loss, i.e.:
     * <ul>
     *   <li>exactly 1 column of type INT or LONG, <strong>or</strong></li>
     *   <li>exactly 2 columns, both INT (packed as two 32-bit halves).</li>
     * </ul>
     * For 3+ columns, LONG+anything, or floating-point types the method returns
     * {@code false} and callers must fall back to {@link #extractKey}.
     */
    public static boolean canUseLongKey(byte[] colTypes) {
        if (colTypes.length == 1) {
            return colTypes[0] == JoinRoutingData.TYPE_INT
                    || colTypes[0] == JoinRoutingData.TYPE_LONG;
        }
        if (colTypes.length == 2) {
            return colTypes[0] == JoinRoutingData.TYPE_INT
                    && colTypes[1] == JoinRoutingData.TYPE_INT;
        }
        return false;
    }

    /**
     * Extracts a composite {@code long} join key. Must only be called when
     * {@link #canUseLongKey} returned {@code true} for the same {@code colTypes}.
     *
     * <p>Packing rules:
     * <ul>
     *   <li>1 INT column  → {@code (long) getInt(off0)} (sign-extended)</li>
     *   <li>1 LONG column → {@code getLong(off0)}</li>
     *   <li>2 INT columns → high 32 bits = col0, low 32 bits = col1</li>
     * </ul>
     *
     * <p>The same packing is applied on both the build side (heap {@code byte[]})
     * and the probe side (off-heap slot), so keys are always comparable.
     */
    public long extractLongKey(int[] colOffsets, byte[] colTypes) {
        if (colTypes.length == 1) {
            return colTypes[0] == JoinRoutingData.TYPE_LONG
                    ? getLong(colOffsets[0])
                    : (long) getInt(colOffsets[0]);
        }
        // 2 INT columns — pack into one long
        return ((long) getInt(colOffsets[0]) << 32)
                | (getInt(colOffsets[1]) & 0xFFFFFFFFL);
    }

    /**
     * Builds a composite {@code '-'}-delimited String key from the columns
     * identified by {@code colOffsets} and {@code colTypes}.
     *
     * <p>This is the single canonical implementation that replaces the
     * duplicated logic previously found in:
     * <ul>
     *   <li>{@code VmsEventHandler.extractBuildKey(byte[], int[], byte[])} — heap side</li>
     *   <li>{@code TransactionManager.extractProbeKey(long, int[], byte[])}  — off-heap side</li>
     * </ul>
     *
     * @param colOffsets byte offsets of each join column (coordinate system determined
     *                   by which {@link #wrap} overload was called)
     * @param colTypes   {@link JoinRoutingData} type codes per column
     * @return composite key string, e.g. {@code "42-7"}
     */
    public String extractKey(int[] colOffsets, byte[] colTypes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < colOffsets.length; i++) {
            switch (colTypes[i]) {
                case JoinRoutingData.TYPE_INT    -> sb.append(getInt(colOffsets[i]));
                case JoinRoutingData.TYPE_LONG   -> sb.append(getLong(colOffsets[i]));
                case JoinRoutingData.TYPE_DOUBLE -> sb.append(
                        Double.doubleToRawLongBits(getDouble(colOffsets[i])));
                case JoinRoutingData.TYPE_FLOAT  -> sb.append(
                        Float.floatToRawIntBits(getFloat(colOffsets[i])));
                default                          -> sb.append(getInt(colOffsets[i]));
            }
            if (i < colOffsets.length - 1) sb.append('-');
        }
        return sb.toString();
    }
}