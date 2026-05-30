package dk.ku.di.dms.vms.modb.common.memory;

import dk.ku.di.dms.vms.modb.common.schema.network.query.JoinRoutingData;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;

/**
 * Reads typed column values from a row by byte offset, whether the row is a heap byte[]
 * (broadcast side, offsets 0-based) or an off-heap slot (local side, offsets include the
 * 5-byte record header). One thread-local instance per thread via threadLocal(); not
 * thread-safe. extractKey() builds a '-'-delimited composite join key from the given columns.
 */
public final class GeneralRowView {

    private static final long BYTE_ARRAY_BASE = UNSAFE.arrayBaseOffset(byte[].class);

    private static final ThreadLocal<GeneralRowView> THREAD_LOCAL =
            ThreadLocal.withInitial(GeneralRowView::new);

    public static GeneralRowView threadLocal() {
        return THREAD_LOCAL.get();
    }
    private Object base;
    private long baseOffset;

    public GeneralRowView() {}

        public void wrap(byte[] data) {
        this.base       = data;
        this.baseOffset = BYTE_ARRAY_BASE;
    }

    public int getInt(int colByteOffset) {
        return UNSAFE.getInt(base, baseOffset + colByteOffset);
    }

    public long getLong(int colByteOffset) {
        return UNSAFE.getLong(base, baseOffset + colByteOffset);
    }

    public float getFloat(int colByteOffset) {
        return UNSAFE.getFloat(base, baseOffset + colByteOffset);
    }

    public double getDouble(int colByteOffset) {
        return UNSAFE.getDouble(base, baseOffset + colByteOffset);
    }


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