package dk.ku.di.dms.vms.modb.common.schema.network.query;

import java.nio.ByteBuffer;

/**
 * Wire-safe descriptor for a distributed hash join.
 * Encodes byte offsets + types for the remote (broadcast) row,
 * and column indices in the local schema.
 *
 * Binary layout: [4: numCols]
 *                [4*n: remoteColOffsets]
 *                [n:   remoteColTypes]
 *                [4*n: localColIndices]
 *                [4:   remoteRecordSize]
 */
public final class JoinRoutingData {

    public static final byte TYPE_INT    = 0;
    public static final byte TYPE_LONG   = 1;
    public static final byte TYPE_DOUBLE = 2;
    public static final byte TYPE_FLOAT  = 3;

    /** Byte offset of each join column from start of the remote payload (no header). */
    public final int[]  remoteColOffsets;
    /** DataType code (TYPE_INT etc.) for each remote join column. */
    public final byte[] remoteColTypes;
    /** Column index in the local Schema for each join column. */
    public final int[]  localColIndices;
    /** Total bytes of one remote row payload (no header). */
    public final int    remoteRecordSize;

    public JoinRoutingData(int[] remoteColOffsets, byte[] remoteColTypes,
                           int[] localColIndices, int remoteRecordSize) {
        this.remoteColOffsets = remoteColOffsets;
        this.remoteColTypes   = remoteColTypes;
        this.localColIndices  = localColIndices;
        this.remoteRecordSize = remoteRecordSize;
    }

    public byte[] toBytes() {
        int n = remoteColOffsets.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 * n + n + 4 * n + 4);
        buf.putInt(n);
        for (int off  : remoteColOffsets) buf.putInt(off);
        for (byte t   : remoteColTypes)   buf.put(t);
        for (int idx  : localColIndices)  buf.putInt(idx);
        buf.putInt(remoteRecordSize);
        return buf.array();
    }

    public static JoinRoutingData fromBytes(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int n = buf.getInt();
        int[]  remoteOffsets = new int[n];
        byte[] remoteTypes   = new byte[n];
        int[]  localIndices  = new int[n];
        for (int i = 0; i < n; i++) remoteOffsets[i] = buf.getInt();
        for (int i = 0; i < n; i++) remoteTypes[i]   = buf.get();
        for (int i = 0; i < n; i++) localIndices[i]  = buf.getInt();
        int remoteRecordSize = buf.getInt();
        return new JoinRoutingData(remoteOffsets, remoteTypes, localIndices, remoteRecordSize);
    }
}