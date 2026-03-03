package dk.ku.di.dms.vms.modb.common.schema.network.query;

import java.nio.ByteBuffer;

/**
 * Binary payload embedded in QueryRequestEvent.routingData for MODE_RECEIVE_AND_JOIN.
 *
 * Wire layout:
 *   [4] numCols
 *   numCols * [4 remoteOffset + 1 remoteTypeCode + 4 localColIndex]
 *   [4] remoteRecordSize  (total bytes per remote row, for sanity checks)
 */
public final class JoinRoutingData {

    public static final byte TYPE_INT    = 0;
    public static final byte TYPE_LONG   = 1;
    public static final byte TYPE_DOUBLE = 2;

    public final int[] remoteColOffsets;   // byte offset within remote row for each join col
    public final byte[] remoteColTypes;    // TYPE_INT / TYPE_LONG / TYPE_DOUBLE
    public final int[] localColIndices;    // index in local OLTP Schema for each join col
    public final int   remoteRecordSize;   // bytes per remote row (without header)

    public JoinRoutingData(int[] remoteColOffsets, byte[] remoteColTypes,
                           int[] localColIndices, int remoteRecordSize) {
        this.remoteColOffsets  = remoteColOffsets;
        this.remoteColTypes    = remoteColTypes;
        this.localColIndices   = localColIndices;
        this.remoteRecordSize  = remoteRecordSize;
    }

    public byte[] toBytes() {
        int n = remoteColOffsets.length;
        ByteBuffer buf = ByteBuffer.allocate(4 + n * 9 + 4);
        buf.putInt(n);
        for (int i = 0; i < n; i++) {
            buf.putInt(remoteColOffsets[i]);
            buf.put(remoteColTypes[i]);
            buf.putInt(localColIndices[i]);
        }
        buf.putInt(remoteRecordSize);
        return buf.array();
    }

    public static JoinRoutingData fromBytes(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int n = buf.getInt();
        int[]  offsets    = new int[n];
        byte[] types      = new byte[n];
        int[]  localIdxs  = new int[n];
        for (int i = 0; i < n; i++) {
            offsets[i]   = buf.getInt();
            types[i]     = buf.get();
            localIdxs[i] = buf.getInt();
        }
        int remoteSize = buf.getInt();
        return new JoinRoutingData(offsets, types, localIdxs, remoteSize);
    }
}