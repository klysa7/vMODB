package dk.ku.di.dms.vms.modb.common.schema.network.query;

import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public final class QueryRequestEvent {

    public static final byte QUERY_REQUEST_TYPE   = 99;
    public static final byte MODE_SCAN_TO_GATEWAY = 0;
    public static final byte MODE_BROADCAST_TO_VMS = 1;
    public static final byte MODE_RECEIVE_AND_JOIN = 2;
    public static final byte MODE_LOCAL_JOIN = 3;


    // Wire format:
    // [99][length:4][queryId:8][snapshotId:8][mode:1]
    // [tableNameLen:4][tableName]
    // [predicatesLen:4][predicates]
    // [routingLen:4][routing]
    // [projectionLen:4][projection]   ← QPO-3: int[] of column indices, 4 bytes each, native order

    public static void write(ByteBuffer buffer, QueryPayloadRaw payload) {
        int startPos = buffer.position();
        buffer.put(QUERY_REQUEST_TYPE);
        buffer.putInt(0);

        buffer.putLong(payload.queryId);
        buffer.putLong(payload.snapshotId);
        buffer.put(payload.mode);

        buffer.putInt(payload.tableName.length);
        buffer.put(payload.tableName);

        if (payload.predicates != null && payload.predicates.length > 0) {
            buffer.putInt(payload.predicates.length);
            buffer.put(payload.predicates);
        } else {
            buffer.putInt(0);
        }

        if (payload.routingData != null && payload.routingData.length > 0) {
            buffer.putInt(payload.routingData.length);
            buffer.put(payload.routingData);
        } else {
            buffer.putInt(0);
        }

        // QPO-3: projection field
        if (payload.projectionData != null && payload.projectionData.length > 0) {
            buffer.putInt(payload.projectionData.length);
            buffer.put(payload.projectionData);
        } else {
            buffer.putInt(0);
        }

        int endPos = buffer.position();
        buffer.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
        buffer.position(endPos);
    }

    public static QueryPayload read(ByteBuffer buffer) {
        long queryId    = buffer.getLong();
        long snapshotId = buffer.getLong();
        byte mode       = buffer.get();

        int    tableNameSize = buffer.getInt();
        String tableName     = ByteUtils.extractStringFromByteBuffer(buffer, tableNameSize);

        int    predicateSize = buffer.getInt();
        byte[] predicates    = new byte[predicateSize];
        if (predicateSize > 0) buffer.get(predicates);

        int    routingSize = buffer.getInt();
        byte[] routingData = new byte[routingSize];
        if (routingSize > 0) buffer.get(routingData);

        // QPO-3: read projection field
        // Guard: older clients may not send this field — check remaining bytes
        byte[] projectionData = new byte[0];
        if (buffer.hasRemaining() && buffer.remaining() >= Integer.BYTES) {
            int projectionSize = buffer.getInt();
            if (projectionSize > 0 && buffer.remaining() >= projectionSize) {
                projectionData = new byte[projectionSize];
                buffer.get(projectionData);
            }
        }

        return new QueryPayload(queryId, snapshotId, mode, tableName,
                predicates, routingData, projectionData);
    }

    // ── QPO-3 helpers ─────────────────────────────────────────────────────────

    /**
     * Serialize int[] column indices to byte[].
     * Uses native byte order to match serializeRow() on the VMS side.
     */
    public static byte[] serializeProjection(int[] colIndices) {
        if (colIndices == null || colIndices.length == 0) return new byte[0];
        ByteBuffer buf = ByteBuffer.allocate(colIndices.length * Integer.BYTES)
                .order(ByteOrder.nativeOrder());
        for (int idx : colIndices) buf.putInt(idx);
        return buf.array();
    }

    /**
     * Deserialize byte[] back to int[] column indices.
     * Returns null if data is null or empty (means full scan — no projection).
     */
    public static int[] deserializeProjection(byte[] data) {
        if (data == null || data.length == 0) return null;
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());
        int[] result = new int[data.length / Integer.BYTES];
        for (int i = 0; i < result.length; i++) result[i] = buf.getInt();
        return result;
    }

    // ── Records ───────────────────────────────────────────────────────────────

    public record QueryPayloadRaw(
            long   queryId,
            long   snapshotId,
            byte   mode,
            byte[] tableName,
            byte[] predicates,
            byte[] routingData,
            byte[] projectionData) {   // QPO-3: new field

        public static QueryPayloadRaw of(long qId, long sId, byte mode,
                                         String table, byte[] preds,
                                         byte[] routing, byte[] projection) {
            return new QueryPayloadRaw(qId, sId, mode,
                    table.getBytes(StandardCharsets.UTF_8),
                    preds, routing, projection);
        }

        /** Backward-compatible factory — no projection */
        public static QueryPayloadRaw of(long qId, long sId, byte mode,
                                         String table, byte[] preds, byte[] routing) {
            return of(qId, sId, mode, table, preds, routing, null);
        }
    }

    public record QueryPayload(
            long   queryId,
            long   snapshotId,
            byte   mode,
            String tableName,
            byte[] predicates,
            byte[] routingData,
            byte[] projectionData) {}  // QPO-3: new field
}