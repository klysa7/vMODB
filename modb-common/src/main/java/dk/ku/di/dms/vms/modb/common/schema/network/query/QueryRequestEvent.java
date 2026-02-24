package dk.ku.di.dms.vms.modb.common.schema.network.query;

import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class QueryRequestEvent {

    public static final byte QUERY_REQUEST_TYPE = 99;
    public static final byte MODE_SCAN_TO_GATEWAY = 0;
    public static final byte MODE_BROADCAST_TO_VMS = 1;
    public static final byte MODE_RECEIVE_AND_JOIN = 2;

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

        int endPos = buffer.position();
        buffer.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
        buffer.position(endPos);
    }

    public static QueryPayload read(ByteBuffer buffer) {
        long queryId = buffer.getLong();
        long snapshotId = buffer.getLong();
        byte mode = buffer.get();

        int tableNameSize = buffer.getInt();
        String tableName = ByteUtils.extractStringFromByteBuffer(buffer, tableNameSize);

        int predicateSize = buffer.getInt();
        byte[] predicates = new byte[predicateSize];
        if (predicateSize > 0) buffer.get(predicates);

        int routingSize = buffer.getInt();
        byte[] routingData = new byte[routingSize];
        if (routingSize > 0) buffer.get(routingData);

        return new QueryPayload(queryId, snapshotId, mode, tableName, predicates, routingData);
    }

    public record QueryPayloadRaw(long queryId, long snapshotId, byte mode, byte[] tableName, byte[] predicates, byte[] routingData) {
        public static QueryPayloadRaw of(long qId, long sId, byte mode, String table, byte[] preds, byte[] routing) {
            return new QueryPayloadRaw(qId, sId, mode, table.getBytes(StandardCharsets.UTF_8), preds, routing);
        }
    }

    public record QueryPayload(long queryId, long snapshotId, byte mode, String tableName, byte[] predicates, byte[] routingData) {}
}