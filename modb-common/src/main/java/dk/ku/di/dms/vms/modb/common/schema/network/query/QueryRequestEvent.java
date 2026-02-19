package dk.ku.di.dms.vms.modb.common.schema.network.query;
import dk.ku.di.dms.vms.modb.common.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class QueryRequestEvent {

    public static final byte QUERY_REQUEST_TYPE = 99;

    public static void write(ByteBuffer buffer, QueryPayloadRaw payload) {
        int startPos = buffer.position();
        buffer.put(QUERY_REQUEST_TYPE);
        buffer.putInt(0);

        buffer.putLong(payload.queryId);
        buffer.putLong(payload.snapshotId);
        buffer.putInt(payload.tableName.length);
        buffer.put(payload.tableName);

        if (payload.predicates != null) {
            buffer.putInt(payload.predicates.length);
            buffer.put(payload.predicates);
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
        int tableNameSize = buffer.getInt();
        String tableName = ByteUtils.extractStringFromByteBuffer(buffer, tableNameSize);

        int predicateSize = buffer.getInt();
        byte[] predicates = new byte[predicateSize];
        if (predicateSize > 0) {
            buffer.get(predicates);
        }

        return new QueryPayload(queryId, snapshotId, tableName, predicates);
    }

    public record QueryPayloadRaw(long queryId, long snapshotId, byte[] tableName, byte[] predicates) {
        public static QueryPayloadRaw of(long qId, long sId, String table, byte[] preds) {
            return new QueryPayloadRaw(qId, sId, table.getBytes(StandardCharsets.UTF_8), preds);
        }
    }

    public record QueryPayload(long queryId, long snapshotId, String tableName, byte[] predicates) {}
}