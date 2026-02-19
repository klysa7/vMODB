package dk.ku.di.dms.vms.modb.common.schema.network.query;

import java.nio.ByteBuffer;

public final class QueryResultEvent {
    public static final byte QUERY_RESULT_TYPE = 100;
    public static final byte END_OF_STREAM_TYPE = 101;
    public static final int HEADER_SIZE = 1 + Integer.BYTES + Long.BYTES;

    public static void initBatch(ByteBuffer buffer, long queryId) {
        buffer.put(QUERY_RESULT_TYPE);
        buffer.putInt(0);
        buffer.putLong(queryId);
    }

    public static void finalizeBatch(ByteBuffer buffer) {
        int endPos = buffer.position();
        int dataSize = endPos - (1 + Integer.BYTES);

        buffer.putInt(1, dataSize);
        buffer.position(endPos);
    }

    public static void writeEndOfStream(ByteBuffer buffer, long queryId) {
        buffer.put(END_OF_STREAM_TYPE);
        buffer.putInt(Long.BYTES);
        buffer.putLong(queryId);
    }
}