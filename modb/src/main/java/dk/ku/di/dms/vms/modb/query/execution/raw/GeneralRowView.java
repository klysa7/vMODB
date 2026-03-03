package dk.ku.di.dms.vms.modb.query.execution.raw;

import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;

public final class GeneralRowView {

    private final Schema schema;
    private Object baseObject;
    private long baseOffset;
    private int headerOffset;

    public GeneralRowView(Schema schema) {
        this.schema = schema;
    }

    public void setPointer(long address) {
        this.baseObject = null;
        this.baseOffset = address;
        this.headerOffset = Schema.RECORD_HEADER;
    }

    public void setByteArray(byte[] data) {
        this.baseObject = data;
        this.baseOffset = UNSAFE.arrayBaseOffset(byte[].class);
        this.headerOffset = 0;  // data starts at byte 0, no header present
    }

    public int getInt(int colIndex) {
        long finalOffset = this.baseOffset + this.headerOffset + schema.columnOffset()[colIndex];
        return UNSAFE.getInt(this.baseObject, finalOffset);
    }

    public long getLong(int colIndex) {
        long finalOffset = this.baseOffset + this.headerOffset + schema.columnOffset()[colIndex];
        return UNSAFE.getLong(this.baseObject, finalOffset);
    }

    public double getDouble(int colIndex) {
        long finalOffset = this.baseOffset + this.headerOffset + schema.columnOffset()[colIndex];
        return UNSAFE.getDouble(this.baseObject, finalOffset);
    }

    public int getHash(int colIndex) {
        DataType type = this.schema.columnDataType(colIndex);
        switch (type) {
            case INT: return Integer.hashCode(getInt(colIndex));
            case LONG: return Long.hashCode(getLong(colIndex));
            case DOUBLE: return Double.hashCode(getDouble(colIndex));
            default:
                throw new UnsupportedOperationException("Hash not implemented for type: " + type);
        }
    }

    public int getCompositeHash(int[] colIndices) {
        int hash = 1;
        for (int colIndex : colIndices) {
            hash = 31 * hash + getHash(colIndex);
        }
        return hash;
    }

    // THE FIX: Collision-free string key builder
    public String getCompositeKey(int[] colIndices) {
        StringBuilder sb = new StringBuilder();
        for (int colIndex : colIndices) {
            DataType type = this.schema.columnDataType(colIndex);
            switch (type) {
                case INT: sb.append(getInt(colIndex)).append("-"); break;
                case LONG: sb.append(getLong(colIndex)).append("-"); break;
                case DOUBLE: sb.append(getDouble(colIndex)).append("-"); break;
                default: sb.append("UNK-"); break;
            }
        }
        return sb.toString();
    }

    public int getRecordSize() {
        return this.schema.getRecordSizeWithoutHeader();
    }
}