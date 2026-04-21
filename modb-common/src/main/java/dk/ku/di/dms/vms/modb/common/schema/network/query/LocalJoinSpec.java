package dk.ku.di.dms.vms.modb.common.schema.network.query;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * QPO-5: Wire format for MODE_LOCAL_JOIN requests.
 * Encodes the build table name, join column indices on both sides,
 * the group-by column index, and an optional JSON predicate string
 * for filtering the build table before hashing.
 *
 * Wire layout:
 * [buildTableNameLen:4][buildTableName]
 * [nBuildJoinCols:4][buildJoinCols: int[] 4 bytes each]
 * [nProbeJoinCols:4][probeJoinCols: int[] 4 bytes each]
 * [groupByCol:4]
 * [buildPredicatesJsonLen:4][buildPredicatesJson bytes]
 */
public final class LocalJoinSpec {

    public final String buildTableName;
    public final int[]  buildJoinCols;
    public final int[]  probeJoinCols;
    public final int    groupByCol;
    public final String buildPredicatesJson; // may be empty

    public LocalJoinSpec(String buildTableName, int[] buildJoinCols,
                         int[] probeJoinCols, int groupByCol,
                         String buildPredicatesJson) {
        this.buildTableName      = buildTableName;
        this.buildJoinCols       = buildJoinCols;
        this.probeJoinCols       = probeJoinCols;
        this.groupByCol          = groupByCol;
        this.buildPredicatesJson = buildPredicatesJson == null ? "" : buildPredicatesJson;
    }

    public byte[] toBytes() {
        byte[] tableBytes = buildTableName.getBytes(StandardCharsets.UTF_8);
        byte[] predBytes  = buildPredicatesJson.getBytes(StandardCharsets.UTF_8);

        int size = 4 + tableBytes.length
                + 4 + buildJoinCols.length * 4
                + 4 + probeJoinCols.length * 4
                + 4
                + 4 + predBytes.length;

        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.nativeOrder());

        buf.putInt(tableBytes.length);
        buf.put(tableBytes);

        buf.putInt(buildJoinCols.length);
        for (int c : buildJoinCols) buf.putInt(c);

        buf.putInt(probeJoinCols.length);
        for (int c : probeJoinCols) buf.putInt(c);

        buf.putInt(groupByCol);

        buf.putInt(predBytes.length);
        if (predBytes.length > 0) buf.put(predBytes);

        return buf.array();
    }

    public static LocalJoinSpec fromBytes(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.nativeOrder());

        int tableLen = buf.getInt();
        byte[] tableBytes = new byte[tableLen];
        buf.get(tableBytes);
        String buildTableName = new String(tableBytes, StandardCharsets.UTF_8);

        int nBuild = buf.getInt();
        int[] buildJoinCols = new int[nBuild];
        for (int i = 0; i < nBuild; i++) buildJoinCols[i] = buf.getInt();

        int nProbe = buf.getInt();
        int[] probeJoinCols = new int[nProbe];
        for (int i = 0; i < nProbe; i++) probeJoinCols[i] = buf.getInt();

        int groupByCol = buf.getInt();

        int predLen = buf.getInt();
        String buildPredicatesJson = "";
        if (predLen > 0) {
            byte[] predBytes = new byte[predLen];
            buf.get(predBytes);
            buildPredicatesJson = new String(predBytes, StandardCharsets.UTF_8);
        }

        return new LocalJoinSpec(buildTableName, buildJoinCols,
                probeJoinCols, groupByCol, buildPredicatesJson);
    }
}