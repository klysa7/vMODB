package dk.ku.di.dms.vms.modb.common.schema.network.query;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Wire format for local-join query requests (MODE_LOCAL_JOIN / CHQ3). Carries the build table
 * name, join columns on both sides, the group-by column, and an optional build-side predicate
 * JSON. CHQ3 also appends an optional semi-join filter (columns + a pre-fetched key set);
 * fromBytes() reads those only if present, so older CHQ4 frames still parse.
 */
public final class LocalJoinSpec {

    public final String buildTableName;
    public final int[]  buildJoinCols;
    public final int[]  probeJoinCols;
    public final int    groupByCol;
    public final String buildPredicatesJson;
    public final int[]  semiJoinBuildCols;
    public final byte[] semiJoinKeysData;

    public LocalJoinSpec(String buildTableName, int[] buildJoinCols,
                         int[] probeJoinCols, int groupByCol,
                         String buildPredicatesJson) {
        this(buildTableName, buildJoinCols, probeJoinCols, groupByCol,
                buildPredicatesJson, new int[0], new byte[0]);
    }

    public LocalJoinSpec(String buildTableName, int[] buildJoinCols,
                         int[] probeJoinCols, int groupByCol,
                         String buildPredicatesJson,
                         int[] semiJoinBuildCols, byte[] semiJoinKeysData) {
        this.buildTableName      = buildTableName;
        this.buildJoinCols       = buildJoinCols;
        this.probeJoinCols       = probeJoinCols;
        this.groupByCol          = groupByCol;
        this.buildPredicatesJson = buildPredicatesJson == null ? "" : buildPredicatesJson;
        this.semiJoinBuildCols   = semiJoinBuildCols == null ? new int[0] : semiJoinBuildCols;
        this.semiJoinKeysData    = semiJoinKeysData == null ? new byte[0] : semiJoinKeysData;
    }

    public byte[] toBytes() {
        byte[] tableBytes = buildTableName.getBytes(StandardCharsets.UTF_8);
        byte[] predBytes  = buildPredicatesJson.getBytes(StandardCharsets.UTF_8);

        int size = 4 + tableBytes.length
                + 4 + buildJoinCols.length * 4
                + 4 + probeJoinCols.length * 4
                + 4
                + 4 + predBytes.length
                + 4 + semiJoinBuildCols.length * 4
                + 4 + semiJoinKeysData.length;

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


        buf.putInt(semiJoinBuildCols.length);
        for (int c : semiJoinBuildCols) buf.putInt(c);

        buf.putInt(semiJoinKeysData.length);
        if (semiJoinKeysData.length > 0) buf.put(semiJoinKeysData);

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

        int[] semiJoinBuildCols  = new int[0];
        byte[] semiJoinKeysData  = new byte[0];

        if (buf.hasRemaining() && buf.remaining() >= Integer.BYTES) {
            int nSemi = buf.getInt();
            if (nSemi > 0 && buf.remaining() >= nSemi * Integer.BYTES) {
                semiJoinBuildCols = new int[nSemi];
                for (int i = 0; i < nSemi; i++) semiJoinBuildCols[i] = buf.getInt();
            }
            if (buf.hasRemaining() && buf.remaining() >= Integer.BYTES) {
                int keysLen = buf.getInt();
                if (keysLen > 0 && buf.remaining() >= keysLen) {
                    semiJoinKeysData = new byte[keysLen];
                    buf.get(semiJoinKeysData);
                }
            }
        }

        return new LocalJoinSpec(buildTableName, buildJoinCols,
                probeJoinCols, groupByCol, buildPredicatesJson,
                semiJoinBuildCols, semiJoinKeysData);
    }
}