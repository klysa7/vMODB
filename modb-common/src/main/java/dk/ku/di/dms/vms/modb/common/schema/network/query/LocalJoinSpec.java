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
 * QPO-7 extension: optional semi-join filter for cross-VMS joins
 * where the cross-VMS dependency reduces to a key-set membership check.
 * CHQ3 uses this to filter `orders` by a pre-fetched customer key set.
 *
 * Wire layout (existing fields):
 * [buildTableNameLen:4][buildTableName]
 * [nBuildJoinCols:4][buildJoinCols: int[] 4 bytes each]
 * [nProbeJoinCols:4][probeJoinCols: int[] 4 bytes each]
 * [groupByCol:4]
 * [buildPredicatesJsonLen:4][buildPredicatesJson bytes]
 *
 * Wire layout (QPO-7 optional extension, appended at the end):
 * [nSemiJoinCols:4][semiJoinBuildCols: int[] 4 bytes each]
 * [semiJoinKeysDataLen:4][semiJoinKeysData bytes]
 *
 * Backward compatibility: fromBytes() checks hasRemaining() before
 * reading the QPO-7 fields. CHQ4 wire frames don't carry them and
 * parsing degrades gracefully to "no semi-join".
 */
public final class LocalJoinSpec {

    public final String buildTableName;
    public final int[]  buildJoinCols;
    public final int[]  probeJoinCols;
    public final int    groupByCol;
    public final String buildPredicatesJson; // may be empty

    // QPO-7 — optional, empty arrays if not used
    public final int[]  semiJoinBuildCols;   // columns of build table to filter on
    public final byte[] semiJoinKeysData;    // serialized key set: [nKeys:4][nCols:4][col0:4][col1:4]...

    /** Original CHQ4 constructor — no semi-join. */
    public LocalJoinSpec(String buildTableName, int[] buildJoinCols,
                         int[] probeJoinCols, int groupByCol,
                         String buildPredicatesJson) {
        this(buildTableName, buildJoinCols, probeJoinCols, groupByCol,
                buildPredicatesJson, new int[0], new byte[0]);
    }

    /** QPO-7 constructor — with optional semi-join filter. */
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

        // QPO-7 fields (always written; lengths are 0 if unused)
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

        // QPO-7 fields — backward compatible. Old CHQ4 wire frames stop here.
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