package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public final class VModbCount extends SingleRel implements VModbRel {

    private final RelDataType relDataType;
    public final boolean distinct;
    public final int distinctColumnIndex;
    public final int[] groupByColumns;

    private VModbCount(RelOptCluster cluster,
                       RelTraitSet traitSet,
                       RelNode input,
                       RelDataType relDataType,
                       boolean distinct,
                       int distinctColumnIndex,
                       int[] groupByColumns) {
        super(cluster, traitSet, input);
        this.relDataType = relDataType;
        this.distinct = distinct;
        this.distinctColumnIndex = distinctColumnIndex;
        this.groupByColumns = groupByColumns;
    }

    public static VModbCount create(RelNode input, RelDataType outRowType,
                                    boolean distinct, int distinctColumnIndex, int[] groupByColumns) {
        RelOptCluster cluster = input.getCluster();
        return new VModbCount(cluster, cluster.traitSetOf(VModbConvention.INSTANCE),
                input, outRowType, distinct, distinctColumnIndex, groupByColumns);
    }

    @Override
    public RelDataType deriveRowType() {
        return relDataType;
    }

    @Override
    public VModbCount copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VModbCount(getCluster(), traitSet, sole(inputs),
                relDataType, distinct, distinctColumnIndex, groupByColumns);
    }
}