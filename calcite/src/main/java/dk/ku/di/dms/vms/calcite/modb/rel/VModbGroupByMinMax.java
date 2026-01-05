package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public final class VModbGroupByMinMax extends SingleRel implements VModbRel {

    private final RelDataType relDataType;
    public final GroupByMinMaxEnum kind;
    public final int[] groupByColumns;
    public final int aggColumnIndex;
    public final int limit;

    private VModbGroupByMinMax(RelOptCluster cluster,
                               RelTraitSet traitSet,
                               RelNode input,
                               RelDataType relDataType,
                               GroupByMinMaxEnum kind,
                               int[] groupByColumns,
                               int aggColumnIndex,
                               int limit) {
        super(cluster, traitSet, input);
        this.relDataType = relDataType;
        this.kind = kind;
        this.groupByColumns = groupByColumns;
        this.aggColumnIndex = aggColumnIndex;
        this.limit = limit;
    }

    public static VModbGroupByMinMax create(RelNode input, RelDataType outRowType,
                                            GroupByMinMaxEnum kind, int[] groupByColumns, int aggColumnIndex, int limit) {
        RelOptCluster cluster = input.getCluster();
        return new VModbGroupByMinMax(cluster, cluster.traitSetOf(VModbConvention.INSTANCE),
                input, outRowType, kind, groupByColumns, aggColumnIndex, limit);
    }

    @Override
    public RelDataType deriveRowType() {
        return relDataType;
    }

    @Override
    public VModbGroupByMinMax copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VModbGroupByMinMax(getCluster(), traitSet, sole(inputs),
                relDataType, kind, groupByColumns, aggColumnIndex, limit);
    }
}
