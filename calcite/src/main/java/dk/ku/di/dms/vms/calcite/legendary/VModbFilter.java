package dk.ku.di.dms.vms.calcite.legendary;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public final class VModbFilter extends SingleRel implements VModbRel {

    public final RexNode condition;
    private final RelDataType rowType;

    private VModbFilter(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelNode input,
                        RexNode condition,
                        RelDataType rowType) {
        super(cluster, traitSet, input);
        this.condition = condition;
        this.rowType = rowType;
    }

    public static VModbFilter create(RelNode input, RexNode condition) {
        RelOptCluster cluster = input.getCluster();
        return new VModbFilter(
                cluster,
                cluster.traitSetOf(VModbConvention.INSTANCE),
                input,
                condition,
                input.getRowType()
        );
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public VModbFilter copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VModbFilter(getCluster(), traitSet, inputs.get(0), condition, rowType);
    }
}