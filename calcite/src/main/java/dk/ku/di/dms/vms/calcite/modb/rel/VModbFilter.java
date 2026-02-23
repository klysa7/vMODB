package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

public final class VModbFilter extends Filter implements VModbRel {

    private VModbFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        super(cluster, traitSet, child, condition);
    }

    public static VModbFilter create(RelNode input, RexNode condition) {
        RelOptCluster cluster = input.getCluster();
        return new VModbFilter(
                cluster,
                cluster.traitSetOf(VModbConvention.INSTANCE),
                input,
                condition
        );
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new VModbFilter(getCluster(), traitSet, input, condition);
    }
}