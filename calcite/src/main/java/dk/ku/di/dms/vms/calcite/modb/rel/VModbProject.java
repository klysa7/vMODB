package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public final class VModbProject extends SingleRel implements VModbRel {

    private final List<RexNode> projects;
    private final RelDataType relDataType;

    private VModbProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<RexNode> projects,
            RelDataType relDataType
    ) {
        super(cluster, traitSet, input);
        this.projects = projects;
        this.relDataType = relDataType;
    }

    public static VModbProject create(RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new VModbProject(input.getCluster(),
                input.getCluster().traitSetOf(VModbConvention.INSTANCE), input, projects, rowType);
    }

    @Override
    public RelDataType deriveRowType() {
        return relDataType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VModbProject(getCluster(),
                traitSet, sole(inputs), projects, relDataType);
    }
}
