package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.List;

public final class VModbProject extends SingleRel implements VModbRel {

    public final int[] projects;
    private final RelDataType rowType;

    private VModbProject(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         RelNode input,
                         RelDataType rowType,
                         int[] projects) {
        super(cluster, traitSet, input);
        this.rowType = rowType;
        this.projects = projects;
    }

    public static VModbProject create(RelNode input, RelDataType outRowType, int[] projects) {
        RelOptCluster cluster = input.getCluster();
        return new VModbProject(
                cluster,
                cluster.traitSetOf(VModbConvention.INSTANCE),
                input,
                outRowType,
                projects
        );
    }

    @Override
    public RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public VModbProject copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VModbProject(getCluster(), traitSet, inputs.get(0), rowType, projects);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("projects", Arrays.toString(projects));
    }

    public int[] getProjects() {
        return projects;
    }
}