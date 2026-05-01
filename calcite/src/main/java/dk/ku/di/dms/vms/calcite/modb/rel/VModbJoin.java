package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Arrays;
import java.util.List;

public final class VModbJoin extends BiRel implements VModbRel {

    public final int[] leftJoinCols;
    public final int[] rightJoinCols;
    private final RelDataType relDataType;

    private VModbJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
                      RelNode right, RelDataType relDataType, int[] leftJoinCols,
                      int[] rightJoinCols) {
        super(cluster, traitSet, left, right);
        this.relDataType = relDataType;
        this.leftJoinCols = leftJoinCols;
        this.rightJoinCols = rightJoinCols;
    }

    public static VModbJoin create(RelNode left, RelNode right,
                                   RelDataType outRowType, int[] leftJoinCols, int[] rightJoinCols) {
        RelOptCluster cluster = left.getCluster();
        return new VModbJoin(cluster, cluster.traitSetOf(VModbConvention.INSTANCE),
                left, right, outRowType, leftJoinCols, rightJoinCols);
    }

    @Override
    public RelDataType deriveRowType() {
        return relDataType;
    }

    @Override
    public VModbJoin copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VModbJoin(getCluster(), traitSet,
                inputs.get(0), inputs.get(1), relDataType, leftJoinCols, rightJoinCols);
    }

    @Override
    public org.apache.calcite.rel.RelWriter explainTerms(org.apache.calcite.rel.RelWriter pw) {
        return super.explainTerms(pw)
                .item("leftJoinCols", Arrays.toString(leftJoinCols))
                .item("rightJoinCols", Arrays.toString(rightJoinCols));
    }
}