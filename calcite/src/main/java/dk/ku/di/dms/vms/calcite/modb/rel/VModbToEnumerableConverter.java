package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

public final class VModbToEnumerableConverter extends ConverterImpl {

    private VModbToEnumerableConverter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input
    ) {
        super(
                cluster,
                ConventionTraitDef.INSTANCE,
                traits,
                input
        );
    }

    public static VModbToEnumerableConverter create(RelNode input) {
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traits = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        return new VModbToEnumerableConverter(cluster, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, java.util.List<RelNode> inputs) {
        return new VModbToEnumerableConverter(
                getCluster(),
                traitSet,
                inputs.get(0)
        );
    }
}