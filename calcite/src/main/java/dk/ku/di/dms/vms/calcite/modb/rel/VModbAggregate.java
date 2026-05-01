package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;


public final class VModbAggregate extends Aggregate implements VModbRel {

    private VModbAggregate(RelOptCluster cluster, RelTraitSet traitSet,
                           RelNode input, ImmutableBitSet groupSet,
                           List<ImmutableBitSet> groupSets,
                           List<AggregateCall> aggCalls) {
        super(cluster, traitSet, List.of(), input, groupSet, groupSets, aggCalls);
    }

    public static VModbAggregate create(RelNode input,
                                        ImmutableBitSet groupSet,
                                        List<ImmutableBitSet> groupSets,
                                        List<AggregateCall> aggCalls) {
        RelOptCluster cluster = input.getCluster();
        return new VModbAggregate(
                cluster,
                cluster.traitSetOf(VModbConvention.INSTANCE),
                input, groupSet, groupSets, aggCalls);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input,
                          ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets,
                          List<AggregateCall> aggCalls) {
        return new VModbAggregate(getCluster(), traitSet, input,
                groupSet, groupSets, aggCalls);
    }
}