package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbCount;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;

public final class VModbCountRule extends RelOptRule {

    public static final VModbCountRule INSTANCE = new VModbCountRule();

    private VModbCountRule() {
        super(operand(LogicalAggregate.class, any()), "VModbCountRule");
    }

    //todo for now we only support one aggregation and that is count
    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggregate = call.rel(0);

        if (aggregate.getAggCallList().size() != 1) return;

        AggregateCall aggregateCall = aggregate.getAggCallList().get(0);
        if (aggregateCall.getAggregation().getKind() != SqlKind.COUNT) return;

        RelNode input = aggregate.getInput();

        RelNode converted = convert(input, input.getTraitSet().replace(VModbConvention.INSTANCE));
        if (converted == null) return;

        call.transformTo(VModbCount.create(converted, aggregate.getRowType(),
                false, -1, new int[0]));
    }
}