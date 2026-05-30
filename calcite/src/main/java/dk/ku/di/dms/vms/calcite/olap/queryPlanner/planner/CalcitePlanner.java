package dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;

public interface CalcitePlanner {

    PlanOutput planOnly(String sql, SchemaPlus schema, List<Object> params);

    record PlanOutput(RelNode logicalPlan, RelNode vmodbPhysicalPlan) {}
}
