package dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rules.*;
import dk.ku.di.dms.vms.calcite.schema.Optimizer;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Objects;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
import static org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_JOIN_RULE;
import static org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_PROJECT_RULE;

public final class CalcitePlannerImpl implements CalcitePlanner {

    private static final System.Logger LOGGER = System.getLogger(CalcitePlannerImpl.class.getName());

    private static final RuleSet RULES = RuleSets.ofList(
            VModbTableAccessRule.INSTANCE,
            VModbProjectRule.INSTANCE,
            VModbFilterRule.INSTANCE,
            VModbJoinRule.INSTANCE
    );

    @Override
    public PlanOutput planJoinOnly(String sql, SchemaPlus rootSchema, List<Object> params) {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(rootSchema, "rootSchema");

        Optimizer optimizer = Optimizer.createFromRootSchema(rootSchema);

        try {
            SqlNode parsed = optimizer.parse(sql);
            SqlNode validated = optimizer.validate(parsed);
            RelNode logical = optimizer.convert(validated);

            // =========================================================
            // 🔥 NEW: FORCE LOGICAL PUSHDOWN BEFORE PHYSICAL PLANNING
            // =========================================================
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
            HepPlanner hepPlanner = new HepPlanner(builder.build());
            hepPlanner.setRoot(logical);
            RelNode pushedDownLogical = hepPlanner.findBestExp();

            LOGGER.log(INFO, explainRel("LOGICAL (PUSHED DOWN)", pushedDownLogical));

            // Now we convert to pure VMODB convention
            RelNode physical = optimizer.optimize(
                    pushedDownLogical,
                    pushedDownLogical.getTraitSet().replace(VModbConvention.INSTANCE),
                    RULES
            );
            LOGGER.log(INFO, explainRel("PHYSICAL", physical));

            return new PlanOutput(pushedDownLogical, physical);
        } catch (Exception e) {
            throw new RuntimeException("Calcite planning failed-- " + e.getMessage(), e);
        }
    }

    private static String explainRel(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();
        sw.append(header).append(":\n");
        RelWriterImpl writer = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);
        relTree.explain(writer);
        return sw.toString();
    }
}