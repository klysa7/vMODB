package dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rules.VModbAggregateRule;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rules.VModbFilterRule;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rules.VModbJoinRule;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rules.VModbProjectRule;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rules.VModbTableAccessRule;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.schema.Optimizer;
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

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

public final class CalcitePlannerImpl implements CalcitePlanner {

    private static final System.Logger LOGGER =
            System.getLogger(CalcitePlannerImpl.class.getName());

    private static final RuleSet RULES = RuleSets.ofList(
            VModbTableAccessRule.INSTANCE,
            VModbProjectRule.INSTANCE,
            VModbFilterRule.INSTANCE,
            VModbJoinRule.INSTANCE,
            VModbAggregateRule.INSTANCE
    );

    @Override
    public PlanOutput planOnly(String sql, SchemaPlus rootSchema, List<Object> params) {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(rootSchema, "rootSchema");

        Optimizer optimizer = Optimizer.createFromRootSchema(rootSchema);

        try {
            SqlNode parsed   = optimizer.parse(sql);
            SqlNode validated = optimizer.validate(parsed);
            RelNode logical  = optimizer.convert(validated);

            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
            HepPlanner hepPlanner = new HepPlanner(builder.build());
            hepPlanner.setRoot(logical);
            RelNode pushedDownLogical = hepPlanner.findBestExp();

            if (LOGGER.isLoggable(DEBUG)) {
                LOGGER.log(DEBUG, explainRel("LOGICAL (PUSHED DOWN)", pushedDownLogical));
            }

            RelNode physical = optimizer.optimize(
                    pushedDownLogical,
                    pushedDownLogical.getTraitSet().replace(VModbConvention.INSTANCE),
                    RULES
            );

            if (LOGGER.isLoggable(DEBUG)) {
                LOGGER.log(DEBUG, explainRel("PHYSICAL", physical));
            }

            LOGGER.log(INFO, "Planning complete for SQL: "
                    + sql.trim().substring(0, Math.min(60, sql.trim().length())) + "...");

            return new PlanOutput(pushedDownLogical, physical);
        } catch (Exception e) {
            throw new RuntimeException("Calcite planning failed -- " + e.getMessage(), e);
        }
    }

    private static String explainRel(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();
        sw.append(header).append(":\n");
        RelWriterImpl writer = new RelWriterImpl(
                new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);
        relTree.explain(writer);
        return sw.toString();
    }
}