package dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner;

import dk.ku.di.dms.vms.calcite.modb.rules.VModbTableAccessRule;
import dk.ku.di.dms.vms.calcite.modb.rules.VModbToEnumerableRule;
import dk.ku.di.dms.vms.calcite.schema.Optimizer;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
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
            VModbToEnumerableRule.INSTANCE,
            ENUMERABLE_PROJECT_RULE,
            ENUMERABLE_JOIN_RULE
    );

    @Override
    public PlanOutput planJoinOnly(String sql, SchemaPlus rootSchema, List<Object> params) {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(rootSchema, "rootSchema");
        // params currently unused (kept for future prepared statements / dynamic params)
        //noinspection unused
        List<Object> ignored = params;

        LOGGER.log(INFO, "Entered planJoinOnly()");
        LOGGER.log(INFO, "SQL:\n" + sql);

        Optimizer optimizer = Optimizer.createFromRootSchema(rootSchema);

        LOGGER.log(INFO, "Root schema: " + rootSchema.getName());
        LOGGER.log(INFO, "Subschemas: " + rootSchema.getSubSchemaNames());
        LOGGER.log(INFO, "Tables: " + rootSchema.getTableNames());

        try {
            SqlNode parsed = optimizer.parse(sql);
            LOGGER.log(INFO, "Parsed OK: " + parsed.getKind());

            SqlNode validated = optimizer.validate(parsed);
            LOGGER.log(INFO, "Validated OK: " + validated.getKind());

            RelNode logical = optimizer.convert(validated);
            LOGGER.log(INFO, explainRel("LOGICAL", logical));

            RelNode physical = optimizer.optimize(
                    logical,
                    logical.getTraitSet().replace(EnumerableConvention.INSTANCE),
                    RULES
            );

            LOGGER.log(INFO, explainRel("PHYSICAL", physical));

            return new PlanOutput(logical, physical);
        } catch (Exception e) {
            LOGGER.log(ERROR, "Calcite planning failed: " + e.getMessage());
            LOGGER.log(ERROR, "SQL was:\n" + sql);
            throw new RuntimeException("Calcite planning failed: " + e.getMessage(), e);
        }
    }

    private static String explainRel(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();
        sw.append(header).append(":\n");

        RelWriterImpl writer = new RelWriterImpl(
                new PrintWriter(sw),
                SqlExplainLevel.ALL_ATTRIBUTES,
                true
        );

        relTree.explain(writer);
        return sw.toString();
    }
}