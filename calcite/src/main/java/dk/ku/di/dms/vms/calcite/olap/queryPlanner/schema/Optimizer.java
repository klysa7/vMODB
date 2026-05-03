package dk.ku.di.dms.vms.calcite.olap.queryPlanner.schema;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

public final class Optimizer {

    private final CalciteConnectionConfig config;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;
    private final VolcanoPlanner planner;

    private Optimizer(
            CalciteConnectionConfig config,
            SqlValidator validator,
            SqlToRelConverter converter,
            VolcanoPlanner planner
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.validator = Objects.requireNonNull(validator, "validator");
        this.converter = Objects.requireNonNull(converter, "converter");
        this.planner = Objects.requireNonNull(planner, "planner");
    }

    public static Optimizer createFromRootSchema(SchemaPlus rootSchema) {
        Objects.requireNonNull(rootSchema, "rootSchema");

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CalciteConnectionConfig config = buildConfig();

        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                Collections.emptyList(),
                typeFactory,
                config
        );

        SqlOperatorTable operatorTable = SqlOperatorTables.chain(SqlStdOperatorTable.instance());

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withSqlConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);

        SqlValidator validator = SqlValidatorUtil.newValidator(
                operatorTable, catalogReader, typeFactory, validatorConfig
        );

        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);

        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );

        return new Optimizer(config, validator, converter, planner);
    }

    private static CalciteConnectionConfig buildConfig() {
        Properties props = new Properties();
        props.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        props.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        props.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        return new CalciteConnectionConfigImpl(props);
    }

    public SqlNode parse(String sql) throws SqlParseException {
        Objects.requireNonNull(sql, "sql");

        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setCaseSensitive(config.caseSensitive())
                .setUnquotedCasing(config.unquotedCasing())
                .setQuotedCasing(config.quotedCasing())
                .setConformance(config.conformance())
                .build();

        return SqlParser.create(sql, parserConfig).parseStmt();
    }

    public SqlNode validate(SqlNode node) {
        return validator.validate(Objects.requireNonNull(node, "node"));
    }

    public RelNode convert(SqlNode node) {
        RelRoot root = converter.convertQuery(Objects.requireNonNull(node, "node"), false, true);
        return root.rel;
    }

    public RelNode optimize(RelNode node, RelTraitSet requiredTraitSet, RuleSet rules) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(requiredTraitSet, "requiredTraitSet");
        Objects.requireNonNull(rules, "rules");

        Program program = Programs.of(rules);

        return program.run(
                planner,
                node,
                requiredTraitSet,
                Collections.emptyList(),
                Collections.emptyList()
        );
    }
}