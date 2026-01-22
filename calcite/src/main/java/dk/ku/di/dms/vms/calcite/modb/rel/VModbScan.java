package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.legacy.VModbEnumerableScans;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public final class VModbScan extends TableScan implements EnumerableRel {

    private final String schemaName;
    private final String tableName;
    private final int[] projects;

    private VModbScan(RelOptCluster cluster,
                      RelTraitSet traitSet,
                      RelOptTable table,
                      String schemaName,
                      String tableName,
                      int[] projects) {
        super(cluster, traitSet, List.of(), table);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.projects = projects;
    }

    public static VModbScan create(RelOptCluster cluster, RelOptTable table, String schemaName, int[] projects) {
        List<String> qn = table.getQualifiedName();
        String tName = qn.get(qn.size() - 1);
        return new VModbScan(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table, schemaName, tName, projects);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final RelDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of(
                implementor.getTypeFactory(),
                rowType,
                JavaRowFormat.ARRAY
        );

        final Expression dataContext = implementor.getRootExpression();

        final Expression call = Expressions.call(
                VModbEnumerableScans.class,
                "fullScan",
                dataContext,
                Expressions.constant(schemaName),
                Expressions.constant(tableName),
                projects == null ? Expressions.constant(null) : Expressions.constant(projects),
                Expressions.constant(rowType.getFieldCount())
        );

        final BlockStatement block = Blocks.toBlock(Expressions.return_(null, call));
        return implementor.result(physType, block);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VModbScan(getCluster(), traitSet, getTable(), schemaName, tableName, projects);
    }
}