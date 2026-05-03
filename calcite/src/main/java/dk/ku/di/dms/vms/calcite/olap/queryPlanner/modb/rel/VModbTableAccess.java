package dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rel;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode; // NEW IMPORT

import java.util.List;

public final class VModbTableAccess extends TableScan implements VModbRel {

    public final String schemaName;
    public final String tableName;
    public final int[] projects;
    public final RexNode filter;
    public final IKey key;
    public final IKey[] keys;
    private RelDataType relDataType;

    private VModbTableAccess(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
                             String schemaName, String tableName, int[] projects,
                             RexNode filter, IKey key, IKey[] keys) { // CHANGED
        super(cluster, traitSet, List.of(), table);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.projects = projects;
        this.filter = filter;
        this.key = key;
        this.keys = keys;
    }

    public static VModbTableAccess create(RelOptCluster cluster, RelOptTable table, String schemaName,
                                          int[] projects, RexNode filter, IKey key, IKey[] keys) {
        List<String> qualifiedName = table.getQualifiedName();
        String tableName = qualifiedName.get(qualifiedName.size() - 1);
        return new VModbTableAccess(cluster, cluster.traitSetOf(VModbConvention.INSTANCE),
                table, schemaName, tableName, projects, filter, key, keys);
    }

    @Override
    public RelDataType deriveRowType() {
        if (relDataType != null) return relDataType;
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        RelDataType base = getTable().getRowType();

        if (projects == null) {
            relDataType = base;
            return relDataType;
        }

        relDataType = typeFactory.createStructType(projectsToTypes(base, projects), projectsToNames(base, projects));
        return relDataType;
    }

    private static List<RelDataType> projectsToTypes(RelDataType base, int[] projects) {
        List<RelDataTypeField> fields = base.getFieldList();
        List<RelDataType> types = new java.util.ArrayList<>(projects.length);
        java.util.Arrays.stream(projects).forEach(p -> types.add(fields.get(p).getType()));
        return types;
    }

    private static List<String> projectsToNames(RelDataType base, int[] projects) {
        List<RelDataTypeField> fields = base.getFieldList();
        List<String> names = new java.util.ArrayList<>(projects.length);
        java.util.Arrays.stream(projects).forEach(p -> names.add(fields.get(p).getName()));
        return names;
    }

    @Override
    public VModbTableAccess copy(RelTraitSet traitSet, List<org.apache.calcite.rel.RelNode> inputs) {
        return new VModbTableAccess(getCluster(), traitSet, getTable(), schemaName, tableName, projects, filter, key, keys);
    }

    @Override
    public org.apache.calcite.rel.RelWriter explainTerms(org.apache.calcite.rel.RelWriter pw) {
        return super.explainTerms(pw)
                .item("schema", schemaName)
                .item("table", tableName)
                .item("projects", projects == null ? "ALL" : java.util.Arrays.toString(projects))
                .item("filter", filter);
    }

    public String getSchemaName() { return schemaName; }
    public String getTableName() { return tableName; }
    public int[] getProjects() { return projects; }
    public RexNode getFilter() { return filter; }
    public IKey getKey() { return key; }
    public IKey[] getKeys() { return keys; }
}