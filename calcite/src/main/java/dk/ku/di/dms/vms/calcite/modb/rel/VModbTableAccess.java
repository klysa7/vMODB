package dk.ku.di.dms.vms.calcite.modb.rel;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public final class VModbTableAccess extends TableScan implements VModbRel {

    public final String schemaName;
    public final String tableName;
    public final int[] projects;
    public final FilterContext filter;
    public final IKey key;//for indexscan later
    public final IKey[] keys;//for indexscan later
    private RelDataType relDataType;

    private VModbTableAccess(RelOptCluster cluster,
                             RelTraitSet traitSet,
                             RelOptTable table,
                             String schemaName,
                             String tableName,
                             int[] projects,
                             FilterContext filter,
                             IKey key,
                             IKey[] keys) {
        super(cluster, traitSet, List.of(), table);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.projects = projects;
        this.filter = filter;
        this.key = key;
        this.keys = keys;
    }

    public static VModbTableAccess create(RelOptCluster cluster, RelOptTable table, String schemaName,
                                          int[] projects, FilterContext filter, IKey key, IKey[] keys) {

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

        relDataType = typeFactory.createStructType(
                projectsToTypes(base, projects),
                projectsToNames(base, projects)
        );

        return relDataType;
    }

    private static List<RelDataType> projectsToTypes(RelDataType base, int[] projects) {
        List<RelDataTypeField> fields = base.getFieldList();
        List<RelDataType> types = new java.util.ArrayList<>(projects.length);

        java.util.Arrays.stream(projects)
                .forEach(p -> types.add(fields.get(p).getType()));
        return types;
    }

    private static List<String> projectsToNames(RelDataType base, int[] projects) {
        List<RelDataTypeField> fields = base.getFieldList();
        List<String> names = new java.util.ArrayList<>(projects.length);

        java.util.Arrays.stream(projects)
                .forEach(p -> names.add(fields.get(p).getName()));
        return names;
    }

    @Override
    public VModbTableAccess copy(RelTraitSet traitSet, List<org.apache.calcite.rel.RelNode> inputs) {
        return new VModbTableAccess(getCluster(), traitSet, getTable(), schemaName, tableName, projects, filter, key, keys);
    }
}
