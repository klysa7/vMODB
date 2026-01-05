package dk.ku.di.dms.vms.calcite.schema;


import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import dk.ku.di.dms.vms.calcite.modb.rules.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class vModbTest {


    @Test
    public void vmodbTableAccessFilter() throws Exception {
        Optimizer optimizer = buildOptimizer();

        String sql = "select * from item where i_name = ?";

        RelNode rel = toRel(optimizer, sql);
        print("ITEM FILTER(*): AFTER CONVERSION (LOGICAL)", rel);

        RelNode opt = optimizeToVmodb(optimizer, rel);
        print("ITEM FILTER(*): AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);

        String plan = explain(opt);
        assertTrue(plan.contains("VModbTableAccess"), "Expected VModbTableAccess in plan, got:\n" + plan);
        assertTrue(!plan.contains("LogicalFilter"), "Expected LogicalFilter gone, got:\n" + plan);
        assertTrue(!plan.contains("LogicalTableScan"), "Expected LogicalTableScan gone, got:\n" + plan);

        VModbTableAccess access = findTableAccess(opt);
        assertNotNull(access, "Expected VModbTableAccess in physical plan");
        assertNotNull(access.filter, "Expected non-null filter for WHERE clause");
    }

    @Test
    public void vmodbGroupByMin() throws Exception {
        Optimizer optimizer = buildOptimizer();

        String sql =
                "select o.o_w_id, min(o.o_ol_cnt) " +
                        "from orders o " +
                        "where o.o_w_id = ? " +
                        "group by o.o_w_id";

        RelNode rel = toRel(optimizer, sql);
        print("GB MIN ORDERS: AFTER CONVERSION (LOGICAL)", rel);

        RelNode opt = optimizeToVmodb(optimizer, rel);
        print("GB MIN ORDERS: AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);

        String plan = explain(opt);

        assertTrue(plan.contains("VModbGroupByMinMax"), "Expected VModbGroupByMinMax in plan, got:\n" + plan);
        assertTrue(plan.contains("VModbTableAccess"), "Expected VModbTableAccess in plan, got:\n" + plan);
        assertTrue(opt.getRowType().getFieldCount() == 2,
                "Expected 2 output columns (group key + min), got: " + opt.getRowType());
    }

    @Test
    public void vmodbGroupByMax() throws Exception {
        Optimizer optimizer = buildOptimizer();

        String sql =
                "select o.o_w_id, max(o.o_ol_cnt) " +
                        "from orders o " +
                        "where o.o_w_id = ? " +
                        "group by o.o_w_id";

        RelNode rel = toRel(optimizer, sql);
        print("GB MAX ORDERS: AFTER CONVERSION (LOGICAL)", rel);

        RelNode opt = optimizeToVmodb(optimizer, rel);
        print("GB MAX ORDERS: AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);

        String plan = explain(opt);

        assertTrue(plan.contains("VModbGroupByMinMax"), "Expected VModbGroupByMinMax in plan, got:\n" + plan);
        assertTrue(plan.contains("VModbTableAccess"), "Expected VModbTableAccess in plan, got:\n" + plan);
        assertTrue(opt.getRowType().getFieldCount() == 2,
                "Expected 2 output columns (group key + max), got: " + opt.getRowType());
    }

    @Test
    public void vmodbCount() throws Exception {
        Optimizer optimizer = buildOptimizer();

        String sql =
                "select count(i.i_id) " +
                        "from item i " +
                        "where i.i_id = ?";

        RelNode rel = toRel(optimizer, sql);
        print("COUNT ITEM: AFTER CONVERSION (LOGICAL)", rel);

        RelNode opt = optimizeToVmodb(optimizer, rel);
        print("COUNT ITEM: AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);
    }


    @Test
    public void vmodbTableAccess() throws Exception {
        Optimizer optimizer = buildOptimizer();

        String sql =
                "select i.i_id, i.i_name, i.i_price " +
                        "from item i " +
                        "where i.i_id = ?";

        RelNode rel = toRel(optimizer, sql);
        print("ITEM: AFTER CONVERSION (LOGICAL)", rel);

        RelNode opt = optimizeToVmodb(optimizer, rel);
        print("ITEM: AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);
    }

    @Test
    public void vmodbTableAccessDetailed() throws Exception {
        Optimizer optimizer = buildOptimizer();

        String sql = "select i.i_id, i.i_name, i.i_price " +
                        "from item i " +
                        "where i.i_id = ?";

        RelNode rel = toRel(optimizer, sql);
        print("ITEM: AFTER CONVERSION (LOGICAL)", rel);

        RelNode opt = optimizeToVmodb(optimizer, rel);
        print("ITEM: AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);

        VModbTableAccess access = findTableAccess(opt);
        assertNotNull(access, "VModbTableAccess in physical plan");

        System.out.println("---- VMODB TABLE ACCESS ----");
        System.out.println("Schema     : " + access.schemaName);
        System.out.println("Table      : " + access.tableName);

        if (access.projects == null) {
            System.out.println("Projection : SELECT *");
        } else {
            System.out.println("Projection : " + java.util.Arrays.toString(access.projects));
            System.out.println("Projected columns:");
            var fields = access.getTable().getRowType().getFieldList();
            for (int p : access.projects) {
                System.out.println("  - " + fields.get(p).getName()
                        + " : " + fields.get(p).getType());
            }
        }

        System.out.println("Filter     : " + access.filter);

        System.out.println("Output RowType:");
        access.getRowType().getFieldList()
                .forEach(f -> System.out.println("  - " + f.getName() + " : " + f.getType()));
    }


    private static VModbTableAccess findTableAccess(RelNode node) {
        if (node instanceof VModbTableAccess v) {
            return v;
        }
        for (RelNode input : node.getInputs()) {
            VModbTableAccess found = findTableAccess(input);
            if (found != null) return found;
        }
        return null;
    }

//    @Test
//    public void vmodbCount() throws Exception {
//        Optimizer optimizer = buildOptimizer();
//
//        String sql =
//                "select o.o_id, o.o_entry_d, o.o_c_id " +
//                        "from orders o " +
//                        "where o.o_w_id = ? and o.o_d_id = ? and o.o_id = ?";
//
//        RelNode rel = toRel(optimizer, sql);
//        print("ORDERS: AFTER CONVERSION (LOGICAL)", rel);
//
//        RelNode opt = optimizeToVmodb(optimizer, rel);
//        print("ORDERS: AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);
//    }



    @Disabled
    @Test
    public void index_scan_item_by_pk_pushdown() throws Exception {
        Optimizer optimizer = buildOptimizer();

        // pk equality
        String sql = "select * from item where i_id = ?";

        RelNode rel = toRel(optimizer, sql);
        print("INDEX SCAN ITEM: AFTER CONVERSION (LOGICAL)", rel);

        RelNode opt = optimizeToVmodb(optimizer, rel);
        print("INDEX SCAN ITEM: AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);

        String plan = explain(opt);

        assertTrue(plan.contains("VModbIndexScan"),
                "Expected VModbIndexScan in plan, got:\n" + plan);
    }


    @Test
    public void join_orders_item_pushdown_splits_correctly() throws Exception {
        Optimizer optimizer = buildOptimizer();

        // No top-level projection => easier milestone
        String sql =
                "select * " +
                        "from (select * from orders where o_w_id = ?) o " +
                        "join (select * from item where i_id = ?) i " +
                        "on o.o_id = i.i_id";

        RelNode rel = toRel(optimizer, sql);
        print("JOIN ORDERS-ITEM: AFTER CONVERSION (LOGICAL)", rel);

        RelNode opt = optimizeToVmodb(optimizer, rel);
        print("JOIN ORDERS-ITEM: AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);

        String plan = explain(opt);

        assertTrue(plan.contains("VModbJoin"), "Expected VModbJoin in plan, got:\n" + plan);

        int accesses = countOccurrences(plan, "VModbTableAccess");
        assertTrue(accesses >= 2,
                "Expected >= 2 VModbTableAccess nodes (left+right), got " + accesses + ":\n" + plan);
    }

    /** Utility: count how many times a substring appears in a string. */
    private static int countOccurrences(String s, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = s.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }
    // -------------------- TESTS --------------------


    @Disabled("No vMODB join physical operator yet. This test requires either Enumerable join or a vMODB join lowering+executor.")
    @Test
    public void combined_join_orders_item() throws Exception {
        Optimizer optimizer = buildOptimizer();

        String sql =
                "select o.o_id, i.i_name, i.i_price " +
                        "from orders o " +
                        "join item i on o.o_id = i.i_id " +
                        "where o.o_w_id = ? and i.i_price > ?";

        RelNode rel = toRel(optimizer, sql);
        print("JOIN: AFTER CONVERSION (LOGICAL)", rel);

        RelNode opt = optimizeToVmodb(optimizer, rel);
        print("JOIN: AFTER OPTIMIZATION (VMODB PHYSICAL)", opt);
    }

    // -------------------- BUILDERS --------------------

    private Optimizer buildOptimizer() {
        vModbTable orders = buildOrdersTable();
        vModbTable item = buildItemTable();

        vModbSchema schema = buildSchema(orders, item);

        return Optimizer.create(schema);
    }

    private vModbTable buildOrdersTable() {
        return vModbTable.newBuilder("orders")
                .addField("o_id", SqlTypeName.INTEGER)
                .addField("o_d_id", SqlTypeName.INTEGER)
                .addField("o_w_id", SqlTypeName.INTEGER)
                .addField("o_c_id", SqlTypeName.INTEGER)
                .addField("o_entry_d", SqlTypeName.DATE)
                .addField("o_carrier_id", SqlTypeName.INTEGER)
                .addField("o_ol_cnt", SqlTypeName.INTEGER)
                .addField("o_all_local", SqlTypeName.INTEGER)
                .withRowCount(1_000_000L)
                .build();
    }

    private vModbTable buildItemTable() {
        return vModbTable.newBuilder("item")
                .addField("i_id", SqlTypeName.INTEGER)
                .addField("i_name", SqlTypeName.VARCHAR)
                .addField("i_price", SqlTypeName.DECIMAL)
                .addField("i_data", SqlTypeName.VARCHAR)
                .withRowCount(100_000L)
                .build();
    }

    private vModbSchema buildSchema(vModbTable orders, vModbTable item) {
        return vModbSchema.newBuilder("tpcc")
                .addTable(orders)
                .addTable(item)
                .build();
    }


    private RelNode toRel(Optimizer optimizer, String sql) throws Exception {
        SqlNode parsed = optimizer.parse(sql);
        SqlNode validated = optimizer.validate(parsed);
        return optimizer.convert(validated);
    }

    private RelNode optimizeToVmodb(Optimizer optimizer, RelNode rel) {
        RuleSet rules = RuleSets.ofList(
                VModbTableAccessRule.INSTANCE,
                VModbTableAccessFilterRule.INSTANCE,
                VModbProjectRule.INSTANCE,
                VModbIndexScanRule.INSTANCE,
                VModbJoinRule.INSTANCE,
                VModbCountRule.INSTANCE,
                VModbGroupByMinMaxRule.INSTANCE
        );

        return optimizer.optimize(rel, rel.getTraitSet().plus(VModbConvention.INSTANCE), rules);
    }

    private void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();
        sw.append(header).append(":\n");

        RelWriterImpl relWriter = new RelWriterImpl(
                new PrintWriter(sw),
                SqlExplainLevel.ALL_ATTRIBUTES,
                true
        );

        relTree.explain(relWriter);
        System.out.println(sw);
    }

    private String explain(RelNode relTree) {
        return explainWithHeader("", relTree);
    }

    private String explainWithHeader(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();
        if (!header.isEmpty()) sw.append(header).append(":\n");

        RelWriterImpl relWriter = new RelWriterImpl(
                new PrintWriter(sw),
                SqlExplainLevel.ALL_ATTRIBUTES,
                true
        );

        relTree.explain(relWriter);
        return sw.toString();
    }
}