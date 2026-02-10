//package dk.ku.di.dms.vms.calcite.legendary;
//
//public class OldDistributedPlanner {
//
//
//            if (!(physicalPlan instanceof VModbProject vModbProject)) {
//        throw new IllegalArgumentException("Expected VModbProject root got " + physicalPlan.getClass().getName());
//    }
//        if (!(vModbProject.getInput() instanceof VModbJoin vModbJoin)) {
//        throw new IllegalArgumentException("Expected Project(Join()), got " + vModbProject.getInput().getClass().getName());
//    }
//        if (!(vModbJoin.getLeft() instanceof VModbTableAccess leftAcc)) {
//        throw new IllegalArgumentException("Expected left TableAccess got " + vModbJoin.getLeft().getClass().getName());
//    }
//        if (!(vModbJoin.getRight() instanceof VModbTableAccess rightAcc)) {
//        throw new IllegalArgumentException("Expected right TableAccess got " + vModbJoin.getRight().getClass().getName());
//    }
//
//    String leftSchema = leftAcc.getSchemaName();
//    String leftTable  = leftAcc.getTableName();
//
//    String rightSchema = rightAcc.getSchemaName();
//    String rightTable  = rightAcc.getTableName();
//
//    VmsSubplan left = new VmsSubplan(
//            placement.ownerVms(leftSchema, leftTable),
//            placement.endpointUrl(leftSchema, leftTable),
//            "ex_left",
//            new ScanAllOperation(leftSchema, leftTable),
//            columnsResolver.columnsInOrder(leftSchema, leftTable)
//    );
//
//    VmsSubplan right = new VmsSubplan(
//            placement.ownerVms(rightSchema, rightTable),
//            placement.endpointUrl(rightSchema, rightTable),
//            "ex_right",
//            new ScanAllOperation(rightSchema, rightTable),
//            columnsResolver.columnsInOrder(rightSchema, rightTable)
//    );
//
//    Integer leftKey = vModbJoin.getLeftJoinCol();
//    Integer rightKey = vModbJoin.getRightJoinCol();
//
//    CoordinatorHashJoinOperation joinOperation =
//            new CoordinatorHashJoinOperation(left.exchangeId, right.exchangeId, leftKey, rightKey, "ex_joined");
//
//    int[] projects = vModbProject.getProjects();
//    CoordinatorProjectOperation projectOp =
//            new CoordinatorProjectOperation(joinOperation.outExchangeId, projects, "ex_final");
//
//        return new DistributedPlan(snapshot, List.of(left, right), joinOperation, projectOp);
//}
