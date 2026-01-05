package dk.ku.di.dms.vms.calcite.modb.rel;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.util.Pair;

import java.util.List;

public interface VModbRel extends PhysicalNode {

    //todo more advanced trait propagation that i ll explore later, useless for now
    @Override
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        return null;
    }

    @Override
    default Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return null;
    }
}