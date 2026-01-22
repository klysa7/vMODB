package dk.ku.di.dms.vms.calcite.modb.convention;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public final class VModbConvention extends Convention.Impl {
    public static final VModbConvention INSTANCE = new VModbConvention();

    private VModbConvention() {
        super("VMODB", VModbRel.class);
    }

    //todo useless for now, we dont have any other trait than convention, thats how apache ignite enforces fixes(sortin, partitioning)
    @Override
    public RelNode enforce(RelNode rel, RelTraitSet set) {
        return rel;
    }

    @Override
    public ConventionTraitDef getTraitDef() {
        return ConventionTraitDef.INSTANCE;
    }
}
