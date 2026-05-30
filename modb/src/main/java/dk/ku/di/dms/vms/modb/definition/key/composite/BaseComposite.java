package dk.ku.di.dms.vms.modb.definition.key.composite;

/** Base class for fixed composite keys, holding a precomputed hash. getValue(int) is a
 *  read-only accessor added so MutableIntArrayKey.equals() can compare a mutable probe key
 *  against a stored key without exposing the subclasses' private fields. */
public abstract class BaseComposite {

    private final int hash;

    public BaseComposite(int hash){
        this.hash = hash;
    }

    @Override
    public int hashCode() {
        return this.hash;
    }
    public abstract Object getValue(int index);

}
