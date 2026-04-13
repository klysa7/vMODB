package dk.ku.di.dms.vms.modb.definition.key.composite;

/**
 * AOO-6 CHANGE: added getValue(int index) abstract method.
 *
 * This is the ONLY change to BaseComposite. All existing behavior is preserved.
 *
 * WHY NEEDED:
 *   MutableIntArrayKey.equals(storedKey) needs to read the stored key's field
 *   values to compare them with the mutable probe values. The fields in
 *   PairCompositeKey, TripleCompositeKey, QuadrupleCompositeKey are private.
 *   getValue(int index) exposes them without breaking encapsulation — it is
 *   a read-only accessor used exclusively by the probe key's equals() method.
 */
public abstract class BaseComposite {

    private final int hash;

    public BaseComposite(int hash) {
        this.hash = hash;
    }

    @Override
    public int hashCode() {
        return this.hash;
    }

    /**
     * AOO-6: Returns the value at the given position in this composite key.
     * Position 0 = first key component, 1 = second, etc.
     * Used by MutableIntArrayKey.equals() to compare probe values with stored values.
     *
     * @param index 0-based position of the key component
     * @return the value at that position (Integer for INT columns)
     */
    public abstract Object getValue(int index);

}