package dk.ku.di.dms.vms.modb.definition.key;

import dk.ku.di.dms.vms.modb.definition.key.composite.BaseComposite;

/** Zero-allocation probe key for updatesPerKeyMap.get() during parallel scans. Reuses one
 *  int[] per worker, recomputing the composite hash in place to match QuadrupleCompositeKey
 *  PROBE-ONLY: must never be put into or stored in the map. The stored keys
 *  are the immutable composite keys from OLTP write time. */
public final class MutableIntArrayKey implements IKey {

    private final int[] vals;
    private int hash;

    public MutableIntArrayKey(int size) {
        this.vals = new int[size];
    }
    public void setValues(int v0, int v1, int v2, int v3) {
        this.vals[0] = v0;
        this.vals[1] = v1;
        this.vals[2] = v2;
        this.vals[3] = v3;
        int h = 31 + v0;
        h = 31 * h + v1;
        h = 31 * h + v2;
        this.hash = 31 * h + v3;
    }

    public void setValues(int v0, int v1, int v2) {
        this.vals[0] = v0;
        this.vals[1] = v1;
        this.vals[2] = v2;
        int h = 31 + v0;
        h = 31 * h + v1;
        this.hash = 31 * h + v2;
    }

    public void setValues(int v0, int v1) {
        this.vals[0] = v0;
        this.vals[1] = v1;
        int h = 31 + v0;
        this.hash = 31 * h + v1;
    }


    @Override
    public int hashCode() {
        return this.hash;
    }

    @Override
    public int size() {
        return this.vals.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseComposite bc)) return false;
        if (!(o instanceof IKey otherKey)) return false;
        if (this.hash != bc.hashCode()) return false;
        if (this.vals.length != otherKey.size()) return false;
        for (int i = 0; i < this.vals.length; i++) {
            Object val = bc.getValue(i);
            if (val == null) return false;
            if (((Number) val).intValue() != this.vals[i]) return false;
        }
        return true;
    }
}