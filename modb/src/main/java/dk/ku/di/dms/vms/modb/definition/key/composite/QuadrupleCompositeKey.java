package dk.ku.di.dms.vms.modb.definition.key.composite;

import dk.ku.di.dms.vms.modb.definition.key.IKey;

public final class QuadrupleCompositeKey extends BaseComposite implements IKey {

    private final Object value0;
    private final Object value1;
    private final Object value2;
    private final Object value3;

    public static QuadrupleCompositeKey of(Object value0, Object value1, Object value2, Object value3){
        return new QuadrupleCompositeKey(value0, value1, value2, value3);
    }

    private QuadrupleCompositeKey(Object value0, Object value1, Object value2, Object value3) {
        super(hashCode(value0, value1, value2, value3));
        this.value0 = value0;
        this.value1 = value1;
        this.value2 = value2;
        this.value3 = value3;
    }

    private static int hashCode(Object value0, Object value1, Object value2, Object value3) {
        int result = 31 * value0.hashCode() + value1.hashCode();
        result = 31 * result + value2.hashCode();
        return 31 * result + value3.hashCode();
    }

    @Override
    public boolean equals(Object key){
        return key instanceof QuadrupleCompositeKey compositeKey &&
                this.value0.equals(compositeKey.value0) &&
                this.value1.equals(compositeKey.value1) &&
                this.value2.equals(compositeKey.value2) &&
                this.value3.equals(compositeKey.value3);
    }

    @Override
    public String toString() {
        return "{"
                + "\"value0\":" + this.value0
                + ",\"value1\":" + this.value1
                + ",\"value2\":" + this.value2
                + ",\"value3\":" + this.value3
                + "}";
    }

    @Override
    public int size(){
        return 4;
    }

}
