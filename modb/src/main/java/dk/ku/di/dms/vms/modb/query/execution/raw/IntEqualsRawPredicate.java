package dk.ku.di.dms.vms.modb.query.execution.raw;

public final class IntEqualsRawPredicate implements RawPredicate {

    private final int columnIndex;
    private final int targetValue;

    public IntEqualsRawPredicate(int columnIndex, int targetValue) {
        this.columnIndex = columnIndex;
        this.targetValue = targetValue;
        System.out.println(">>> [IntEqualsRawPredicate] Instantiated Filter: Column " + columnIndex + " == " + targetValue);
    }

    @Override
    public boolean matches(Object[] row) {
        Object memoryValue = row[this.columnIndex];
        if (memoryValue == null) return false;

        return ((Number) memoryValue).intValue() == this.targetValue;
    }
}