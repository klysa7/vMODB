package dk.ku.di.dms.vms.modb.query.execution.raw;

public interface RawPredicate {
    boolean matches(Object[] row);
}