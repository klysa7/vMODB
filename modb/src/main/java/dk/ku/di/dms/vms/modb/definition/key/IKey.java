package dk.ku.di.dms.vms.modb.definition.key;

/**
 * An interface for keys of rows and indexes
 */
public interface IKey {

    default int size() { return 1; }

    int hashCode();

}
