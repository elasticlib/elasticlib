package org.elasticlib.node.manager.storage;

/**
 * Functionnal interface defining a procedure, that is an operation which returns nothing.
 */
@FunctionalInterface
public interface Procedure {

    /**
     * Invoke this procedure.
     */
    void apply();
}
