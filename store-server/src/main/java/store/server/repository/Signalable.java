package store.server.repository;

import store.common.hash.Guid;

/**
 * Allows an external instance to be notified when a change happens in a repository, typically for replication purposes.
 */
public interface Signalable {

    /**
     * Callback called each time a change happens.
     *
     * @param guid A repository GUID.
     */
    void signal(Guid guid);
}
