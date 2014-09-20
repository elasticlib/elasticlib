package store.server.manager.message;

import store.common.hash.Guid;

/**
 * Base class for messages related to a repository state change.
 */
abstract class RepositoryChangeMessage {

    private final Guid repositoryGuid;

    /**
     * Constructor.
     *
     * @param repositoryGuid The repository GUID.
     */
    public RepositoryChangeMessage(Guid repositoryGuid) {
        this.repositoryGuid = repositoryGuid;
    }

    /**
     * @return The repository GUID.
     */
    public Guid getRepositoryGuid() {
        return repositoryGuid;
    }
}
