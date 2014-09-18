package store.server.manager.message;

import store.common.hash.Guid;

/**
 * Message indicating that a new event happened in a repository.
 */
public final class NewRepositoryEvent {

    private final Guid repositoryGuid;

    /**
     * Constructor.
     *
     * @param repositoryGuid The repository GUID.
     */
    public NewRepositoryEvent(Guid repositoryGuid) {
        this.repositoryGuid = repositoryGuid;
    }

    /**
     * @return The repository GUID.
     */
    public Guid getRepositoryGuid() {
        return repositoryGuid;
    }
}
