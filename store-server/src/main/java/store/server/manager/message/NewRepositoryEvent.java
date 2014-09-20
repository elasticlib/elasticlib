package store.server.manager.message;

import store.common.hash.Guid;

/**
 * Message indicating that a new event happened in a repository.
 */
public final class NewRepositoryEvent extends RepositoryChangeMessage {

    /**
     * Constructor.
     *
     * @param repositoryGuid The repository GUID.
     */
    public NewRepositoryEvent(Guid repositoryGuid) {
        super(repositoryGuid);
    }
}