package store.server.manager.message;

import store.common.hash.Guid;

/**
 * Message indicating that a repository has been removed.
 */
public class RepositoryRemoved extends RepositoryChangeMessage {

    /**
     * Constructor.
     *
     * @param repositoryGuid The repository GUID.
     */
    public RepositoryRemoved(Guid repositoryGuid) {
        super(repositoryGuid);
    }
}
