package store.server.manager.message;

import store.common.hash.Guid;

/**
 * Message indicating that a repository has been closed.
 */
public class RepositoryClosed extends RepositoryChangeMessage {

    /**
     * Constructor.
     *
     * @param repositoryGuid The repository GUID.
     */
    public RepositoryClosed(Guid repositoryGuid) {
        super(repositoryGuid);
    }
}
