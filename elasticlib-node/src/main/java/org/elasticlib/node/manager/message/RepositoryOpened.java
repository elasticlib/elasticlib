package org.elasticlib.node.manager.message;

import org.elasticlib.common.hash.Guid;

/**
 * Message indicating that a repository has been opened.
 */
public final class RepositoryOpened extends RepositoryChangeMessage {

    /**
     * Constructor.
     *
     * @param repositoryGuid The repository GUID.
     */
    public RepositoryOpened(Guid repositoryGuid) {
        super(repositoryGuid);
    }
}
