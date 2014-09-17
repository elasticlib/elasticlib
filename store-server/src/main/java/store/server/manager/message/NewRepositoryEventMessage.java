package store.server.manager.message;

import store.common.hash.Guid;

/**
 * Indicates that a new event happened in a repository.
 */
public final class NewRepositoryEventMessage implements Message {

    private final Guid repositoryGuid;

    /**
     * Constructor.
     *
     * @param repositoryGuid The repository GUID.
     */
    public NewRepositoryEventMessage(Guid repositoryGuid) {
        this.repositoryGuid = repositoryGuid;
    }

    @Override
    public MessageType getType() {
        return MessageType.NEW_REPOSITORY_EVENT;
    }

    /**
     * @return The repository GUID.
     */
    public Guid getRepositoryGuid() {
        return repositoryGuid;
    }
}
