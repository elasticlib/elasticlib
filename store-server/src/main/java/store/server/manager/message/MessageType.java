package store.server.manager.message;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;

/**
 * Defines possible types of a message.
 */
public enum MessageType {

    /**
     * Indicates that a new event happened in a repository.
     */
    NEW_REPOSITORY_EVENT;

    @Override
    public String toString() {
        return UPPER_UNDERSCORE.to(LOWER_HYPHEN, name());
    }
}
