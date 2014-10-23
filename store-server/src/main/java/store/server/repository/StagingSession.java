package store.server.repository;

import store.common.hash.Digest.DigestBuilder;
import store.common.hash.Guid;

/**
 * Represents a content staging session.
 */
class StagingSession {

    private final Guid sessionId;
    private final DigestBuilder digest;

    /**
     * Constructor.
     *
     * @param sessionId Staging session identifier.
     * @param digest Current digest.
     */
    public StagingSession(Guid sessionId, DigestBuilder digest) {
        this.sessionId = sessionId;
        this.digest = digest;
    }

    /**
     * @return The identifier of this session.
     */
    public Guid getSessionId() {
        return sessionId;
    }

    /**
     * @return Current digest of this session.
     */
    public DigestBuilder getDigest() {
        return digest;
    }
}
