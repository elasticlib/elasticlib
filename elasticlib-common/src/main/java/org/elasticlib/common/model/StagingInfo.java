package org.elasticlib.common.model;

import static com.google.common.base.MoreObjects.toStringHelper;
import java.util.Map;
import java.util.Objects;
import org.elasticlib.common.hash.Guid;
import org.elasticlib.common.hash.Hash;
import org.elasticlib.common.mappable.MapBuilder;
import org.elasticlib.common.mappable.Mappable;
import org.elasticlib.common.util.EqualsBuilder;
import org.elasticlib.common.value.Value;

/**
 * Holds info about a content staging session.
 */
public class StagingInfo implements Mappable {

    private static final String SESSION_ID = "sessionId";
    private static final String HASH = "hash";
    private static final String LENGTH = "length";
    private final Guid sessionId;
    private final Hash hash;
    private final Long length;

    /**
     * Constructor.
     *
     * @param sessionId Staging session identifier.
     * @param hash Staged content hash.
     * @param length Staged content length.
     */
    public StagingInfo(Guid sessionId, Hash hash, Long length) {
        this.sessionId = sessionId;
        this.hash = hash;
        this.length = length;
    }

    /**
     * @return Staging session identifier.
     */
    public Guid getSessionId() {
        return sessionId;
    }

    /**
     * @return (partial) Hash of the currently staged content.
     */
    public Hash getHash() {
        return hash;
    }

    /**
     * @return length of the currently staged content.
     */
    public Long getLength() {
        return length;
    }

    @Override
    public Map<String, Value> toMap() {
        return new MapBuilder()
                .put(SESSION_ID, sessionId)
                .put(HASH, hash)
                .put(LENGTH, length)
                .build();
    }

    /**
     * Read a new instance from supplied map of values.
     *
     * @param map A map of values.
     * @return A new instance.
     */
    public static StagingInfo fromMap(Map<String, Value> map) {
        return new StagingInfo(map.get(SESSION_ID).asGuid(),
                               map.get(HASH).asHash(),
                               map.get(LENGTH).asLong());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId,
                            hash,
                            length);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StagingInfo)) {
            return false;
        }
        StagingInfo other = (StagingInfo) obj;
        return new EqualsBuilder()
                .append(sessionId, other.sessionId)
                .append(hash, other.hash)
                .append(length, other.length)
                .build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add(SESSION_ID, sessionId)
                .add(HASH, hash)
                .add(LENGTH, length)
                .toString();
    }
}
